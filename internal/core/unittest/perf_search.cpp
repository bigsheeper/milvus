// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdint>
#include <string>
#include <iostream>
#include <cstdlib>

#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/Timer.h"

using namespace milvus;

static int dim = 128;
static int nq = 10;

static int efConstruction = 150;
static int M = 12;

static int topK = 50;
static int ef = 50;

const auto schema = []() {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, MetricType::METRIC_L2);
    return schema;
}();

const auto plan = [] {
    std::string hnsw_dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "ef": 50
                        },
                        "query": "$0",
                        "topk": 50,
                        "round_decimal": 6
                    }
                }
            }
            ]
        }
    })";
    auto plan = query::CreatePlan(*schema, hnsw_dsl);
    return plan;
}();

auto ph_group = [] {
    auto ph_group_raw = segcore::CreatePlaceholderGroup(nq, dim, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    return ph_group;
}();

std::vector<segcore::SegmentSealedPtr> segments;

void
Create_Index_HNSW(int n, int num_segments) {
    static int inited = 0;
    static int64_t N = n;
    const auto dataset_ = [] {
        auto dataset_ = segcore::DataGen(schema, N);
        return dataset_;
    }();

    auto index_type = knowhere::IndexEnum::INDEX_HNSW;
    auto metric_type = MetricType::METRIC_L2;
    auto conf = knowhere::Config{
        {knowhere::meta::DIM, dim},
        {knowhere::IndexParams::efConstruction, efConstruction},
        {knowhere::IndexParams::M, M},
        {knowhere::Metric::TYPE, knowhere::Metric::L2},
        {knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
    };
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto vec = reinterpret_cast<const float*>(dataset_.cols_[0].data());
    auto xb_dataset = knowhere::GenDataset(N, dim, vec);
    index->Train(xb_dataset, conf);
    index->AddWithoutIds(xb_dataset, conf);
    // std::cout << "Build index done" << std::endl;

    segments.resize(num_segments);
    for (int i = 0; i < num_segments; i++) {
        segments[i] = segcore::CreateSealedSegment(schema);
        SealedLoader(dataset_, *segments[i]);
        LoadIndexInfo info;
        info.index = index;
        info.field_id = (*schema)[FieldName("fakevec")].get_id().get();
        info.index_params["index_mode"] = "CPU";
        info.index_params["metric_type"] = MetricTypeToName(metric_type);
        segments[i]->LoadIndex(info);
    }
}

static void
Search_Index_HNSW(int i, int round) {
    auto tid = std::this_thread::get_id();
    Timer timer;
    Timestamp time = 1000000000;
    double totalDuration = 0;
    double count = 0;
    auto round_per_tick = 1000;
    for (int r = 1; r <= round; r++) {
        if ((r % round_per_tick) == 0) {
            auto t = timer.get_step_seconds();
            std::cout << "thread " << tid << ", " << round_per_tick << " cost: " << t << " seconds" << std::endl;
            totalDuration += t;
            count++;
            timer.reset();
        }
        auto qr = segments[i]->Search(plan.get(), ph_group.get(), time);
    }
    auto avg_duration = totalDuration / count;
    std::cout << "Perf done, totalDuration = " << totalDuration << " s"
              << ", count = " << count << ", avg_duration = " << avg_duration << " s" << std::endl;
}

std::string
GetEnv(const std::string& var) {
    const char* val = std::getenv(var.c_str());
    if (val == nullptr) {  // invalid to assign nullptr to std::string
        return "";
    } else {
        return val;
    }
}

void
PrintProcessInfo() {
    std::cout << "PID:" << ::getpid() << std::endl;
    setenv("OMP_WAIT_POLICY", "PASSIVE", 1);
    auto x = GetEnv("OMP_WAIT_POLICY");
    std::cout << "OMP:$" << x << "$" << std::endl;
}

void
BenchmarkSearch(int N, int total_round, int num_segments) {
    std::cout << "Start BenchmarkSearch..." << std::endl;
    std::cout << "Nb=" << N << " dim=" << dim << " total_round=" << total_round << " num_segments=" << num_segments
              << std::endl;
    std::cout << "IndexType=" << knowhere::IndexEnum::INDEX_HNSW << " efConstruction=" << efConstruction << " M=" << M
              << std::endl;
    std::cout << "TopK=" << topK << " nq=" << nq << " ef=" << ef << std::endl;

    Create_Index_HNSW(N, num_segments);
    // start searching...
    std::vector<std::thread> threads;
    for (int i = 0; i < num_segments; i++) {
        threads.emplace_back(Search_Index_HNSW, i, total_round);
    }

    for (auto& th : threads) {
        th.join();
    }
}

int
main() {
    int N = 1000000;
    int round = 500000;
    int num_segments = 10;

    PrintProcessInfo();
    BenchmarkSearch(N, round, num_segments);

    return 0;
}
