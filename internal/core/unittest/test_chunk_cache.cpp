// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "fmt/format.h"
#include "common/Schema.h"
#include "test_utils/DataGen.h"
#include "storage/Util.h"
#include "storage/ChunkCacheSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/MinioChunkManager.h"

TEST(ChunkCacheTest, Read) {
    auto N = 10000;
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;

    auto mmap_dir = "/tmp/mmap";
    auto file_name = std::string("chunk_cache_test/insert_log/1/101/1000000");

    auto sc = milvus::storage::StorageConfig{};
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(sc);
    auto mcm = std::make_unique<milvus::storage::MinioChunkManager>(sc);
    mcm->CreateBucket(sc.bucket_name);
    milvus::storage::ChunkCacheSingleton::GetInstance().Init(mmap_dir);

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_id = schema->AddDebugField(
            "fakevec", milvus::DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField(
            "counter", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto field_data_meta = milvus::storage::FieldDataMeta{
            1, 2, 3, fake_id.get()
    };
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"), fake_id, milvus::DataType::VECTOR_FLOAT, dim, metric_type);

    auto rcm = milvus::storage::RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    auto data = dataset.get_col<float>(fake_id);
    auto data_slices = std::vector<const uint8_t*>{(uint8_t*)data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(data.size())};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(rcm.get(), data_slices, slice_sizes, slice_names, field_data_meta, field_meta);

    auto cc = milvus::storage::ChunkCacheSingleton::GetInstance().GetChunkCache();
    const auto& column = cc->Read(file_name);
    Assert(column->Size() == dim * N * 4);

    cc->Prefetch(file_name);
    auto actual = column->Data();
    for (auto i = 0; i < data.size(); i++) {
        AssertInfo(data[i] == (uint8_t)actual[i], fmt::format("expect {}, actual {}", data[i], actual[i]));
    }

    cc->Remove(file_name);
    rcm->Remove(file_name);
    std::filesystem::remove_all(mmap_dir);

    auto exist = rcm->Exist(file_name);
    Assert(!exist);
    exist = std::filesystem::exists(mmap_dir);
    Assert(!exist);
}