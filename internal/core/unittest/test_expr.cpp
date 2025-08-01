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

#include <algorithm>
#include <any>
#include <boost/format.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <chrono>
#include <roaring/roaring.hh>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/JsonInvertedIndex.h"
#include "index/BitmapIndex.h"
#include "knowhere/comp/index_param.h"
#include "mmap/Types.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/padded_string.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"
#include "index/IndexFactory.h"
#include "exec/Task.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "mmap/Types.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

class ExprTest
    : public ::testing::TestWithParam<
          std::tuple<std::pair<milvus::DataType, knowhere::MetricType>, bool>> {
 public:
    void
    SetUp() override {
        auto param = GetParam();
        data_type = std::get<0>(param).first;  // Get the DataType from the pair
        metric_type =
            std::get<0>(param).second;  // Get the MetricType from the pair
        GROWING_JSON_KEY_STATS_ENABLED =
            std::get<1>(param);  // Get the bool parameter
    }

    // replace the metric type in the plan string with the proper type
    std::vector<char>
    translate_text_plan_with_metric_type(std::string plan) {
        return milvus::segcore::
            replace_metric_and_translate_text_plan_to_binary_plan(
                std::move(plan), metric_type);
    }

    milvus::DataType data_type;
    knowhere::MetricType metric_type;
};

// Instantiate test suite with new bool parameter
INSTANTIATE_TEST_SUITE_P(
    ExprTestSuite,
    ExprTest,
    ::testing::Values(
        std::make_tuple(std::pair(milvus::DataType::VECTOR_FLOAT,
                                  knowhere::metric::L2),
                        false),
        std::make_tuple(std::pair(milvus::DataType::VECTOR_SPARSE_FLOAT,
                                  knowhere::metric::IP),
                        false),
        std::make_tuple(std::pair(milvus::DataType::VECTOR_BINARY,
                                  knowhere::metric::JACCARD),
                        false),
        std::make_tuple(std::pair(milvus::DataType::VECTOR_FLOAT,
                                  knowhere::metric::L2),
                        true),
        std::make_tuple(std::pair(milvus::DataType::VECTOR_SPARSE_FLOAT,
                                  knowhere::metric::IP),
                        true),
        std::make_tuple(std::pair(milvus::DataType::VECTOR_BINARY,
                                  knowhere::metric::JACCARD),
                        true)));

TEST_P(ExprTest, Range) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    std::string raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: GreaterThan
                                        value: <
                                          int64_val: 1
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 100
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 10
                                  round_decimal: 3
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_with_metric_type(raw_plan);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("age", DataType::INT32);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    Assert(plan->tag2field_.at("$0") ==
           schema->get_field_id(FieldName("fakevec")));
}

TEST_P(ExprTest, InvalidRange) {
    SUCCEED();
    std::string raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        op: GreaterThan
                                        value: <
                                          int64_val: 1
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 100
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 10
                                  round_decimal: 3
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_with_metric_type(raw_plan);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("age", DataType::INT32);
    ASSERT_ANY_THROW(
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size()));
}

TEST_P(ExprTest, ShowExecutor) {
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto field_id =
        schema->AddDebugField("fakevec", data_type, 16, metric_type);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->search_info_;

    info.metric_type_ = metric_type;
    info.topk_ = 20;
    info.field_id_ = field_id;
}

TEST_P(ExprTest, TestRange) {
    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 < v && v < 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 <= v && v < 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 < v && v <= 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 <= v && v <= 3000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: GreaterEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v >= 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: GreaterThan,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v > 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: LessEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v <= 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: LessThan,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v < 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: Equal,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v == 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: NotEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v != 2000; }},
    };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < num_iters; ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        EXPECT_EQ(final.size(), N * num_iters);
        EXPECT_EQ(view.size(), num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;

            if (i < num_iters) {
                ASSERT_EQ(view[i], ref) << clause << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestRangeNullable) {
    std::vector<std::tuple<std::string, std::function<bool(int, bool)>>>
        testcases = {
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 < v && v < 3000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v < 3000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 < v && v <= 3000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v <= 3000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: GreaterEqual,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v >= 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: GreaterThan,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v > 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: LessEqual,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v <= 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: LessThan,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v < 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: Equal,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Int64
              >
              op: NotEqual,
              value: <
                int64_val: 2000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v != 2000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 1000000
              >
              upper_value: <
                int64_val: 1000001
              >
        >)",
             [](int v, bool valid) { return false; }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: -1000001
              >
              upper_value: <
                int64_val: -1000000
              >
        >)",
             [](int v, bool valid) { return false; }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: GreaterEqual,
              value: <
                int64_val: 1000000
              >
        >)",
             [](int v, bool valid) { return false; }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: GreaterEqual,
              value: <
                int64_val: -1000000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: LessEqual,
              value: <
                int64_val: 1000000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: LessThan,
              value: <
                int64_val: -1000000
              >
        >)",
             [](int v, bool valid) { return false; }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: Equal,
              value: <
                int64_val: 1000000
              >
        >)",
             [](int v, bool valid) { return false; }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Int8
              >
              op: NotEqual,
              value: <
                int64_val: 1000000
              >
        >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);

    auto nullable_fid_pre_check =
        schema->AddDebugField("pre_check", DataType::INT8, true);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> data_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_data_col = raw_data.get_col<int>(i64_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        data_col.insert(
            data_col.end(), new_data_col.begin(), new_data_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        EXPECT_EQ(final.size(), N * num_iters);
        EXPECT_EQ(view.size(), int(N * num_iters / 2));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = data_col[i];
            auto valid_data = valid_data_col[i];
            auto ref = ref_func(val, valid_data);
            ASSERT_EQ(ans, ref)
                << clause << "@" << i << "!!" << val << "!!" << valid_data;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!" << val << "!!" << valid_data;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryRangeJSON) {
    struct Testcase {
        bool lower_inclusive;
        bool upper_inclusive;
        int64_t lower;
        int64_t upper;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {true, false, 10, 20, {"int"}},
        {true, true, 20, 30, {"int"}},
        {false, true, 30, 40, {"int"}},
        {false, false, 40, 50, {"int"}},
        {true, false, 10, 20, {"double"}},
        {true, true, 20, 30, {"double"}},
        {false, true, 30, 40, {"double"}},
        {false, false, 40, 50, {"double"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            int64_t lower = testcase.lower, upper = testcase.upper;
            if (!testcase.lower_inclusive) {
                lower++;
            }
            if (!testcase.upper_inclusive) {
                upper--;
            }
            return lower <= value && value <= upper;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        milvus::proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(testcase.lower);
        milvus::proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(testcase.upper);
        auto expr = std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            lower_val,
            upper_val,
            testcase.lower_inclusive,
            testcase.upper_inclusive);
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            if (testcase.nested_path[0] == "int") {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryRangeJSONNullable) {
    struct Testcase {
        bool lower_inclusive;
        bool upper_inclusive;
        int64_t lower;
        int64_t upper;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {true, false, 10, 20, {"int"}},
        {true, true, 20, 30, {"int"}},
        {false, true, 30, 40, {"int"}},
        {false, false, 40, 50, {"int"}},
        {true, false, 10, 20, {"double"}},
        {true, true, 20, 30, {"double"}},
        {false, true, 30, 40, {"double"}},
        {false, false, 40, 50, {"double"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col = raw_data.get_col_valid(json_fid);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
            int64_t lower = testcase.lower, upper = testcase.upper;
            if (!testcase.lower_inclusive) {
                lower++;
            }
            if (!testcase.upper_inclusive) {
                upper--;
            }
            return lower <= value && value <= upper;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        RetrievePlanNode plan;
        milvus::proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(testcase.lower);
        milvus::proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(testcase.upper);
        auto expr = std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            lower_val,
            upper_val,
            testcase.lower_inclusive,
            testcase.upper_inclusive);
        BitsetType final;
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            if (testcase.nested_path[0] == "int") {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(pointer)
                               .value();
                auto ref = check(val, valid_data_col[i]);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val, valid_data_col[i]);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestExistsJson) {
    struct Testcase {
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{"A"}},
        {{"int"}},
        {{"double"}},
        {{"B"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](bool value) { return value; };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr =
            std::make_shared<milvus::expr::ExistsExpr>(milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path));
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(pointer);
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestExistsJsonNullable) {
    struct Testcase {
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{"A"}},
        {{"int"}},
        {{"double"}},
        {{"B"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col = raw_data.get_col_valid(json_fid);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](bool value, bool valid) {
            if (!valid) {
                return false;
            }
            return value;
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr =
            std::make_shared<milvus::expr::ExistsExpr>(milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path));
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(pointer);
            auto ref = check(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref);
            }
        }
    }
}

template <typename T>
T
GetValueFromProto(const milvus::proto::plan::GenericValue& value_proto) {
    if constexpr (std::is_same_v<T, bool>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kBoolVal);
        return static_cast<T>(value_proto.bool_val());
    } else if constexpr (std::is_integral_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kInt64Val);
        return static_cast<T>(value_proto.int64_val());
    } else if constexpr (std::is_floating_point_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kFloatVal);
        return static_cast<T>(value_proto.float_val());
    } else if constexpr (std::is_same_v<T, std::string>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kStringVal);
        return static_cast<T>(value_proto.string_val());
    } else if constexpr (std::is_same_v<T, milvus::proto::plan::Array>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kArrayVal);
        return static_cast<T>(value_proto.array_val());
    } else if constexpr (std::is_same_v<T, milvus::proto::plan::GenericValue>) {
        return static_cast<T>(value_proto);
    } else {
        ThrowInfo(milvus::ErrorCode::UnexpectedError,
                  "unsupported generic value type");
    }
};

TEST_P(ExprTest, TestUnaryRangeJson) {
    struct Testcase {
        int64_t val;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{{10, {"int"}},
                                    {20, {"int"}},
                                    {30, {"int"}},
                                    {40, {"int"}},
                                    {1, {"array", "0"}},
                                    {2, {"array", "1"}},
                                    {3, {"array", "2"}}};

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<OpType> ops{
        OpType::Equal,
        OpType::NotEqual,
        OpType::GreaterThan,
        OpType::GreaterEqual,
        OpType::LessThan,
        OpType::LessEqual,
    };
    for (const auto& testcase : testcases) {
        auto check = [&](int64_t value) { return value == testcase.val; };
        std::function<bool(int64_t)> f = check;
        for (auto& op : ops) {
            switch (op) {
                case OpType::Equal: {
                    f = [&](int64_t value) { return value == testcase.val; };
                    break;
                }
                case OpType::NotEqual: {
                    f = [&](int64_t value) { return value != testcase.val; };
                    break;
                }
                case OpType::GreaterEqual: {
                    f = [&](int64_t value) { return value >= testcase.val; };
                    break;
                }
                case OpType::GreaterThan: {
                    f = [&](int64_t value) { return value > testcase.val; };
                    break;
                }
                case OpType::LessEqual: {
                    f = [&](int64_t value) { return value <= testcase.val; };
                    break;
                }
                case OpType::LessThan: {
                    f = [&](int64_t value) { return value < testcase.val; };
                    break;
                }
                default: {
                    ThrowInfo(Unsupported, "unsupported range node");
                }
            }

            auto pointer = milvus::Json::pointer(testcase.nested_path);
            proto::plan::GenericValue value;
            value.set_int64_val(testcase.val);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                value,
                std::vector<proto::plan::GenericValue>{});
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                if (testcase.nested_path[0] == "int" ||
                    testcase.nested_path[0] == "array") {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<int64_t>(pointer)
                            .value();

                    auto ref = f(val);
                    ASSERT_EQ(ans, ref) << "@" << i << "op" << op;

                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                } else {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    {
        struct Testcase {
            double val;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{{1.1, {"double"}},
                                        {2.2, {"double"}},
                                        {3.3, {"double"}},
                                        {4.4, {"double"}},
                                        {1e40, {"double"}}};

        auto schema = std::make_shared<Schema>();
        auto i64_fid = schema->AddDebugField("id", DataType::INT64);
        auto json_fid = schema->AddDebugField("json", DataType::JSON);
        schema->set_primary_field_id(i64_fid);

        auto seg = CreateGrowingSegment(schema, empty_index_meta);
        int N = 1000;
        std::vector<std::string> json_col;
        int num_iters = 1;
        for (int iter = 0; iter < num_iters; ++iter) {
            auto raw_data = DataGen(schema, N, iter);
            auto new_json_col = raw_data.get_col<std::string>(json_fid);

            json_col.insert(
                json_col.end(), new_json_col.begin(), new_json_col.end());
            seg->PreInsert(N);
            seg->Insert(iter * N,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
        auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](double value) { return value == testcase.val; };
            std::function<bool(double)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](double value) { return value == testcase.val; };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](double value) { return value != testcase.val; };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](double value) { return value >= testcase.val; };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](double value) { return value > testcase.val; };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](double value) { return value <= testcase.val; };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](double value) { return value < testcase.val; };
                        break;
                    }
                    default: {
                        ThrowInfo(Unsupported, "unsupported range node");
                    }
                }

                auto pointer = milvus::Json::pointer(testcase.nested_path);
                proto::plan::GenericValue value;
                value.set_float_val(testcase.val);
                auto expr =
                    std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                        milvus::expr::ColumnInfo(
                            json_fid, DataType::JSON, testcase.nested_path),
                        op,
                        value,
                        std::vector<proto::plan::GenericValue>{});
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, expr);
                auto final = ExecuteQueryExpr(
                    plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
                EXPECT_EQ(final.size(), N * num_iters);

                // specify some offsets and do scalar filtering on these offsets
                milvus::exec::OffsetVector offsets;
                offsets.reserve(N * num_iters / 2);
                for (auto i = 0; i < N * num_iters; ++i) {
                    if (i % 2 == 0) {
                        offsets.emplace_back(i);
                    }
                }
                auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                            seg_promote,
                                                            N * num_iters,
                                                            MAX_TIMESTAMP,
                                                            &offsets);
                BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
                EXPECT_EQ(view.size(), N * num_iters / 2);

                for (int i = 0; i < N * num_iters; ++i) {
                    auto ans = final[i];

                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    {
        struct Testcase {
            std::string val;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{
            {"abc", {"string"}},
            {"This is a line break\\nThis is a new line!", {"string"}}};

        auto schema = std::make_shared<Schema>();
        auto i64_fid = schema->AddDebugField("id", DataType::INT64);
        auto json_fid = schema->AddDebugField("json", DataType::JSON);
        schema->set_primary_field_id(i64_fid);

        auto seg = CreateGrowingSegment(schema, empty_index_meta);
        int N = 1000;
        std::vector<std::string> json_col;
        int num_iters = 1;
        for (int iter = 0; iter < num_iters; ++iter) {
            auto raw_data = DataGen(schema, N, iter);
            auto new_json_col = raw_data.get_col<std::string>(json_fid);

            json_col.insert(
                json_col.end(), new_json_col.begin(), new_json_col.end());
            seg->PreInsert(N);
            seg->Insert(iter * N,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
        auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](std::string_view value) {
                return value == testcase.val;
            };
            std::function<bool(std::string_view)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](std::string_view value) {
                            return value == testcase.val;
                        };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](std::string_view value) {
                            return value != testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](std::string_view value) {
                            return value >= testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](std::string_view value) {
                            return value > testcase.val;
                        };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](std::string_view value) {
                            return value <= testcase.val;
                        };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](std::string_view value) {
                            return value < testcase.val;
                        };
                        break;
                    }
                    default: {
                        ThrowInfo(Unsupported, "unsupported range node");
                    }
                }

                auto pointer = milvus::Json::pointer(testcase.nested_path);
                proto::plan::GenericValue value;
                value.set_string_val(testcase.val);
                auto expr =
                    std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                        milvus::expr::ColumnInfo(
                            json_fid, DataType::JSON, testcase.nested_path),
                        op,
                        value,
                        std::vector<proto::plan::GenericValue>{});
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, expr);
                auto final = ExecuteQueryExpr(
                    plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
                EXPECT_EQ(final.size(), N * num_iters);

                // specify some offsets and do scalar filtering on these offsets
                milvus::exec::OffsetVector offsets;
                offsets.reserve(N * num_iters / 2);
                for (auto i = 0; i < N * num_iters; ++i) {
                    if (i % 2 == 0) {
                        offsets.emplace_back(i);
                    }
                }
                auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                            seg_promote,
                                                            N * num_iters,
                                                            MAX_TIMESTAMP,
                                                            &offsets);
                BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
                EXPECT_EQ(view.size(), N * num_iters / 2);

                for (int i = 0; i < N * num_iters; ++i) {
                    auto ans = final[i];

                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<std::string_view>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    struct TestArrayCase {
        proto::plan::GenericValue val;
        std::vector<std::string> nested_path;
    };

    proto::plan::GenericValue value;
    auto* arr = value.mutable_array_val();
    arr->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    arr->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    arr->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    arr->add_array()->CopyFrom(int_val3);

    std::vector<TestArrayCase> array_cases = {{value, {"array"}}};
    for (const auto& testcase : array_cases) {
        auto check = [&](OpType op) {
            if (testcase.nested_path[0] == "array" && op == OpType::Equal) {
                return true;
            }
            return false;
        };
        for (auto& op : ops) {
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                testcase.val,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                auto ref = check(op);
                ASSERT_EQ(ans, ref) << "@" << i << "op" << op;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref) << "@" << i << "op" << op;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryRangeJsonNullable) {
    struct Testcase {
        int64_t val;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{{10, {"int"}},
                                    {20, {"int"}},
                                    {30, {"int"}},
                                    {40, {"int"}},
                                    {1, {"array", "0"}},
                                    {2, {"array", "1"}},
                                    {3, {"array", "2"}}};

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data_col = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    std::vector<OpType> ops{
        OpType::Equal,
        OpType::NotEqual,
        OpType::GreaterThan,
        OpType::GreaterEqual,
        OpType::LessThan,
        OpType::LessEqual,
    };
    for (const auto& testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            return value == testcase.val;
        };
        std::function<bool(int64_t, bool)> f = check;
        for (auto& op : ops) {
            switch (op) {
                case OpType::Equal: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value == testcase.val;
                    };
                    break;
                }
                case OpType::NotEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value != testcase.val;
                    };
                    break;
                }
                case OpType::GreaterEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value >= testcase.val;
                    };
                    break;
                }
                case OpType::GreaterThan: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value > testcase.val;
                    };
                    break;
                }
                case OpType::LessEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value <= testcase.val;
                    };
                    break;
                }
                case OpType::LessThan: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value < testcase.val;
                    };
                    break;
                }
                default: {
                    ThrowInfo(Unsupported, "unsupported range node");
                }
            }

            auto pointer = milvus::Json::pointer(testcase.nested_path);
            proto::plan::GenericValue value;
            value.set_int64_val(testcase.val);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                value,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                if (testcase.nested_path[0] == "int") {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<int64_t>(pointer)
                            .value();
                    auto ref = f(val, valid_data_col[i]);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                } else {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val, valid_data_col[i]);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    struct TestArrayCase {
        proto::plan::GenericValue val;
        std::vector<std::string> nested_path;
    };

    proto::plan::GenericValue value;
    auto* arr = value.mutable_array_val();
    arr->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    arr->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    arr->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    arr->add_array()->CopyFrom(int_val3);

    std::vector<TestArrayCase> array_cases = {{value, {"array"}}};
    for (const auto& testcase : array_cases) {
        auto check = [&](OpType op, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.nested_path[0] == "array" && op == OpType::Equal) {
                return true;
            }
            return false;
        };
        for (auto& op : ops) {
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                testcase.val,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                auto ref = check(op, valid_data_col[i]);
                ASSERT_EQ(ans, ref) << "@" << i << "op" << op;
            }
        }
    }
}

TEST_P(ExprTest, TestTermJson) {
    struct Testcase {
        std::vector<int64_t> term;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{1, 2, 3, 4}, {"int"}},
        {{10, 100, 1000, 10000}, {"int"}},
        {{100, 10000, 9999, 444}, {"int"}},
        {{23, 42, 66, 17, 25}, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                 testcase.term.end());
            return term_set.find(value) != term_set.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue value;
            value.set_int64_val(val);
            values.push_back(value);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<int64_t>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestTermJsonNullable) {
    struct Testcase {
        std::vector<int64_t> term;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{1, 2, 3, 4}, {"int"}},
        {{10, 100, 1000, 10000}, {"int"}},
        {{100, 10000, 9999, 444}, {"int"}},
        {{23, 42, 66, 17, 25}, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        auto new_valid_data_col = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col.insert(valid_data_col.end(),
                              new_valid_data_col.begin(),
                              new_valid_data_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
            std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                 testcase.term.end());
            return term_set.find(value) != term_set.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue value;
            value.set_int64_val(val);
            values.push_back(value);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<int64_t>(pointer)
                           .value();
            auto ref = check(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestTerm) {
    auto vec_2k_3k = [] {
        std::string buf;
        for (int i = 2000; i < 3000; ++i) {
            buf += "values: < int64_val: " + std::to_string(i) + " >\n";
        }
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"(values: <
                int64_val: 2000
            >
            values: <
                int64_val: 3000
            >
        )",
         [](int v) { return v == 2000 || v == 3000; }},
        {R"(values: <
                int64_val: 2000
            >)",
         [](int v) { return v == 2000; }},
        {R"(values: <
                int64_val: 3000
            >)",
         [](int v) { return v == 3000; }},
        {R"()", [](int v) { return false; }},
        {vec_2k_3k, [](int v) { return 2000 <= v && v < 3000; }},
    };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      term_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        @@@@
                                      >
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref) << clause << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestTermNullable) {
    auto vec_2k_3k = [] {
        std::string buf;
        for (int i = 2000; i < 3000; ++i) {
            buf += "values: < int64_val: " + std::to_string(i) + " >\n";
        }
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int, bool)>>>
        testcases = {
            {R"(values: <
                int64_val: 2000
            >
            values: <
                int64_val: 3000
            >
        )",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000 || v == 3000;
             }},
            {R"(values: <
                int64_val: 2000
            >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000;
             }},
            {R"(values: <
                int64_val: 3000
            >)",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 3000;
             }},
            {R"()",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return false;
             }},
            {vec_2k_3k,
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v < 3000;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      term_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        @@@@
                                      >
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_nullable_col = raw_data.get_col<int>(nullable_fid);
        auto new_valid_data_col = raw_data.get_col_valid(nullable_fid);
        valid_data_col.insert(valid_data_col.end(),
                              new_valid_data_col.begin(),
                              new_valid_data_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = nullable_col[i];
            auto ref = ref_func(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref) << clause << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestCall) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    factory.Initialize();

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto varchar_fid = schema->AddDebugField("address", DataType::VARCHAR);
    schema->set_primary_field_id(varchar_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> address_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_address_col = raw_data.get_col<std::string>(varchar_fid);
        address_col.insert(
            address_col.end(), new_address_col.begin(), new_address_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::tuple<std::string, std::function<bool(std::string&)>> test_cases[] = {
        {R"(vector_anns: <
                field_id: 100
                predicates: <
                  call_expr: <
                    function_name: "empty"
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                  >
                >
                query_info: <
                  topk: 10
                  round_decimal: 3
                  metric_type: "L2"
                  search_params: "{\"nprobe\": 10}"
                >
                placeholder_tag: "$0"
     >)",
         [](std::string& v) { return v.empty(); }},
        {R"(vector_anns: <
                field_id: 100
                predicates: <
                  call_expr: <
                    function_name: "starts_with"
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                  >
                >
                query_info: <
                  topk: 10
                  round_decimal: 3
                  metric_type: "L2"
                  search_params: "{\"nprobe\": 10}"
                >
                placeholder_tag: "$0"
     >)",
         [](std::string&) { return true; }}};

    for (auto& [raw_plan, ref_func] : test_cases) {
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            ASSERT_EQ(ans, ref_func(address_col[i]))
                << "@" << i << "!!" << address_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref_func(address_col[i]))
                    << "@" << i << "!!" << address_col[i];
            }
        }
    }

    std::string incorrect_test_cases[] = {
        R"(vector_anns: <
                field_id: 100
                predicates: <
                  call_expr: <
                    function_name: "empty"
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                  >
                >
                query_info: <
                  topk: 10
                  round_decimal: 3
                  metric_type: "L2"
                  search_params: "{\"nprobe\": 10}"
                >
                placeholder_tag: "$0"
               >)",
        R"(vector_anns: <
                field_id: 100
                predicates: <
                  call_expr: <
                    function_name: "starts_with"
                    function_parameters: <
                        column_expr: <
                            info: <
                                field_id: 101
                                data_type: VarChar
                            >
                        >
                    >
                  >
                >
                query_info: <
                  topk: 10
                  round_decimal: 3
                  metric_type: "L2"
                  search_params: "{\"nprobe\": 10}"
                >
                placeholder_tag: "$0"
               >)"};
    for (auto& raw_plan : incorrect_test_cases) {
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        EXPECT_ANY_THROW(
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size()));
    }
}

TEST_P(ExprTest, TestCompare) {
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
            {R"(LessThan)", [](int a, int64_t b) { return a < b; }},
            {R"(LessEqual)", [](int a, int64_t b) { return a <= b; }},
            {R"(GreaterThan)", [](int a, int64_t b) { return a > b; }},
            {R"(GreaterEqual)", [](int a, int64_t b) { return a >= b; }},
            {R"(Equal)", [](int a, int64_t b) { return a == b; }},
            {R"(NotEqual)", [](int a, int64_t b) { return a != b; }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      compare_expr: <
                                        left_column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        right_column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        op: @@@@
                                      >
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> age2_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_age2_col = raw_data.get_col<int64_t>(i64_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        age2_col.insert(
            age2_col.end(), new_age2_col.begin(), new_age2_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val1 = age1_col[i];
            auto val2 = age2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {R"(LessEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {R"(GreaterThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {R"(GreaterEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {R"(Equal)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {R"(NotEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      compare_expr: <
                                        left_column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        right_column_info: <
                                          field_id: 103
                                          data_type: Int64
                                        >
                                        op: @@@@
                                      >
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_nullable_col = raw_data.get_col<int64_t>(nullable_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val1 = age1_col[i];
            auto val2 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareNullable2) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {R"(LessEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {R"(GreaterThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {R"(GreaterEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {R"(Equal)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {R"(NotEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      compare_expr: <
                                        left_column_info: <
                                          field_id: 103
                                          data_type: Int64
                                        >
                                        right_column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: @@@@
                                      >
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_nullable_col = raw_data.get_col<int64_t>(nullable_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val2 = age1_col[i];
            auto val1 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndex) {
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
            {R"(LessThan)", [](int a, int64_t b) { return a < b; }},
            {R"(LessEqual)", [](int a, int64_t b) { return a <= b; }},
            {R"(GreaterThan)", [](int a, int64_t b) { return a > b; }},
            {R"(GreaterEqual)", [](int a, int64_t b) { return a >= b; }},
            {R"(Equal)", [](int a, int64_t b) { return a == b; }},
            {R"(NotEqual)", [](int a, int64_t b) { return a != b; }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: %4%
                                                    >
                                                    right_column_info: <
                                                        field_id: %5%
                                                        data_type: %6%
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 1000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data());
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string =
            boost::format(serialized_expr_plan) % vec_fid.get() % clause %
            i32_fid.get() % proto::schema::DataType_Name(int(DataType::INT32)) %
            i64_fid.get() % proto::schema::DataType_Name(int(DataType::INT64));
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = age32_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {R"(LessEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {R"(GreaterThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {R"(GreaterEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {R"(Equal)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {R"(NotEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: %4%
                                                    >
                                                    right_column_info: <
                                                        field_id: %5%
                                                        data_type: %6%
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto nullable_col = raw_data.get_col<int32_t>(nullable_fid);
    nullable_col[0] = 1000;
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto nullable_index = milvus::index::CreateScalarIndexSort<int32_t>();
    nullable_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(nullable_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(nullable_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % nullable_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT32)) %
                          i64_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT64));
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = nullable_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexNullable2) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {R"(LessEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {R"(GreaterThan)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {R"(GreaterEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {R"(Equal)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {R"(NotEqual)",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: %4%
                                                    >
                                                    right_column_info: <
                                                        field_id: %5%
                                                        data_type: %6%
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto nullable_col = raw_data.get_col<int32_t>(nullable_fid);
    nullable_col[0] = 1000;
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto nullable_index = milvus::index::CreateScalarIndexSort<int32_t>();
    nullable_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(nullable_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(nullable_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % i64_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT64)) %
                          nullable_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT32));
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val2 = nullable_col[i];
            auto val1 = age64_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, test_term_pk_with_sorted) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(
        schema, nullptr, 1, SegcoreConfig::default_config(), true);
    int N = 100000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    std::vector<proto::plan::GenericValue> retrieve_ints;
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i);
        retrieve_ints.push_back(val);
    }
    auto expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(final[i], true);
    }
    for (int i = 10; i < N; ++i) {
        EXPECT_EQ(final[i], false);
    }
    retrieve_ints.clear();
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i + N);
        retrieve_ints.push_back(val);
    }
    expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);

    // specify some offsets and do scalar filtering on these offsets
    milvus::exec::OffsetVector offsets;
    offsets.reserve(N / 2);
    for (auto i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            offsets.emplace_back(i);
        }
    }
    auto col_vec = milvus::test::gen_filter_res(
        plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_EQ(view.size(), N / 2);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(final[i], false);
        if (i % 2 == 0) {
            EXPECT_EQ(view[int(i / 2)], false);
        }
    }
}

TEST_P(ExprTest, TestSealedSegmentGetBatchSize) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str3_fid = schema->AddDebugField("string3", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_1_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_1_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_fid,
                                                        int16_1_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_fid,
                                                        int32_1_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_1_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_fid,
                                                        float_1_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_fid,
                                                        double_1_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str2_fid,
                                                        str3_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_fid,
                                                           int8_1_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    std::cout << "start compare test" << std::endl;
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    std::cout << "end compare test" << std::endl;
}

TEST_P(ExprTest, TestReorder) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, false);
    auto str_array_fid =
        schema->AddDebugField("str_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(pk);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int index) -> expr::TypedExprPtr {
        switch (index) {
            case 0: {
                proto::plan::GenericValue val1;
                val1.set_string_val("xxx");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(str1_fid, DataType::VARCHAR),
                    proto::plan::OpType::Equal,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 1: {
                proto::plan::GenericValue val1;
                val1.set_string_val("xxx");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(json_fid, DataType::JSON, {"int"}),
                    proto::plan::OpType::Equal,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 2: {
                proto::plan::GenericValue val1;
                val1.set_string_val("12");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(str_array_fid, DataType::ARRAY, {"0"}),
                    proto::plan::OpType::Match,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 3: {
                auto expr1 =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_1_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            default:
                ThrowInfo(ErrorCode::UnexpectedError, "not implement");
        }
    };
    BitsetType final;
    auto expr = build_expr(0);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(1);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(2);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(3);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
}

TEST_P(ExprTest, TestCompareExprNullable) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_nullable_fid =
        schema->AddDebugField("bool1", DataType::BOOL, true);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int81", DataType::INT8, true);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int161", DataType::INT16, true);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int321", DataType::INT32, true);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int641", DataType::INT64, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_nullable_fid =
        schema->AddDebugField("float1", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_nullable_fid =
        schema->AddDebugField("double1", DataType::DOUBLE, true);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str_nullable_fid =
        schema->AddDebugField("string3", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_nullable_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_nullable_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_fid,
                                                        int16_nullable_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_fid,
                                                        int32_nullable_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_nullable_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_fid,
                                                        float_nullable_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_fid,
                                                        double_nullable_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str2_fid,
                                                        str_nullable_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_fid,
                                                           int8_nullable_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    std::cout << "start compare test" << std::endl;
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    std::cout << "end compare test" << std::endl;
}

TEST_P(ExprTest, TestCompareExprNullable2) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_nullable_fid =
        schema->AddDebugField("bool1", DataType::BOOL, true);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int81", DataType::INT8, true);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int161", DataType::INT16, true);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int321", DataType::INT32, true);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int641", DataType::INT64, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_nullable_fid =
        schema->AddDebugField("float1", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_nullable_fid =
        schema->AddDebugField("double1", DataType::DOUBLE, true);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str_nullable_fid =
        schema->AddDebugField("string3", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_nullable_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_nullable_fid,
                                                        int8_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_nullable_fid,
                                                        int16_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_nullable_fid,
                                                        int32_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_nullable_fid,
                                                        int64_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_nullable_fid,
                                                        float_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_nullable_fid,
                                                        double_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str_nullable_fid,
                                                        str2_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_nullable_fid,
                                                           int8_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    std::cout << "start compare test" << std::endl;
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    std::cout << "end compare test" << std::endl;
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeExpr_forbigint_mod) {
    // test (bigint mod 10 == 0)
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto insert_data = std::make_unique<InsertRecordProto>();
    {
        // insert pk fid
        auto field_meta = schema->operator[](int64_fid);
        std::vector<int64_t> data(N);
        for (int i = 0; i < N; i++) {
            data[i] = i;
        }
        InsertCol(insert_data.get(), data, field_meta, false);
    }

    BitsetType expect(N, false);
    {
        auto field_meta = schema->operator[](json_fid);
        std::vector<std::string> data(N);

        auto start = 1ULL << 54;
        for (int i = 0; i < N; i++) {
            data[i] = R"({"meta":)" + std::to_string(start + i) + "}";
            std::cout << "data[i]: " << data[i] << std::endl;
            if ((start + i) % 10 == 0) {
                expect.set(i);
            }
        }
        InsertCol(insert_data.get(), data, field_meta, false);
    }

    GeneratedData raw_data;
    raw_data.schema_ = schema;
    raw_data.raw_ = insert_data.release();
    raw_data.raw_->set_num_rows(N);
    for (int i = 0; i < N; ++i) {
        raw_data.row_ids_.push_back(i);
        raw_data.timestamps_.push_back(i);
    }

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    proto::plan::GenericValue val1;
    val1.set_int64_val(10);
    proto::plan::GenericValue val2;
    val2.set_int64_val(0);
    auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"meta"}),
        proto::plan::OpType::Equal,
        proto::plan::ArithOpType::Mod,
        val2,
        val1);

    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), expect.size())
        << "final size: " << final.size() << " expect size: " << expect.size();
    for (auto i = 0; i < final.size(); i++) {
        EXPECT_EQ(final[i], expect[i])
            << "i: " << i << " final: " << final[i] << " expect: " << expect[i];
    }
}

TEST_P(ExprTest, TestMutiInConvert) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, false);
    auto str_array_fid =
        schema->AddDebugField("str_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(pk);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int index) -> expr::TypedExprPtr {
        switch (index) {
            case 0: {
                proto::plan::GenericValue val1;
                val1.set_int64_val(100);
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val1);
                proto::plan::GenericValue val2;
                val2.set_int64_val(200);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val2);
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
                proto::plan::GenericValue val3;
                val3.set_int64_val(300);
                auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val3);
                return std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
            };
            default:
                ThrowInfo(ErrorCode::UnexpectedError, "not implement");
        }
    };

    auto expr = build_expr(0);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final1 = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    OPTIMIZE_EXPR_ENABLED = false;
    auto final2 = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final1.size(), final2.size());
    for (auto i = 0; i < final1.size(); i++) {
        EXPECT_EQ(final1[i], final2[i]);
    }
}

TEST(Expr, TestExprPerformance) {
    GTEST_SKIP() << "Skip performance test, open it when test performance";
    auto schema = std::make_shared<Schema>();
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    enum ExprType {
        UnaryRangeExpr = 0,
        TermExprImpl = 1,
        CompareExpr = 2,
        LogicalUnaryExpr = 3,
        BinaryRangeExpr = 4,
        LogicalBinaryExpr = 5,
        BinaryArithOpEvalRangeExpr = 6,
    };

    auto build_unary_range_expr = [&](DataType data_type,
                                      int64_t value) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_int64_val(value);
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_float_val(float(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_string_val(std::to_string(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_binary_range_expr = [&](DataType data_type,
                                       int64_t low,
                                       int64_t high) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(low);
            proto::plan::GenericValue val2;
            val2.set_int64_val(high);
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(low));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(high));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_string_val(std::to_string(low));
            proto::plan::GenericValue val2;
            val2.set_string_val(std::to_string(low));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_term_expr =
        [&](DataType data_type,
            std::vector<int64_t> in_vals) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_int64_val(v);
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsFloatDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_float_val(float(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsStringDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_string_val(std::to_string(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_compare_expr = [&](DataType data_type) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type) || IsFloatDataType(data_type) ||
            IsStringDataType(data_type)) {
            return std::make_shared<expr::CompareExpr>(
                fids[data_type],
                fids[data_type],
                data_type,
                data_type,
                proto::plan::OpType::LessThan);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_logical_unary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    };

    auto build_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 10);
        auto child2_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
    };

    auto build_multi_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 100);
        auto child2_expr = build_unary_range_expr(data_type, 100);
        auto child3_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child4_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child5_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        auto child6_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child5_expr, child6_expr);
    };

    auto build_arith_op_expr = [&](DataType data_type,
                                   int64_t right_val,
                                   int64_t val) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(right_val);
            proto::plan::GenericValue val2;
            val2.set_int64_val(val);
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(right_val));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(val));
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto test_case_base = [=, &seg](expr::TypedExprPtr expr) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        std::cout << expr->ToString() << std::endl;
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 100; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);
        }
        std::cout << "cost: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                             .count() /
                         100.0
                  << "us" << std::endl;
    };

    std::cout << "test unary range operator" << std::endl;
    auto expr = build_unary_range_expr(DataType::INT8, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT16, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT32, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT64, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::FLOAT, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::DOUBLE, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::VARCHAR, 10);
    test_case_base(expr);

    std::cout << "test binary range operator" << std::endl;
    expr = build_binary_range_expr(DataType::INT8, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT16, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT32, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT64, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::FLOAT, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::DOUBLE, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::VARCHAR, 10, 100);
    test_case_base(expr);

    std::cout << "test compare expr operator" << std::endl;
    expr = build_compare_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_compare_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_compare_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_compare_expr(DataType::VARCHAR);
    test_case_base(expr);

    std::cout << "test artih op val operator" << std::endl;
    expr = build_arith_op_expr(DataType::INT8, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT16, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT32, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT64, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::FLOAT, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::DOUBLE, 10, 100);
    test_case_base(expr);

    std::cout << "test logical unary expr operator" << std::endl;
    expr = build_logical_unary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::VARCHAR);
    test_case_base(expr);

    std::cout << "test logical binary expr operator" << std::endl;
    expr = build_logical_binary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::VARCHAR);
    test_case_base(expr);

    std::cout << "test multi logical binary expr operator" << std::endl;
    expr = build_multi_logical_binary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::VARCHAR);
    test_case_base(expr);
}

TEST(Expr, TestExprNOT) {
    auto schema = std::make_shared<Schema>();
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, true);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16, true);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, true);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE, true);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    auto seg = CreateSealedSegment(schema);
    FixedVector<bool> valid_data_i8;
    FixedVector<bool> valid_data_i16;
    FixedVector<bool> valid_data_i32;
    FixedVector<bool> valid_data_i64;
    FixedVector<bool> valid_data_str;
    FixedVector<bool> valid_data_float;
    FixedVector<bool> valid_data_double;
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    valid_data_i8 = raw_data.get_col_valid(int8_fid);
    valid_data_i16 = raw_data.get_col_valid(int16_fid);
    valid_data_i32 = raw_data.get_col_valid(int32_fid);
    valid_data_i64 = raw_data.get_col_valid(int64_fid);
    valid_data_str = raw_data.get_col_valid(str2_fid);
    valid_data_float = raw_data.get_col_valid(float_fid);
    valid_data_double = raw_data.get_col_valid(double_fid);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    enum ExprType {
        UnaryRangeExpr = 0,
        TermExprImpl = 1,
        CompareExpr = 2,
        LogicalUnaryExpr = 3,
        BinaryRangeExpr = 4,
        LogicalBinaryExpr = 5,
        BinaryArithOpEvalRangeExpr = 6,
    };

    auto build_unary_range_expr = [&](DataType data_type,
                                      int64_t value) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_int64_val(value);
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_float_val(float(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_string_val(std::to_string(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_binary_range_expr = [&](DataType data_type,
                                       int64_t low,
                                       int64_t high) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(low);
            proto::plan::GenericValue val2;
            val2.set_int64_val(high);
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(low));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(high));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_string_val(std::to_string(low));
            proto::plan::GenericValue val2;
            val2.set_string_val(std::to_string(low));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_term_expr =
        [&](DataType data_type,
            std::vector<int64_t> in_vals) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_int64_val(v);
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsFloatDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_float_val(float(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsStringDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_string_val(std::to_string(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_compare_expr = [&](DataType data_type) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type) || IsFloatDataType(data_type) ||
            IsStringDataType(data_type)) {
            return std::make_shared<expr::CompareExpr>(
                fids[data_type],
                fids[data_type],
                data_type,
                data_type,
                proto::plan::OpType::LessThan);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 10);
        auto child2_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
    };

    auto build_multi_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 100);
        auto child2_expr = build_unary_range_expr(data_type, 100);
        auto child3_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child4_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child5_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        auto child6_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child5_expr, child6_expr);
    };

    auto build_arith_op_expr = [&](DataType data_type,
                                   int64_t right_val,
                                   int64_t val) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(right_val);
            proto::plan::GenericValue val2;
            val2.set_int64_val(val);
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(right_val));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(val));
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_logical_unary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    };

    auto test_ans = [=, &seg](expr::TypedExprPtr expr,
                              FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; i++) {
            if (!valid_data[i]) {
                EXPECT_EQ(final[i], false);
                if (i % 2 == 0) {
                    EXPECT_EQ(view[int(i / 2)], false);
                }
            }
        }
    };

    auto expr = build_unary_range_expr(DataType::INT8, 10);
    test_ans(expr, valid_data_i8);
    expr = build_unary_range_expr(DataType::INT16, 10);
    test_ans(expr, valid_data_i16);
    expr = build_unary_range_expr(DataType::INT32, 10);
    test_ans(expr, valid_data_i32);
    expr = build_unary_range_expr(DataType::INT64, 10);
    test_ans(expr, valid_data_i64);
    expr = build_unary_range_expr(DataType::FLOAT, 10);
    test_ans(expr, valid_data_float);
    expr = build_unary_range_expr(DataType::DOUBLE, 10);
    test_ans(expr, valid_data_double);
    expr = build_unary_range_expr(DataType::VARCHAR, 10);
    test_ans(expr, valid_data_str);

    expr = build_binary_range_expr(DataType::INT8, 10, 100);
    test_ans(expr, valid_data_i8);
    expr = build_binary_range_expr(DataType::INT16, 10, 100);
    test_ans(expr, valid_data_i16);
    expr = build_binary_range_expr(DataType::INT32, 10, 100);
    test_ans(expr, valid_data_i32);
    expr = build_binary_range_expr(DataType::INT64, 10, 100);
    test_ans(expr, valid_data_i64);
    expr = build_binary_range_expr(DataType::FLOAT, 10, 100);
    test_ans(expr, valid_data_float);
    expr = build_binary_range_expr(DataType::DOUBLE, 10, 100);
    test_ans(expr, valid_data_double);
    expr = build_binary_range_expr(DataType::VARCHAR, 10, 100);
    test_ans(expr, valid_data_str);

    expr = build_compare_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_compare_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_compare_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_compare_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_compare_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_compare_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_compare_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_arith_op_expr(DataType::INT8, 10, 100);
    test_ans(expr, valid_data_i8);
    expr = build_arith_op_expr(DataType::INT16, 10, 100);
    test_ans(expr, valid_data_i16);
    expr = build_arith_op_expr(DataType::INT32, 10, 100);
    test_ans(expr, valid_data_i32);
    expr = build_arith_op_expr(DataType::INT64, 10, 100);
    test_ans(expr, valid_data_i64);
    expr = build_arith_op_expr(DataType::FLOAT, 10, 100);
    test_ans(expr, valid_data_float);
    expr = build_arith_op_expr(DataType::DOUBLE, 10, 100);
    test_ans(expr, valid_data_double);

    expr = build_logical_unary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_logical_unary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_logical_unary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_logical_unary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_logical_unary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_logical_unary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_logical_unary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_logical_binary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_logical_binary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_logical_binary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_logical_binary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_logical_binary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_logical_binary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_logical_binary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_multi_logical_binary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_multi_logical_binary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_multi_logical_binary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_multi_logical_binary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_multi_logical_binary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_multi_logical_binary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_multi_logical_binary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);
}

TEST_P(ExprTest, test_term_pk) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    std::vector<proto::plan::GenericValue> retrieve_ints;
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i);
        retrieve_ints.push_back(val);
    }
    auto expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(final[i], true);
    }
    for (int i = 10; i < N; ++i) {
        EXPECT_EQ(final[i], false);
    }
    retrieve_ints.clear();
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i + N);
        retrieve_ints.push_back(val);
    }
    expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);

    // specify some offsets and do scalar filtering on these offsets
    milvus::exec::OffsetVector offsets;
    offsets.reserve(N / 2);
    for (auto i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            offsets.emplace_back(i);
        }
    }
    auto col_vec = milvus::test::gen_filter_res(
        plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_EQ(view.size(), N / 2);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(final[i], false);
        if (i % 2 == 0) {
            EXPECT_EQ(view[int(i / 2)], false);
        }
    }
}

TEST_P(ExprTest, TestGrowingSegmentGetBatchSize) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    proto::plan::GenericValue val;
    val.set_int64_val(10);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int8_fid, DataType::INT8),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto plan_node =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

    std::vector<int64_t> test_batch_size = {
        8192, 10240, 20480, 30720, 40960, 102400, 204800, 307200};

    for (const auto& batch_size : test_batch_size) {
        EXEC_EVAL_EXPR_BATCH_SIZE = batch_size;
        auto plan = plan::PlanFragment(plan_node);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            "query id", seg.get(), N, MAX_TIMESTAMP);

        auto task =
            milvus::exec::Task::Create("task_expr", plan, 0, query_context);
        auto last_num = N % batch_size;
        auto iter_num = last_num == 0 ? N / batch_size : N / batch_size + 1;
        int iter = 0;
        for (;;) {
            auto result = task->Next();
            if (!result) {
                break;
            }
            auto childrens = result->childrens();
            if (++iter != iter_num) {
                EXPECT_EQ(childrens[0]->size(), batch_size);
            } else {
                EXPECT_EQ(childrens[0]->size(), last_num);
            }
        }
    }
}

TEST_P(ExprTest, TestConjuctExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int l, int r) -> expr::TypedExprPtr {
        ::milvus::proto::plan::GenericValue value;
        value.set_int64_val(l);
        auto left = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});
        value.set_int64_val(r);
        auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, left, right);
    };

    std::vector<std::pair<int, int>> test_case = {
        {100, 0}, {0, 100}, {8192, 8194}};
    for (auto& pair : test_case) {
        std::cout << pair.first << "|" << pair.second << std::endl;
        auto expr = build_expr(pair.first, pair.second);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(final[i], pair.first < i && i < pair.second) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[int(i / 2)], pair.first < i && i < pair.second)
                    << i;
            }
        }
    }
}

TEST_P(ExprTest, TestConjuctExprNullable) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int8_nullable", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int16_nullable", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int32_nullable", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int64_nullable", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int l, int r) -> expr::TypedExprPtr {
        ::milvus::proto::plan::GenericValue value;
        value.set_int64_val(l);
        auto left = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_nullable_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});
        value.set_int64_val(r);
        auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_nullable_fid, DataType::INT64),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, left, right);
    };

    std::vector<std::pair<int, int>> test_case = {
        {100, 0}, {0, 100}, {8192, 8194}};
    for (auto& pair : test_case) {
        std::cout << pair.first << "|" << pair.second << std::endl;
        auto expr = build_expr(pair.first, pair.second);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(final[i], pair.first < i && i < pair.second) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[int(i / 2)], pair.first < i && i < pair.second)
                    << i;
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};
    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.second) << std::endl;
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(10);
        } else {
            val.set_int64_val(10);
        }
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 10; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 10.0 << "us" << std::endl;
    }
}

TEST_P(ExprTest, TestBinaryRangeBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.second) << std::endl;
        proto::plan::GenericValue lower;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            lower.set_float_val(10);
        } else {
            lower.set_int64_val(10);
        }
        proto::plan::GenericValue upper;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            upper.set_float_val(45);
        } else {
            upper.set_int64_val(45);
        }
        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            lower,
            upper,
            true,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 10; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 10.0 << "us" << std::endl;
    }
}

TEST_P(ExprTest, TestLogicalUnaryBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.second) << std::endl;
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(10);
        } else {
            val.set_int64_val(10);
        }
        auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 50; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 50.0 << "us" << std::endl;
    }
}

TEST_P(ExprTest, TestBinaryLogicalBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.second) << std::endl;
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(-1000000);
        } else {
            val.set_int64_val(-1000000);
        }
        proto::plan::GenericValue val1;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val1.set_float_val(-100);
        } else {
            val1.set_int64_val(-100);
        }
        auto child1_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::LessThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto child2_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::NotEqual,
            val1,
            std::vector<proto::plan::GenericValue>{});
        auto expr = std::make_shared<const expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 50; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 50.0 << "us" << std::endl;
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeBenchExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.second) << std::endl;
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(100);
        } else {
            val.set_int64_val(100);
        }
        proto::plan::GenericValue right;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            right.set_float_val(10);
        } else {
            right.set_int64_val(10);
        }
        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Add,
            val,
            right);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 50; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 50.0 << "us" << std::endl;
    }
}

TEST(BitmapIndexTest, PatternMatchTest) {
    // Initialize bitmap index
    using namespace milvus::index;
    BitmapIndex<std::string> index;

    // Add test data
    std::vector<std::string> data = {"apple", "banana", "orange", "pear"};

    // Build index
    index.Build(data.size(), data.data(), nullptr);

    // Create test datasets with different operators
    auto prefix_dataset = std::make_shared<Dataset>();
    prefix_dataset->Set(OPERATOR_TYPE, OpType::PrefixMatch);
    prefix_dataset->Set(MATCH_VALUE, std::string("a"));

    auto contains_dataset = std::make_shared<Dataset>();
    contains_dataset->Set(OPERATOR_TYPE, OpType::InnerMatch);
    contains_dataset->Set(MATCH_VALUE, std::string("an"));

    auto posix_dataset = std::make_shared<Dataset>();
    posix_dataset->Set(OPERATOR_TYPE, OpType::PostfixMatch);
    posix_dataset->Set(MATCH_VALUE, std::string("a"));

    // Execute queries
    auto prefix_result = index.Query(prefix_dataset);
    auto contains_result = index.Query(contains_dataset);
    auto posix_result = index.Query(posix_dataset);

    // Verify results
    EXPECT_TRUE(prefix_result[0]);
    EXPECT_FALSE(prefix_result[2]);

    EXPECT_FALSE(contains_result[0]);
    EXPECT_TRUE(contains_result[1]);
    EXPECT_TRUE(contains_result[2]);

    EXPECT_FALSE(posix_result[0]);
    EXPECT_TRUE(posix_result[1]);
    EXPECT_FALSE(posix_result[2]);

    auto prefix_result2 =
        index.PatternMatch(std::string("a"), OpType::PrefixMatch);
    auto contains_result2 =
        index.PatternMatch(std::string("an"), OpType::InnerMatch);
    auto posix_result2 =
        index.PatternMatch(std::string("a"), OpType::PostfixMatch);

    EXPECT_TRUE(prefix_result == prefix_result2);
    EXPECT_TRUE(contains_result == contains_result2);
    EXPECT_TRUE(posix_result == posix_result2);
}

TEST(Expr, TestExprNull) {
    auto schema = std::make_shared<Schema>();
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL, true);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, true);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16, true);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, true);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT, true);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE, true);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::BOOL, bool_fid},
                                        {DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    std::map<DataType, FieldId> fids_not_nullable = {
        {DataType::BOOL, bool_1_fid},
        {DataType::INT8, int8_1_fid},
        {DataType::INT16, int16_1_fid},
        {DataType::INT32, int32_1_fid},
        {DataType::INT64, int64_1_fid},
        {DataType::VARCHAR, str1_fid},
        {DataType::FLOAT, float_1_fid},
        {DataType::DOUBLE, double_1_fid}};

    auto seg = CreateSealedSegment(schema);
    FixedVector<bool> valid_data_bool;
    FixedVector<bool> valid_data_i8;
    FixedVector<bool> valid_data_i16;
    FixedVector<bool> valid_data_i32;
    FixedVector<bool> valid_data_i64;
    FixedVector<bool> valid_data_str;
    FixedVector<bool> valid_data_float;
    FixedVector<bool> valid_data_double;

    int N = 1000;
    auto raw_data = DataGen(schema, N);
    valid_data_bool = raw_data.get_col_valid(bool_fid);
    valid_data_i8 = raw_data.get_col_valid(int8_fid);
    valid_data_i16 = raw_data.get_col_valid(int16_fid);
    valid_data_i32 = raw_data.get_col_valid(int32_fid);
    valid_data_i64 = raw_data.get_col_valid(int64_fid);
    valid_data_str = raw_data.get_col_valid(str2_fid);
    valid_data_float = raw_data.get_col_valid(float_fid);
    valid_data_double = raw_data.get_col_valid(double_fid);

    FixedVector<bool> valid_data_all_true(N, true);

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    auto build_nullable_expr = [&](DataType data_type,
                                   NullExprType op) -> expr::TypedExprPtr {
        return std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(fids[data_type], data_type, {}, true), op);
    };

    auto build_not_nullable_expr = [&](DataType data_type,
                                       NullExprType op) -> expr::TypedExprPtr {
        return std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(
                fids_not_nullable[data_type], data_type, {}, false),
            op);
    };

    auto test_is_null_ans = [=, &seg](expr::TypedExprPtr expr,
                                      FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);
        for (int i = 0; i < N; i++) {
            EXPECT_NE(final[i], valid_data[i]);
        }
    };

    auto test_is_not_null_ans = [=, &seg](expr::TypedExprPtr expr,
                                          FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);
        for (int i = 0; i < N; i++) {
            EXPECT_EQ(final[i], valid_data[i]);
        }
    };

    auto expr = build_nullable_expr(DataType::BOOL,
                                    proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_bool);
    expr = build_nullable_expr(DataType::INT8,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i8);
    expr = build_nullable_expr(DataType::INT16,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i16);
    expr = build_nullable_expr(DataType::INT32,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i32);
    expr = build_nullable_expr(DataType::INT64,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i64);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::BOOL,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_bool);
    expr = build_nullable_expr(DataType::INT8,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i8);
    expr = build_nullable_expr(DataType::INT16,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i16);
    expr = build_nullable_expr(DataType::INT32,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i32);
    expr = build_nullable_expr(DataType::INT64,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i64);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_double);
    //not nullable expr
    expr = build_not_nullable_expr(DataType::BOOL,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT8,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT16,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT32,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT64,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::BOOL,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT8,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT16,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT32,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT64,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
}

TEST_P(ExprTest, TestCompareExprBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);

    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<
        std::pair<std::pair<FieldId, DataType>, std::pair<FieldId, DataType>>>
        test_cases = {
            {{int8_fid, DataType::INT8}, {int8_1_fid, DataType::INT8}},
            {{int16_fid, DataType::INT16}, {int16_fid, DataType::INT16}},
            {{int32_fid, DataType::INT32}, {int32_1_fid, DataType::INT32}},
            {{int64_fid, DataType::INT64}, {int64_1_fid, DataType::INT64}},
            {{float_fid, DataType::FLOAT}, {float_1_fid, DataType::FLOAT}},
            {{double_fid, DataType::DOUBLE}, {double_1_fid, DataType::DOUBLE}}};

    for (const auto& pair : test_cases) {
        std::cout << "start test type:" << int(pair.first.second) << std::endl;
        proto::plan::GenericValue lower;
        auto expr = std::make_shared<expr::CompareExpr>(pair.first.first,
                                                        pair.second.first,
                                                        pair.first.second,
                                                        pair.second.second,
                                                        OpType::LessThan);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        int64_t all_cost = 0;
        for (int i = 0; i < 10; i++) {
            auto start = std::chrono::steady_clock::now();
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            all_cost += std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        }
        std::cout << " cost: " << all_cost / 10 << "us" << std::endl;
    }
}

TEST_P(ExprTest, TestRefactorExprs) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    enum ExprType {
        UnaryRangeExpr = 0,
        TermExprImpl = 1,
        CompareExpr = 2,
        LogicalUnaryExpr = 3,
        BinaryRangeExpr = 4,
        LogicalBinaryExpr = 5,
        BinaryArithOpEvalRangeExpr = 6,
    };

    auto build_expr = [&](enum ExprType test_type,
                          int n) -> expr::TypedExprPtr {
        switch (test_type) {
            case UnaryRangeExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                return std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
            }
            case TermExprImpl: {
                std::vector<proto::plan::GenericValue> retrieve_ints;
                for (int i = 0; i < n; ++i) {
                    proto::plan::GenericValue val;
                    val.set_float_val(i);
                    retrieve_ints.push_back(val);
                }
                return std::make_shared<expr::TermFilterExpr>(
                    expr::ColumnInfo(double_fid, DataType::DOUBLE),
                    retrieve_ints);
            }
            case CompareExpr: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_1_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case BinaryRangeExpr: {
                proto::plan::GenericValue lower;
                lower.set_int64_val(10);
                proto::plan::GenericValue upper;
                upper.set_int64_val(45);
                return std::make_shared<expr::BinaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    lower,
                    upper,
                    true,
                    true);
            }
            case LogicalUnaryExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                return std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
            }
            case LogicalBinaryExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                auto child1_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                auto child2_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::NotEqual,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                ;
                return std::make_shared<const expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And,
                    child1_expr,
                    child2_expr);
            }
            case BinaryArithOpEvalRangeExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(100);
                proto::plan::GenericValue right;
                right.set_int64_val(10);
                return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::Equal,
                    proto::plan::ArithOpType::Add,
                    val,
                    right);
            }
            default: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                return std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
            }
        }
    };
    auto test_case = [&](int n) {
        auto expr = build_expr(UnaryRangeExpr, n);
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        std::cout << "start test" << std::endl;
        auto start = std::chrono::steady_clock::now();
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        std::cout << n << "cost: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << "us" << std::endl;
    };
    test_case(3);
    test_case(10);
    test_case(20);
    test_case(30);
    test_case(50);
    test_case(100);
    test_case(200);
    // test_case(500);
}

TEST_P(ExprTest, TestCompareWithScalarIndexMaris) {
    std::vector<
        std::tuple<std::string, std::function<bool(std::string, std::string)>>>
        testcases = {
            {R"(LessThan)",
             [](std::string a, std::string b) { return a.compare(b) < 0; }},
            {R"(LessEqual)",
             [](std::string a, std::string b) { return a.compare(b) <= 0; }},
            {R"(GreaterThan)",
             [](std::string a, std::string b) { return a.compare(b) > 0; }},
            {R"(GreaterEqual)",
             [](std::string a, std::string b) { return a.compare(b) >= 0; }},
            {R"(Equal)",
             [](std::string a, std::string b) { return a.compare(b) == 0; }},
            {R"(NotEqual)",
             [](std::string a, std::string b) { return a.compare(b) != 0; }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: VarChar
                                                    >
                                                    right_column_info: <
                                                        field_id: %4%
                                                        data_type: VarChar
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto str2_col = raw_data.get_col<std::string>(str2_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, str2_col.data());
    load_index_info.field_id = str2_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % str1_fid.get() % str2_fid.get();
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        //         std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = str2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexMarisNullable) {
    std::vector<std::tuple<std::string,
                           std::function<bool(std::string, std::string, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) < 0;
             }},
            {R"(LessEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) <= 0;
             }},
            {R"(GreaterThan)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) > 0;
             }},
            {R"(GreaterEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) >= 0;
             }},
            {R"(Equal)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) == 0;
             }},
            {R"(NotEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) != 0;
             }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: VarChar
                                                    >
                                                    right_column_info: <
                                                        field_id: %4%
                                                        data_type: VarChar
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto nullable_fid =
        schema->AddDebugField("nullable_fid", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto nullable_col = raw_data.get_col<std::string>(nullable_fid);
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % str1_fid.get() % nullable_fid.get();
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        //         std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexMarisNullable2) {
    std::vector<std::tuple<std::string,
                           std::function<bool(std::string, std::string, bool)>>>
        testcases = {
            {R"(LessThan)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) < 0;
             }},
            {R"(LessEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) <= 0;
             }},
            {R"(GreaterThan)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) > 0;
             }},
            {R"(GreaterEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) >= 0;
             }},
            {R"(Equal)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) == 0;
             }},
            {R"(NotEqual)",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) != 0;
             }},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: VarChar
                                                    >
                                                    right_column_info: <
                                                        field_id: %4%
                                                        data_type: VarChar
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto nullable_fid =
        schema->AddDebugField("nullable_fid", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto nullable_col = raw_data.get_col<std::string>(nullable_fid);
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % nullable_fid.get() % str1_fid.get();
        auto binary_plan =
            translate_text_plan_with_metric_type(dsl_string.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        //         std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = nullable_col[i];
            auto val2 = str1_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRange) {
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>> testcases = {
        // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 4
                  >
                  op: Equal
                  value: <
                    int64_val: 8
                  >
             >)",
         [](int8_t v) { return (v + 4) == 8; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 500
                  >
                  op: Equal
                  value: <
                    int64_val: 1500
                  >
             >)",
         [](int16_t v) { return (v - 500) == 1500; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4000
                  >
             >)",
         [](int32_t v) { return (v * 2) == 4000; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int64_t v) { return (v / 2) == 1000; },
         DataType::INT64},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: Equal
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) == 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) == 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v + 500) == 2500; },
         DataType::DOUBLE},
        // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) != 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v - 500) != 2500; },
         DataType::DOUBLE},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
         [](int8_t v) { return (v * 2) != 2; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int16_t v) { return (v / 2) != 1000; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: NotEqual
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) != 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
         [](int64_t v) { return (v + 500) != 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterThan
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) > 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterThan
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v - 500) > 2500; },
         DataType::DOUBLE},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
         [](int8_t v) { return (v * 2) > 2; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int16_t v) { return (v / 2) > 1000; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) > 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2500
                  >
             >)",
         [](int64_t v) { return (v + 500) > 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) >= 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v - 500) >= 2500; },
         DataType::DOUBLE},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
         [](int8_t v) { return (v * 2) >= 2; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int16_t v) { return (v / 2) >= 1000; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) >= 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
         [](int64_t v) { return (v + 500) >= 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: LessThan
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) < 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: LessThan
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v - 500) < 2500; },
         DataType::DOUBLE},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
         [](int8_t v) { return (v * 2) < 2; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int16_t v) { return (v / 2) < 1000; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: LessThan
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) < 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: LessThan
                  value: <
                    int64_val: 2500
                  >
             >)",
         [](int64_t v) { return (v + 500) < 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: LessEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](float v) { return (v + 500) <= 2500; },
         DataType::FLOAT},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: LessEqual
                  value: <
                    float_val: 2500
                  >
             >)",
         [](double v) { return (v - 500) <= 2500; },
         DataType::DOUBLE},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
         [](int8_t v) { return (v * 2) <= 2; },
         DataType::INT8},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
         [](int16_t v) { return (v / 2) <= 1000; },
         DataType::INT16},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: LessEqual
                  value: <
                    int64_val: 0
                  >
             >)",
         [](int32_t v) { return (v % 100) <= 0; },
         DataType::INT32},
        {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
         [](int64_t v) { return (v + 500) <= 2500; },
         DataType::INT64},
    };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int8_t> age8_col;
    std::vector<int16_t> age16_col;
    std::vector<int32_t> age32_col;
    std::vector<int64_t> age64_col;
    std::vector<float> age_float_col;
    std::vector<double> age_double_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto new_age8_col = raw_data.get_col<int8_t>(i8_fid);
        auto new_age16_col = raw_data.get_col<int16_t>(i16_fid);
        auto new_age32_col = raw_data.get_col<int32_t>(i32_fid);
        auto new_age64_col = raw_data.get_col<int64_t>(i64_fid);
        auto new_age_float_col = raw_data.get_col<float>(float_fid);
        auto new_age_double_col = raw_data.get_col<double>(double_fid);

        age8_col.insert(
            age8_col.end(), new_age8_col.begin(), new_age8_col.end());
        age16_col.insert(
            age16_col.end(), new_age16_col.begin(), new_age16_col.end());
        age32_col.insert(
            age32_col.end(), new_age32_col.begin(), new_age32_col.end());
        age64_col.insert(
            age64_col.end(), new_age64_col.begin(), new_age64_col.end());
        age_float_col.insert(age_float_col.end(),
                             new_age_float_col.begin(),
                             new_age_float_col.end());
        age_double_col.insert(age_double_col.end(),
                              new_age_double_col.begin(),
                              new_age_double_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 5, clause);
        // if (dtype == DataType::INT8) {
        //     dsl_string.replace(loc, 5, dsl_string_int8);
        // } else if (dtype == DataType::INT16) {
        //     dsl_string.replace(loc, 5, dsl_string_int16);
        // } else if (dtype == DataType::INT32) {
        //     dsl_string.replace(loc, 5, dsl_string_int32);
        // } else if (dtype == DataType::INT64) {
        //     dsl_string.replace(loc, 5, dsl_string_int64);
        // } else if (dtype == DataType::FLOAT) {
        //     dsl_string.replace(loc, 5, dsl_string_float);
        // } else if (dtype == DataType::DOUBLE) {
        //     dsl_string.replace(loc, 5, dsl_string_double);
        // } else {
        //     ASSERT_TRUE(false) << "No test case defined for this data type";
        // }
        // loc = dsl_string.find("@@@@");
        // dsl_string.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref)
                    << clause << "@" << i << "!!" << val << std::endl;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val << std::endl;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, bool)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 4
                  >
                  op: Equal
                  value: <
                    int64_val: 8
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) == 8;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 500
                  >
                  op: Equal
                  value: <
                    int64_val: 1500
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) == 1500;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4000
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) == 4000;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) == 1000;
             },
             DataType::INT64},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: Equal
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) == 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::DOUBLE},
            // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) != 2500;
             },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) != 2;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) != 1000;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: NotEqual
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) != 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterThan
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) > 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterThan
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) > 2500;
             },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) > 2;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) > 1000;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) > 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) > 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) >= 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) >= 2500;
             },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) >= 2;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) >= 1000;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) >= 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) >= 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: LessThan
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) < 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: LessThan
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) < 2500;
             },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) < 2;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) < 1000;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: LessThan
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) < 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: LessThan
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) < 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: LessEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) <= 2500;
             },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: LessEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) <= 2500;
             },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) <= 2;
             },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) <= 1000;
             },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: LessEqual
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) <= 0;
             },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) <= 2500;
             },
             DataType::INT64},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_nullable_fid = schema->AddDebugField("age8", DataType::INT8, true);
    auto i16_nullable_fid =
        schema->AddDebugField("age16", DataType::INT16, true);
    auto i32_nullable_fid =
        schema->AddDebugField("age32", DataType::INT32, true);
    auto i64_nullable_fid =
        schema->AddDebugField("age64_nullable", DataType::INT64, true);
    auto float_nullable_fid =
        schema->AddDebugField("age_float", DataType::FLOAT, true);
    auto double_nullable_fid =
        schema->AddDebugField("age_double", DataType::DOUBLE, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int8_t> age8_col;
    std::vector<int16_t> age16_col;
    std::vector<int32_t> age32_col;
    std::vector<int64_t> age64_col;
    std::vector<float> age_float_col;
    std::vector<double> age_double_col;
    FixedVector<bool> age8_valid_col;
    FixedVector<bool> age16_valid_col;
    FixedVector<bool> age32_valid_col;
    FixedVector<bool> age64_valid_col;
    FixedVector<bool> age_float_valid_col;
    FixedVector<bool> age_double_valid_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto new_age8_col = raw_data.get_col<int8_t>(i8_nullable_fid);
        auto new_age16_col = raw_data.get_col<int16_t>(i16_nullable_fid);
        auto new_age32_col = raw_data.get_col<int32_t>(i32_nullable_fid);
        auto new_age64_col = raw_data.get_col<int64_t>(i64_nullable_fid);
        auto new_age_float_col = raw_data.get_col<float>(float_nullable_fid);
        auto new_age_double_col = raw_data.get_col<double>(double_nullable_fid);
        age8_valid_col = raw_data.get_col_valid(i8_nullable_fid);
        age16_valid_col = raw_data.get_col_valid(i16_nullable_fid);
        age32_valid_col = raw_data.get_col_valid(i32_nullable_fid);
        age64_valid_col = raw_data.get_col_valid(i64_nullable_fid);
        age_float_valid_col = raw_data.get_col_valid(float_nullable_fid);
        age_double_valid_col = raw_data.get_col_valid(double_nullable_fid);

        age8_col.insert(
            age8_col.end(), new_age8_col.begin(), new_age8_col.end());
        age16_col.insert(
            age16_col.end(), new_age16_col.begin(), new_age16_col.end());
        age32_col.insert(
            age32_col.end(), new_age32_col.begin(), new_age32_col.end());
        age64_col.insert(
            age64_col.end(), new_age64_col.begin(), new_age64_col.end());
        age_float_col.insert(age_float_col.end(),
                             new_age_float_col.begin(),
                             new_age_float_col.end());
        age_double_col.insert(age_double_col.end(),
                              new_age_double_col.begin(),
                              new_age_double_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 5, clause);
        // if (dtype == DataType::INT8) {
        //     dsl_string.replace(loc, 5, dsl_string_int8);
        // } else if (dtype == DataType::INT16) {
        //     dsl_string.replace(loc, 5, dsl_string_int16);
        // } else if (dtype == DataType::INT32) {
        //     dsl_string.replace(loc, 5, dsl_string_int32);
        // } else if (dtype == DataType::INT64) {
        //     dsl_string.replace(loc, 5, dsl_string_int64);
        // } else if (dtype == DataType::FLOAT) {
        //     dsl_string.replace(loc, 5, dsl_string_float);
        // } else if (dtype == DataType::DOUBLE) {
        //     dsl_string.replace(loc, 5, dsl_string_double);
        // } else {
        //     ASSERT_TRUE(false) << "No test case defined for this data type";
        // }
        // loc = dsl_string.find("@@@@");
        // dsl_string.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val, age8_valid_col[i]);
                ASSERT_EQ(ans, ref)
                    << clause << "@" << i << "!!" << val << std::endl;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val << std::endl;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val, age16_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val, age32_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val, age64_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val, age_float_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val, age_double_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    std::vector<
        std::tuple<std::string, std::function<bool(const milvus::Json& json)>>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: Equal
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) == 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: Equal
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) == 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value:<int64_val:4>
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: Equal
                  value:<int64_val:4>
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length == 4;
             }},
            // Add test cases for BinaryArithOpEvalRangeExpr NQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length != 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) > 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) > 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length > 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) >= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) >= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length >= 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) < 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) < 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length < 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) <= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) <= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length <= 4;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 5, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto ref =
                ref_func(milvus::Json(simdjson::padded_string(json_col[i])));
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << json_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!" << json_col[i];
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONNullable) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    std::vector<
        std::tuple<std::string,
                   std::function<bool(const milvus::Json& json, bool valid)>>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: Equal
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) == 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: Equal
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) == 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value:<int64_val:4>
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) == 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: Equal
                  value:<int64_val:4>
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length == 4;
             }},
            // Add test cases for BinaryArithOpEvalRangeExpr NQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) != 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: NotEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length != 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) > 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) > 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) > 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: GreaterThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length > 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) >= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) >= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) >= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: GreaterEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length >= 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) < 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessThan
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) < 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) < 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: LessThan
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length < 4;
             }},

            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) <= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 1
                  >
                  op: LessEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) <= 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"int"
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 2
                  >
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) <= 4;
             }},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id:102
                    data_type:JSON
                    nested_path:"array"
                  >
                  arith_op: ArrayLength
                  op: LessEqual
                  value: <
                    int64_val: 4
                  >
             >)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length <= 4;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto nullable_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(nullable_fid);
        valid_data = raw_data.get_col_valid(nullable_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 5, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto ref =
                ref_func(milvus::Json(simdjson::padded_string(json_col[i])),
                         valid_data[i]);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << json_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!" << json_col[i];
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONFloat) {
    struct Testcase {
        double right_operand;
        double value;
        OpType op;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {10, 20, OpType::Equal, {"double"}},
        {20, 30, OpType::Equal, {"double"}},
        {30, 40, OpType::NotEqual, {"double"}},
        {40, 50, OpType::NotEqual, {"double"}},
        {10, 20, OpType::Equal, {"int"}},
        {20, 30, OpType::Equal, {"int"}},
        {30, 40, OpType::NotEqual, {"int"}},
        {40, 50, OpType::NotEqual, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](double value) {
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_float_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_float_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::Add,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<double>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref)
                << testcase.value << " " << val << " " << testcase.op;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << val << " " << testcase.op;
            }
        }
    }

    std::vector<Testcase> array_testcases{
        {0, 3, OpType::Equal, {"array"}},
        {0, 5, OpType::NotEqual, {"array"}},
    };

    for (auto testcase : array_testcases) {
        auto check = [&](int64_t value) {
            if (testcase.op == OpType::Equal) {
                return value == testcase.value;
            }
            return value != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_int64_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_int64_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::ArrayLength,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto json = milvus::Json(simdjson::padded_string(json_col[i]));
            int64_t array_length = 0;
            auto doc = json.doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (!array.error()) {
                array_length = array.count_elements();
            }
            auto ref = check(array_length);
            ASSERT_EQ(ans, ref) << testcase.value << " " << array_length;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << array_length;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONFloatNullable) {
    struct Testcase {
        double right_operand;
        double value;
        OpType op;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {10, 20, OpType::Equal, {"double"}},
        {20, 30, OpType::Equal, {"double"}},
        {30, 40, OpType::NotEqual, {"double"}},
        {40, 50, OpType::NotEqual, {"double"}},
        {10, 20, OpType::Equal, {"int"}},
        {20, 30, OpType::Equal, {"int"}},
        {30, 40, OpType::NotEqual, {"int"}},
        {40, 50, OpType::NotEqual, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](double value, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_float_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_float_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::Add,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<double>(pointer)
                           .value();
            auto ref = check(val, valid_data[i]);
            ASSERT_EQ(ans, ref)
                << testcase.value << " " << val << " " << testcase.op;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << val << " " << testcase.op;
            }
        }
    }

    std::vector<Testcase> array_testcases{
        {0, 3, OpType::Equal, {"array"}},
        {0, 5, OpType::NotEqual, {"array"}},
    };

    for (auto testcase : array_testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.op == OpType::Equal) {
                return value == testcase.value;
            }
            return value != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_int64_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_int64_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::ArrayLength,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto json = milvus::Json(simdjson::padded_string(json_col[i]));
            int64_t array_length = 0;
            auto doc = json.doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (!array.error()) {
                array_length = array.count_elements();
            }
            auto ref = check(array_length, valid_data[i]);
            ASSERT_EQ(ans, ref) << testcase.value << " " << array_length;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << array_length;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeWithScalarSortIndex) {
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: Equal
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) == 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: Equal
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) == 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) == 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) == 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: Equal
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) == 0; },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](float v) { return (v + 500) == 2500; },
             DataType::FLOAT},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](double v) { return (v + 500) == 2500; },
             DataType::DOUBLE},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2000
            >)",
             [](float v) { return (v + 500) != 2000; },
             DataType::FLOAT},
            {R"(arith_op: Sub
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2500
            >)",
             [](double v) { return (v - 500) != 2000; },
             DataType::DOUBLE},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2
            >)",
             [](int8_t v) { return (v * 2) != 2; },
             DataType::INT8},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int16_t v) { return (v / 2) != 2000; },
             DataType::INT16},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: NotEqual
            value: <
                int64_val: 1
            >)",
             [](int32_t v) { return (v % 100) != 1; },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                int64_val: 500
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int64_t v) { return (v + 500) != 2000; },
             DataType::INT64},

            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: GreaterThan
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) > 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: GreaterThan
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) > 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: GreaterThan
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) > 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: GreaterThan
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) > 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: GreaterThan
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) > 0; },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: GreaterEqual
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) >= 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: GreaterEqual
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) >= 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: GreaterEqual
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) >= 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: GreaterEqual
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) >= 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: GreaterEqual
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) >= 0; },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: LessThan
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) < 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: LessThan
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) < 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: LessThan
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) < 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: LessThan
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) < 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: LessThan
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) < 0; },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: LessEqual
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) <= 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: LessEqual
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) <= 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: LessEqual
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) <= 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: LessEqual
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) <= 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: LessEqual
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) <= 0; },
             DataType::INT32},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                binary_arith_op_eval_range_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    segcore::LoadIndexInfo load_index_info;

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_fid);
    age8_col[0] = 4;
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data(), nullptr);
    load_index_info.field_id = i8_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index_params = GenIndexParams(age8_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age8_index));
    seg->LoadIndex(load_index_info);

    // load index for 16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_fid);
    age16_col[0] = 2000;
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data(), nullptr);
    load_index_info.field_id = i16_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index_params = GenIndexParams(age16_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age16_index));
    seg->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 2000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data(), nullptr);
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data(), nullptr);
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_fid);
    age_float_col[0] = 2000;
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data(), nullptr);
    load_index_info.field_id = float_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_float_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_float_index));
    seg->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_fid);
    age_double_col[0] = 2000;
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data(), nullptr);
    load_index_info.field_id = double_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_double_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_double_index));
    seg->LoadIndex(load_index_info);

    auto seg_promote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        if (dtype == DataType::INT8) {
            expr = boost::format(expr_plan) % vec_fid.get() % i8_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT8));
        } else if (dtype == DataType::INT16) {
            expr = boost::format(expr_plan) % vec_fid.get() % i16_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT16));
        } else if (dtype == DataType::INT32) {
            expr = boost::format(expr_plan) % vec_fid.get() % i32_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT32));
        } else if (dtype == DataType::INT64) {
            expr = boost::format(expr_plan) % vec_fid.get() % i64_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT64));
        } else if (dtype == DataType::FLOAT) {
            expr = boost::format(expr_plan) % vec_fid.get() % float_fid.get() %
                   proto::schema::DataType_Name(int(DataType::FLOAT));
        } else if (dtype == DataType::DOUBLE) {
            expr = boost::format(expr_plan) % vec_fid.get() % double_fid.get() %
                   proto::schema::DataType_Name(int(DataType::DOUBLE));
        } else {
            ASSERT_TRUE(false) << "No test case defined for this data type";
        }

        auto binary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeWithScalarSortIndexNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, bool)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: Equal
            value: <
                int64_val: 8
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) == 8;
             },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: Equal
            value: <
                int64_val: 1500
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) == 1500;
             },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 4000
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) == 4000;
             },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 1000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) == 1000;
             },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: Equal
            value: <
                int64_val: 0
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) == 0;
             },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::FLOAT},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::DOUBLE},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2000
            >)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2000;
             },
             DataType::FLOAT},
            {R"(arith_op: Sub
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2500
            >)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) != 2000;
             },
             DataType::DOUBLE},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) != 2;
             },
             DataType::INT8},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) != 2000;
             },
             DataType::INT16},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: NotEqual
            value: <
                int64_val: 1
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) != 1;
             },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                int64_val: 500
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2000;
             },
             DataType::INT64},

            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: GreaterThan
            value: <
                int64_val: 8
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) > 8;
             },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: GreaterThan
            value: <
                int64_val: 1500
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) > 1500;
             },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: GreaterThan
            value: <
                int64_val: 4000
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) > 4000;
             },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: GreaterThan
            value: <
                int64_val: 1000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) > 1000;
             },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: GreaterThan
            value: <
                int64_val: 0
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) > 0;
             },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: GreaterEqual
            value: <
                int64_val: 8
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) >= 8;
             },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: GreaterEqual
            value: <
                int64_val: 1500
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) >= 1500;
             },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: GreaterEqual
            value: <
                int64_val: 4000
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) >= 4000;
             },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: GreaterEqual
            value: <
                int64_val: 1000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) >= 1000;
             },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: GreaterEqual
            value: <
                int64_val: 0
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) >= 0;
             },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: LessThan
            value: <
                int64_val: 8
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) < 8;
             },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: LessThan
            value: <
                int64_val: 1500
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) < 1500;
             },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: LessThan
            value: <
                int64_val: 4000
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) < 4000;
             },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: LessThan
            value: <
                int64_val: 1000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) < 1000;
             },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: LessThan
            value: <
                int64_val: 0
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) < 0;
             },
             DataType::INT32},

            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: LessEqual
            value: <
                int64_val: 8
            >)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) <= 8;
             },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: LessEqual
            value: <
                int64_val: 1500
            >)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) <= 1500;
             },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: LessEqual
            value: <
                int64_val: 4000
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) <= 4000;
             },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: LessEqual
            value: <
                int64_val: 1000
            >)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) <= 1000;
             },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: LessEqual
            value: <
                int64_val: 0
            >)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) <= 0;
             },
             DataType::INT32},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                binary_arith_op_eval_range_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_nullable_fid = schema->AddDebugField("age8", DataType::INT8, true);
    auto i16_nullable_fid =
        schema->AddDebugField("age16", DataType::INT16, true);
    auto i32_nullable_fid =
        schema->AddDebugField("age32", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto i64_nullable_fid =
        schema->AddDebugField("age641", DataType::INT64, true);
    auto float_nullable_fid =
        schema->AddDebugField("age_float", DataType::FLOAT, true);
    auto double_nullable_fid =
        schema->AddDebugField("age_double", DataType::DOUBLE, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    segcore::LoadIndexInfo load_index_info;

    auto i8_valid_data = raw_data.get_col_valid(i8_nullable_fid);
    auto i16_valid_data = raw_data.get_col_valid(i16_nullable_fid);
    auto i32_valid_data = raw_data.get_col_valid(i32_nullable_fid);
    auto i64_valid_data = raw_data.get_col_valid(i64_nullable_fid);
    auto float_valid_data = raw_data.get_col_valid(float_nullable_fid);
    auto double_valid_data = raw_data.get_col_valid(double_nullable_fid);

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_nullable_fid);
    age8_col[0] = 4;
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data(), i8_valid_data.data());
    load_index_info.field_id = i8_nullable_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index_params = GenIndexParams(age8_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age8_index));
    seg->LoadIndex(load_index_info);

    // load index for 16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_nullable_fid);
    age16_col[0] = 2000;
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data(), i16_valid_data.data());
    load_index_info.field_id = i16_nullable_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index_params = GenIndexParams(age16_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age16_index));
    seg->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_nullable_fid);
    age32_col[0] = 2000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data(), i32_valid_data.data());
    load_index_info.field_id = i32_nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_nullable_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data(), i64_valid_data.data());
    load_index_info.field_id = i64_nullable_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_nullable_fid);
    age_float_col[0] = 2000;
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data(), float_valid_data.data());
    load_index_info.field_id = float_nullable_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_float_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_float_index));
    seg->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_nullable_fid);
    age_double_col[0] = 2000;
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data(), double_valid_data.data());
    load_index_info.field_id = double_nullable_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_double_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_double_index));
    seg->LoadIndex(load_index_info);

    auto seg_promote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        if (dtype == DataType::INT8) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   i8_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT8));
        } else if (dtype == DataType::INT16) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   i16_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT16));
        } else if (dtype == DataType::INT32) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   i32_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT32));
        } else if (dtype == DataType::INT64) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   i64_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT64));
        } else if (dtype == DataType::FLOAT) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   float_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::FLOAT));
        } else if (dtype == DataType::DOUBLE) {
            expr = boost::format(expr_plan) % vec_fid.get() %
                   double_nullable_fid.get() %
                   proto::schema::DataType_Name(int(DataType::DOUBLE));
        } else {
            ASSERT_TRUE(false) << "No test case defined for this data type";
        }

        auto binary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N, 10));

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val, i8_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val, i16_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val, i32_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val, i64_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val, float_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val, double_valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryRangeWithJSON) {
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType>>
        testcases = {
            {R"(op: Equal
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: LessEqual
                        value: <
                            int64_val: 1500
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) < 1500;
             },
             DataType::INT64},
            {R"(op: LessEqual
                        value: <
                            float_val: 4000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) <= 4000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterThan
                        value: <
                            float_val: 1000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) > 1000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterEqual
                        value: <
                            int64_val: 0
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) >= 0;
             },
             DataType::INT64},
            {R"(op: NotEqual
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return !std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: Equal
            value: <
                string_val: "test"
            >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<std::string_view>(v) == "test";
             },
             DataType::STRING},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                unary_range_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryRangeWithJSONNullable) {
    std::vector<std::tuple<
        std::string,
        std::function<bool(
            std::variant<int64_t, bool, double, std::string_view>, bool)>,
        DataType>>
        testcases = {
            {R"(op: Equal
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: LessEqual
                        value: <
                            int64_val: 1500
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<int64_t>(v) < 1500;
             },
             DataType::INT64},
            {R"(op: LessEqual
                        value: <
                            float_val: 4000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<double>(v) <= 4000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterThan
                        value: <
                            float_val: 1000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<double>(v) > 1000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterEqual
                        value: <
                            int64_val: 0
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<int64_t>(v) >= 0;
             },
             DataType::INT64},
            {R"(op: NotEqual
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return !std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: Equal
            value: <
                string_val: "test"
            >)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<std::string_view>(v) == "test";
             },
             DataType::STRING},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                unary_range_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestNullExprWithJSON) {
    std::vector<std::tuple<std::string, std::function<bool(bool)>>> testcases =
        {
            {R"(null_expr: <
                column_info: <
                    field_id: 102
                    data_type:JSON
                    nullable: true
                  >
                op:IsNull
        >)",
             [](bool v) { return !v; }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    int num_iters = 1;
    FixedVector<bool> valid_data;
    std::vector<std::string> json_col;

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        auto new_valid_col = raw_data.get_col_valid(json_fid);
        valid_data.insert(
            valid_data.end(), new_valid_col.begin(), new_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto valid = valid_data[i];
            auto ref = ref_func(valid);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST_P(ExprTest, TestTermWithJSON) {
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType>>
        testcases = {
            {R"(values: <bool_val: true>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<bool> term_set;
                 term_set = {true, false};
                 return term_set.find(std::get<bool>(v)) != term_set.end();
             },
             DataType::BOOL},
            {R"(values: <int64_val: 1500>, values: <int64_val: 2048>, values: <int64_val: 3216>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<int64_t> term_set;
                 term_set = {1500, 2048, 3216};
                 return term_set.find(std::get<int64_t>(v)) != term_set.end();
             },
             DataType::INT64},
            {R"(values: <float_val: 1500.0>, values: <float_val: 4000>, values: <float_val: 235.14>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<double> term_set;
                 term_set = {1500.0, 4000, 235.14};
                 return term_set.find(std::get<double>(v)) != term_set.end();
             },
             DataType::DOUBLE},
            {R"(values: <string_val: "aaa">, values: <string_val: "abc">, values: <string_val: "235.14">)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<std::string_view> term_set;
                 term_set = {"aaa", "abc", "235.14"};
                 return term_set.find(std::get<std::string_view>(v)) !=
                        term_set.end();
             },
             DataType::STRING},
            {R"()",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return false;
             },
             DataType::INT64},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                term_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestTermWithJSONNullable) {
    std::vector<std::tuple<
        std::string,
        std::function<bool(
            std::variant<int64_t, bool, double, std::string_view>, bool)>,
        DataType>>
        testcases = {
            {R"(values: <bool_val: true>)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<bool> term_set;
                 term_set = {true, false};
                 return term_set.find(std::get<bool>(v)) != term_set.end();
             },
             DataType::BOOL},
            {R"(values: <int64_val: 1500>, values: <int64_val: 2048>, values: <int64_val: 3216>)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<int64_t> term_set;
                 term_set = {1500, 2048, 3216};
                 return term_set.find(std::get<int64_t>(v)) != term_set.end();
             },
             DataType::INT64},
            {R"(values: <float_val: 1500.0>, values: <float_val: 4000>, values: <float_val: 235.14>)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<double> term_set;
                 term_set = {1500.0, 4000, 235.14};
                 return term_set.find(std::get<double>(v)) != term_set.end();
             },
             DataType::DOUBLE},
            {R"(values: <string_val: "aaa">, values: <string_val: "abc">, values: <string_val: "235.14">)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<std::string_view> term_set;
                 term_set = {"aaa", "abc", "235.14"};
                 return term_set.find(std::get<std::string_view>(v)) !=
                        term_set.end();
             },
             DataType::STRING},
            {R"()",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return false;
             },
             DataType::INT64},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                term_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestExistsWithJSON) {
    std::vector<std::tuple<std::string, std::function<bool(bool)>, DataType>>
        testcases = {
            {R"()", [](bool v) { return v; }, DataType::BOOL},
            {R"()", [](bool v) { return v; }, DataType::INT64},
            {R"()", [](bool v) { return v; }, DataType::STRING},
            {R"()", [](bool v) { return v; }, DataType::VARCHAR},
            {R"()", [](bool v) { return v; }, DataType::DOUBLE},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                exists_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            case DataType::VARCHAR: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "varchar";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/bool");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/int");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/double");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/string");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::VARCHAR) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/varchar");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestExistsWithJSONNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(bool, bool)>, DataType>>
        testcases = {
            {R"()",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             DataType::BOOL},
            {R"()",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             DataType::INT64},
            {R"()",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             DataType::STRING},
            {R"()",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             DataType::VARCHAR},
            {R"()",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             DataType::DOUBLE},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                exists_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            case DataType::VARCHAR: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "varchar";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_with_metric_type(expr.str());
        auto plan = CreateSearchPlanByExpr(
            schema, unary_plan.data(), unary_plan.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/bool");
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/int");
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/double");
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/string");
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::VARCHAR) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/varchar");
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

template <typename T>
struct Testcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
    bool res;
};

TEST_P(ExprTest, TestTermInFieldJson) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST_P(ExprTest, TestTermInFieldJsonNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        // std::cout << "cost"
        //           << std::chrono::duration_cast<std::chrono::microseconds>(
        //                  std::chrono::steady_clock::now() - start)
        //                  .count()
        //           << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values,
                         bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }
}

TEST_P(ExprTest, PraseJsonContainsExpr) {
    std::vector<std::string> raw_plans{
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<int64_val:1 > elements:<int64_val:2 > elements:<int64_val:3 >
                    op:ContainsAny
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<int64_val:1 > elements:<int64_val:2 > elements:<int64_val:3 >
                    op:ContainsAll
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<bool_val:true > elements:<bool_val:false > elements:<bool_val:true >
                    op:ContainsAll
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<float_val:1.1 > elements:<float_val:2.2 > elements:<float_val:3.3 >
                    op:ContainsAll
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<string_val:"1" > elements:<string_val:"2" > elements:<string_val:"3" >
                    op:ContainsAll
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:JSON
                        nested_path:"A"
                    >
                    elements:<string_val:"1" >
                    elements:<int64_val:2 >
                    elements:<float_val:3.3 >
                    elements:<bool_val:true>
                    op:ContainsAll
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
    };

    for (auto& raw_plan : raw_plans) {
        auto plan_str = translate_text_plan_with_metric_type(raw_plan);
        auto schema = std::make_shared<Schema>();
        schema->AddDebugField("fakevec", data_type, 16, metric_type);
        schema->AddDebugField("json", DataType::JSON);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    }
}

TEST_P(ExprTest, TestJsonContainsAny) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsAnyNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values,
                         bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsAll) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<Testcase<bool>> bool_testcases{{{true, true}, {"bool"}},
                                               {{false, false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);

        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsAllNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<Testcase<bool>> bool_testcases{{{true, true}, {"bool"}},
                                               {{false, false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values,
                         bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsArray) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue generic_a;
    auto* a = generic_a.mutable_array_val();
    a->set_same_type(false);
    for (int i = 0; i < 4; ++i) {
        if (i % 4 == 0) {
            proto::plan::GenericValue int_val;
            int_val.set_int64_val(int64_t(i));
            a->add_array()->CopyFrom(int_val);
        } else if ((i - 1) % 4 == 0) {
            proto::plan::GenericValue bool_val;
            bool_val.set_bool_val(bool(i));
            a->add_array()->CopyFrom(bool_val);
        } else if ((i - 2) % 4 == 0) {
            proto::plan::GenericValue float_val;
            float_val.set_float_val(double(i));
            a->add_array()->CopyFrom(float_val);
        } else if ((i - 3) % 4 == 0) {
            proto::plan::GenericValue string_val;
            string_val.set_string_val(std::to_string(i));
            a->add_array()->CopyFrom(string_val);
        }
    }
    proto::plan::GenericValue generic_b;
    auto* b = generic_b.mutable_array_val();
    b->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    b->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    b->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    b->add_array()->CopyFrom(int_val3);

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{generic_a}, {"string"}}, {{generic_b}, {"array"}}};

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i) {
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i) {
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr1;
    auto* sub_arr1 = g_sub_arr1.mutable_array_val();
    sub_arr1->set_same_type(true);
    proto::plan::GenericValue int_val11;
    int_val11.set_int64_val(int64_t(1));
    sub_arr1->add_array()->CopyFrom(int_val11);

    proto::plan::GenericValue int_val12;
    int_val12.set_int64_val(int64_t(2));
    sub_arr1->add_array()->CopyFrom(int_val12);

    proto::plan::GenericValue g_sub_arr2;
    auto* sub_arr2 = g_sub_arr2.mutable_array_val();
    sub_arr2->set_same_type(true);
    proto::plan::GenericValue int_val21;
    int_val21.set_int64_val(int64_t(3));
    sub_arr2->add_array()->CopyFrom(int_val21);

    proto::plan::GenericValue int_val22;
    int_val22.set_int64_val(int64_t(4));
    sub_arr2->add_array()->CopyFrom(int_val22);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases2{
        {{g_sub_arr1, g_sub_arr2}, {"array2"}}};

    for (auto& testcase : diff_testcases2) {
        auto check = [&]() { return true; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }

    for (auto& testcase : diff_testcases2) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr3;
    auto* sub_arr3 = g_sub_arr3.mutable_array_val();
    sub_arr3->set_same_type(true);
    proto::plan::GenericValue int_val31;
    int_val31.set_int64_val(int64_t(5));
    sub_arr3->add_array()->CopyFrom(int_val31);

    proto::plan::GenericValue int_val32;
    int_val32.set_int64_val(int64_t(6));
    sub_arr3->add_array()->CopyFrom(int_val32);

    proto::plan::GenericValue g_sub_arr4;
    auto* sub_arr4 = g_sub_arr4.mutable_array_val();
    sub_arr4->set_same_type(true);
    proto::plan::GenericValue int_val41;
    int_val41.set_int64_val(int64_t(7));
    sub_arr4->add_array()->CopyFrom(int_val41);

    proto::plan::GenericValue int_val42;
    int_val42.set_int64_val(int64_t(8));
    sub_arr4->add_array()->CopyFrom(int_val42);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases3{
        {{g_sub_arr3, g_sub_arr4}, {"array2"}}};

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsArrayNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue generic_a;
    auto* a = generic_a.mutable_array_val();
    a->set_same_type(false);
    for (int i = 0; i < 4; ++i) {
        if (i % 4 == 0) {
            proto::plan::GenericValue int_val;
            int_val.set_int64_val(int64_t(i));
            a->add_array()->CopyFrom(int_val);
        } else if ((i - 1) % 4 == 0) {
            proto::plan::GenericValue bool_val;
            bool_val.set_bool_val(bool(i));
            a->add_array()->CopyFrom(bool_val);
        } else if ((i - 2) % 4 == 0) {
            proto::plan::GenericValue float_val;
            float_val.set_float_val(double(i));
            a->add_array()->CopyFrom(float_val);
        } else if ((i - 3) % 4 == 0) {
            proto::plan::GenericValue string_val;
            string_val.set_string_val(std::to_string(i));
            a->add_array()->CopyFrom(string_val);
        }
    }
    proto::plan::GenericValue generic_b;
    auto* b = generic_b.mutable_array_val();
    b->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    b->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    b->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    b->add_array()->CopyFrom(int_val3);

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{generic_a}, {"string"}}, {{generic_b}, {"array"}}};

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr1;
    auto* sub_arr1 = g_sub_arr1.mutable_array_val();
    sub_arr1->set_same_type(true);
    proto::plan::GenericValue int_val11;
    int_val11.set_int64_val(int64_t(1));
    sub_arr1->add_array()->CopyFrom(int_val11);

    proto::plan::GenericValue int_val12;
    int_val12.set_int64_val(int64_t(2));
    sub_arr1->add_array()->CopyFrom(int_val12);

    proto::plan::GenericValue g_sub_arr2;
    auto* sub_arr2 = g_sub_arr2.mutable_array_val();
    sub_arr2->set_same_type(true);
    proto::plan::GenericValue int_val21;
    int_val21.set_int64_val(int64_t(3));
    sub_arr2->add_array()->CopyFrom(int_val21);

    proto::plan::GenericValue int_val22;
    int_val22.set_int64_val(int64_t(4));
    sub_arr2->add_array()->CopyFrom(int_val22);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases2{
        {{g_sub_arr1, g_sub_arr2}, {"array2"}}};

    for (auto& testcase : diff_testcases2) {
        auto check = [&](bool valid) {
            if (!valid) {
                return false;
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check(valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases2) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr3;
    auto* sub_arr3 = g_sub_arr3.mutable_array_val();
    sub_arr3->set_same_type(true);
    proto::plan::GenericValue int_val31;
    int_val31.set_int64_val(int64_t(5));
    sub_arr3->add_array()->CopyFrom(int_val31);

    proto::plan::GenericValue int_val32;
    int_val32.set_int64_val(int64_t(6));
    sub_arr3->add_array()->CopyFrom(int_val32);

    proto::plan::GenericValue g_sub_arr4;
    auto* sub_arr4 = g_sub_arr4.mutable_array_val();
    sub_arr4->set_same_type(true);
    proto::plan::GenericValue int_val41;
    int_val41.set_int64_val(int64_t(7));
    sub_arr4->add_array()->CopyFrom(int_val41);

    proto::plan::GenericValue int_val42;
    int_val42.set_int64_val(int64_t(8));
    sub_arr4->add_array()->CopyFrom(int_val42);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases3{
        {{g_sub_arr3, g_sub_arr4}, {"array2"}}};

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }
}

milvus::proto::plan::GenericValue
generatedArrayWithFourDiffType(int64_t int_val,
                               double float_val,
                               bool bool_val,
                               std::string string_val) {
    proto::plan::GenericValue value;
    proto::plan::Array diff_type_array;
    diff_type_array.set_same_type(false);
    proto::plan::GenericValue int_value;
    int_value.set_int64_val(int_val);
    diff_type_array.add_array()->CopyFrom(int_value);

    proto::plan::GenericValue float_value;
    float_value.set_float_val(float_val);
    diff_type_array.add_array()->CopyFrom(float_value);

    proto::plan::GenericValue bool_value;
    bool_value.set_bool_val(bool_val);
    diff_type_array.add_array()->CopyFrom(bool_value);

    proto::plan::GenericValue string_value;
    string_value.set_string_val(string_val);
    diff_type_array.add_array()->CopyFrom(string_value);

    value.mutable_array_val()->CopyFrom(diff_type_array);
    return value;
}

TEST_P(ExprTest, TestJsonContainsDiffTypeArray) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    auto diff_type_array1 =
        generatedArrayWithFourDiffType(1, 2.2, false, "abc");
    auto diff_type_array2 =
        generatedArrayWithFourDiffType(1, 2.2, false, "def");
    auto diff_type_array3 = generatedArrayWithFourDiffType(1, 2.2, true, "abc");
    auto diff_type_array4 =
        generatedArrayWithFourDiffType(1, 3.3, false, "abc");
    auto diff_type_array5 =
        generatedArrayWithFourDiffType(2, 2.2, false, "abc");

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{diff_type_array1, int_value}, {"array3"}, true},
        {{diff_type_array2, int_value}, {"array3"}, false},
        {{diff_type_array3, int_value}, {"array3"}, false},
        {{diff_type_array4, int_value}, {"array3"}, false},
        {{diff_type_array5, int_value}, {"array3"}, false},
    };

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return testcase.res; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return false; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsDiffTypeArrayNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    auto diff_type_array1 =
        generatedArrayWithFourDiffType(1, 2.2, false, "abc");
    auto diff_type_array2 =
        generatedArrayWithFourDiffType(1, 2.2, false, "def");
    auto diff_type_array3 = generatedArrayWithFourDiffType(1, 2.2, true, "abc");
    auto diff_type_array4 =
        generatedArrayWithFourDiffType(1, 3.3, false, "abc");
    auto diff_type_array5 =
        generatedArrayWithFourDiffType(2, 2.2, false, "abc");

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{diff_type_array1, int_value}, {"array3"}, true},
        {{diff_type_array2, int_value}, {"array3"}, false},
        {{diff_type_array3, int_value}, {"array3"}, false},
        {{diff_type_array4, int_value}, {"array3"}, false},
        {{diff_type_array5, int_value}, {"array3"}, false},
    };

    for (auto& testcase : diff_testcases) {
        auto check = [&](bool valid) {
            if (!valid) {
                return false;
            }
            return testcase.res;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check(valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return false; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsDiffType) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue int_val;
    int_val.set_int64_val(int64_t(3));
    proto::plan::GenericValue bool_val;
    bool_val.set_bool_val(bool(false));
    proto::plan::GenericValue float_val;
    float_val.set_float_val(double(100.34));
    proto::plan::GenericValue string_val;
    string_val.set_string_val("10dsf");

    proto::plan::GenericValue string_val2;
    string_val2.set_string_val("abc");
    proto::plan::GenericValue bool_val2;
    bool_val2.set_bool_val(bool(true));
    proto::plan::GenericValue float_val2;
    float_val2.set_float_val(double(2.2));
    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(1));

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{int_val, bool_val, float_val, string_val},
         {"diff_type_array"},
         false},
        {{string_val2, bool_val2, float_val2, int_val2},
         {"diff_type_array"},
         true},
    };

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, testcase.res);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], testcase.res);
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            ASSERT_EQ(ans, testcase.res);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], testcase.res);
            }
        }
    }
}
TEST_P(ExprTest, TestJsonContainsDiffTypeNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue int_val;
    int_val.set_int64_val(int64_t(3));
    proto::plan::GenericValue bool_val;
    bool_val.set_bool_val(bool(false));
    proto::plan::GenericValue float_val;
    float_val.set_float_val(double(100.34));
    proto::plan::GenericValue string_val;
    string_val.set_string_val("10dsf");

    proto::plan::GenericValue string_val2;
    string_val2.set_string_val("abc");
    proto::plan::GenericValue bool_val2;
    bool_val2.set_bool_val(bool(true));
    proto::plan::GenericValue float_val2;
    float_val2.set_float_val(double(2.2));
    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(1));

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{int_val, bool_val, float_val, string_val},
         {"diff_type_array"},
         false},
        {{string_val2, bool_val2, float_val2, int_val2},
         {"diff_type_array"},
         true},
    };

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], false);
                }
            } else {
                ASSERT_EQ(ans, testcase.res);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], testcase.res);
                }
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto start = std::chrono::steady_clock::now();
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], false);
                }
            } else {
                ASSERT_EQ(ans, testcase.res);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], testcase.res);
                }
            }
        }
    }
}

template <typename T>
class JsonIndexTestFixture : public testing::Test {
 public:
    using DataType = T;

    JsonIndexTestFixture() {
        if constexpr (std::is_same_v<T, bool>) {
            schema_data_type = proto::schema::Bool;
            json_path = "/bool";
            lower_bound.set_bool_val(std::numeric_limits<bool>::min());
            upper_bound.set_bool_val(std::numeric_limits<bool>::max());
            cast_type = JsonCastType::FromString("BOOL");
            wrong_type_val.set_int64_val(123);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            schema_data_type = proto::schema::Int64;
            json_path = "/int";
            lower_bound.set_int64_val(std::numeric_limits<int64_t>::min());
            upper_bound.set_int64_val(std::numeric_limits<int64_t>::max());
            cast_type = JsonCastType::FromString("DOUBLE");
            wrong_type_val.set_string_val("123");
        } else if constexpr (std::is_same_v<T, double>) {
            schema_data_type = proto::schema::Double;
            json_path = "/double";
            lower_bound.set_float_val(std::numeric_limits<double>::min());
            upper_bound.set_float_val(std::numeric_limits<double>::max());
            cast_type = JsonCastType::FromString("DOUBLE");
            wrong_type_val.set_string_val("123");
        } else if constexpr (std::is_same_v<T, std::string>) {
            schema_data_type = proto::schema::String;
            json_path = "/string";
            lower_bound.set_string_val("");
            std::string s(1024, '9');
            upper_bound.set_string_val(s);
            cast_type = JsonCastType::FromString("VARCHAR");
            wrong_type_val.set_int64_val(123);
        }
    }
    proto::schema::DataType schema_data_type;
    std::string json_path;
    proto::plan::GenericValue lower_bound;
    proto::plan::GenericValue upper_bound;
    JsonCastType cast_type = JsonCastType::UNKNOWN;

    proto::plan::GenericValue wrong_type_val;
};

using JsonIndexTypes = ::testing::Types<bool, int64_t, double, std::string>;
TYPED_TEST_SUITE(JsonIndexTestFixture, JsonIndexTypes);

TYPED_TEST(JsonIndexTestFixture, TestJsonIndexUnaryExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = this->cast_type,
            .json_path = this->json_path,
        },
        file_manager_ctx);

    using json_index_type =
        index::JsonInvertedIndex<typename TestFixture::DataType>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_col = raw_data.get_col<std::string>(json_fid);
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_col) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    // load_index_info.index = std::move(json_index);
    load_index_info.index_params = {
        {JSON_PATH, this->json_path},
        {JSON_CAST_TYPE, this->cast_type.ToString()}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test_cache_index", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::LessEqual,
        this->upper_bound,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N);

    // test for wrong filter type
    unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::LessEqual,
        this->wrong_type_val,
        std::vector<proto::plan::GenericValue>());
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::GreaterEqual,
        this->lower_bound,
        std::vector<proto::plan::GenericValue>());
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        std::vector<proto::plan::GenericValue>(),
        false);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, term_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    using DT = std::conditional_t<
        std::is_same_v<typename TestFixture::DataType, std::string>,
        std::string_view,
        typename TestFixture::DataType>;
    std::vector<proto::plan::GenericValue> vals;
    int expect_count = 10;
    if constexpr (std::is_same_v<DT, bool>) {
        proto::plan::GenericValue val;
        val.set_bool_val(true);
        vals.push_back(val);
        val.set_bool_val(false);
        vals.push_back(val);
        expect_count = N;
    } else {
        for (int i = 0; i < expect_count; ++i) {
            proto::plan::GenericValue val;

            auto v = jsons[i].at<DT>(this->json_path).value();
            if constexpr (std::is_same_v<DT, int64_t>) {
                val.set_int64_val(v);
            } else if constexpr (std::is_same_v<DT, double>) {
                val.set_float_val(v);
            } else if constexpr (std::is_same_v<DT, std::string_view>) {
                val.set_string_val(std::string(v));
            } else if constexpr (std::is_same_v<DT, bool>) {
                val.set_bool_val(i % 2 == 0);
            }
            vals.push_back(val);
        }
    }
    term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        vals,
        false);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, term_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

    EXPECT_EQ(final.count(), expect_count);
    // not expr
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, term_expr);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, not_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N - expect_count);
}

TEST(JsonIndexTest, TestJsonNotEqualExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("DOUBLE"),
            .json_path = "/a",
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_strs = std::vector<std::string>{
        R"({"a": 1.0})", R"({"a": "abc"})", R"({"a": 3.0})", R"({"a": null})"};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    auto json_field2 =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);
    json_field2->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field, json_field2});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field, json_field2}, cm);
    seg->LoadFieldData(load_info);

    proto::plan::GenericValue val;
    val.set_int64_val(1);
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        val,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto final =
        ExecuteQueryExpr(plan, seg.get(), 2 * json_strs.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 2 * json_strs.size() - 2);
}

class JsonIndexExistsTest : public ::testing::TestWithParam<std::string> {};

INSTANTIATE_TEST_SUITE_P(JsonIndexExistsTestParams,
                         JsonIndexExistsTest,
                         ::testing::Values("/a", ""));

TEST_P(JsonIndexExistsTest, TestExistsExpr) {
    std::vector<std::string> json_strs = {
        R"({"a": 1.0})",
        R"({"a": "abc"})",
        R"({"a": 3.0})",
        R"({"a": true})",
        R"({"a": {"b": 1}})",
        R"({"a": []})",
        R"({"a": ["a", "b"]})",
        R"({"a": null})",  // exists null
        R"(1)",
        R"("abc")",
        R"(1.0)",
        R"(true)",
        R"([1, 2, 3])",
        R"({"a": 1, "b": 2})",
        R"({})",
        R"(null)",
    };

    // bool: exists or not
    std::vector<std::tuple<std::vector<std::string>, bool, uint32_t>>
        test_cases = {
            {{"a"}, true, 0b1111111000000100},
            {{"a", "b"}, true, 0b0000100000000000},
            {{"a"}, false, 0b0000000111111011},
            {{"a", "b"}, false, 0b1111011111111111},
        };

    auto json_index_path = GetParam();

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("DOUBLE"),
            .json_path = json_index_path,
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    for (auto& json_str : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json_str)));
    }
    json_field->add_json_data(jsons);
    auto json_valid_data = json_field->ValidData();
    json_valid_data[0] = 0xFF;
    json_valid_data[1] = 0xFE;

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, json_index_path},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    for (auto& [nested_path, exists, expect] : test_cases) {
        BitsetType expect_res;
        expect_res.resize(json_strs.size());
        for (int i = json_strs.size() - 1; expect > 0; i--) {
            expect_res.set(i, (expect & 1) != 0);
            expect >>= 1;
        }

        std::shared_ptr<expr::ITypeFilterExpr> exists_expr;
        if (exists) {
            exists_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path, true));
        } else {
            auto child_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path, true));
            exists_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
        }
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);
        auto result =
            ExecuteQueryExpr(plan, seg.get(), json_strs.size(), MAX_TIMESTAMP);

        EXPECT_TRUE(result == expect_res);
    }
}

class JsonIndexBinaryExprTest : public testing::TestWithParam<JsonCastType> {};

INSTANTIATE_TEST_SUITE_P(JsonIndexBinaryExprTestParams,
                         JsonIndexBinaryExprTest,
                         testing::Values(JsonCastType::FromString("DOUBLE"),
                                         JsonCastType::FromString("VARCHAR")));

TEST_P(JsonIndexBinaryExprTest, TestBinaryRangeExpr) {
    auto json_strs = std::vector<std::string>{
        R"({"a": 1})",
        R"({"a": 2})",
        R"({"a": 3})",
        R"({"a": 4})",

        R"({"a": 1.0})",
        R"({"a": 2.0})",
        R"({"a": 3.0})",
        R"({"a": 4.0})",

        R"({"a": "1"})",
        R"({"a": "2"})",
        R"({"a": "3"})",
        R"({"a": "4"})",

        R"({"a": null})",
        R"({"a": true})",
        R"({"a": false})",
    };

    auto test_cases = std::vector<std::tuple<std::any,
                                             std::any,
                                             /*lower inclusive*/ bool,
                                             /*upper inclusive*/ bool,
                                             uint32_t>>{
        // Exact match for integer 1 (matches both int 1 and float 1.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(1),
         true,
         true,
         0b1000'1000'0000'000},

        // Range [1, 3] inclusive (matches int 1,2,3 and float 1.0,2.0,3.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         true,
         true,
         0b1110'1110'0000'000},

        // Range (1, 3) exclusive (matches only int 2 and float 2.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         false,
         false,
         0b0100'0100'0000'000},

        // Range [1, 3) left inclusive, right exclusive (matches int 1,2 and float 1.0,2.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         true,
         false,
         0b1100'1100'0000'000},

        // Range (1, 3] left exclusive, right inclusive (matches int 2,3 and float 2.0,3.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         false,
         true,
         0b0110'0110'0000'000},

        // Float range test [1.0, 3.0] (matches int 1,2,3 and float 1.0,2.0,3.0)
        {std::make_any<double>(1.0),
         std::make_any<double>(3.0),
         true,
         true,
         0b1110'1110'0000'000},

        // String range test ["1", "3"] (matches string "1","2","3")
        {std::make_any<std::string>("1"),
         std::make_any<std::string>("3"),
         true,
         true,
         0b0000'0000'1110'000},

        // Range that should match nothing
        {std::make_any<int64_t>(10),
         std::make_any<int64_t>(20),
         true,
         true,
         0b0000'0000'0000'000},

        // Range [2, 4] inclusive (matches int 2,3,4 and float 2.0,3.0,4.0)
        {std::make_any<int64_t>(2),
         std::make_any<int64_t>(4),
         true,
         true,
         0b0111'0111'0000'000},

        // Mixed type range test - int to float [1, 3.0]
        // {std::make_any<int64_t>(1),
        //  std::make_any<double>(3.0),
        //  true,
        //  true,
        //  0b1110'1110'0000'000},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = GetParam(),
            .json_path = "/a",
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, GetParam().ToString()}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    for (auto& [lower, upper, lower_inclusive, upper_inclusive, result] :
         test_cases) {
        proto::plan::GenericValue lower_val;
        proto::plan::GenericValue upper_val;
        if (lower.type() == typeid(int64_t)) {
            lower_val.set_int64_val(std::any_cast<int64_t>(lower));
        } else if (lower.type() == typeid(double)) {
            lower_val.set_float_val(std::any_cast<double>(lower));
        } else if (lower.type() == typeid(std::string)) {
            lower_val.set_string_val(std::any_cast<std::string>(lower));
        }

        if (upper.type() == typeid(int64_t)) {
            upper_val.set_int64_val(std::any_cast<int64_t>(upper));
        } else if (upper.type() == typeid(double)) {
            upper_val.set_float_val(std::any_cast<double>(upper));
        } else if (upper.type() == typeid(std::string)) {
            upper_val.set_string_val(std::any_cast<std::string>(upper));
        }

        BitsetType expect_result;
        expect_result.resize(json_strs.size());
        for (int i = json_strs.size() - 1; result > 0; i--) {
            expect_result.set(i, (result & 0x1) != 0);
            result >>= 1;
        }

        auto binary_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
            lower_val,
            upper_val,
            lower_inclusive,
            upper_inclusive);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_expr);
        auto res =
            ExecuteQueryExpr(plan, seg.get(), json_strs.size(), MAX_TIMESTAMP);
        EXPECT_TRUE(res == expect_result);
    }
}
