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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/format.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>

#include "boost/container/vector.hpp"
#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "common/VectorTrait.h"
#include "index/IndexFactory.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "segcore/collection_c.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "futures/Future.h"
#include "futures/future_c.h"
#include "test_utils/DataGen.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/PbHelper.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/GenExprProto.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "segcore/load_index_c.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/DataGen.h"
#include "segcore/vector_index_c.h"
#include "common/jsmn.h"

using namespace milvus;
using namespace milvus::test;
using namespace milvus::index;
using namespace milvus::segcore;
using namespace milvus::tracer;
using namespace knowhere;
using milvus::index::VectorIndex;
using milvus::segcore::LoadIndexInfo;

namespace {
// const int DIM = 16;
const int64_t ROW_COUNT = 10 * 1000;
const int64_t BIAS = 4200;

CStatus
CRetrieve(CSegmentInterface c_segment,
          CRetrievePlan c_plan,
          uint64_t timestamp,
          CRetrieveResult** result) {
    auto future = AsyncRetrieve(
        {}, c_segment, c_plan, timestamp, DEFAULT_MAX_OUTPUT_SIZE, false, 0, 0);
    auto futurePtr = static_cast<milvus::futures::IFuture*>(
        static_cast<void*>(static_cast<CFuture*>(future)));

    std::mutex mu;
    mu.lock();
    futurePtr->registerReadyCallback(
        [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();

    auto [retrieveResult, status] = futurePtr->leakyGet();
    future_destroy(future);

    if (status.error_code != 0) {
        return status;
    }
    *result = static_cast<CRetrieveResult*>(retrieveResult);
    return status;
}

CStatus
CRetrieveByOffsets(CSegmentInterface c_segment,
                   CRetrievePlan c_plan,
                   int64_t* offsets,
                   int64_t len,
                   CRetrieveResult** result) {
    auto future = AsyncRetrieveByOffsets({}, c_segment, c_plan, offsets, len);
    auto futurePtr = static_cast<milvus::futures::IFuture*>(
        static_cast<void*>(static_cast<CFuture*>(future)));

    std::mutex mu;
    mu.lock();
    futurePtr->registerReadyCallback(
        [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();

    auto [retrieveResult, status] = futurePtr->leakyGet();
    future_destroy(future);

    if (status.error_code != 0) {
        return status;
    }
    *result = static_cast<CRetrieveResult*>(retrieveResult);
    return status;
}

const char*
get_default_index_meta() {
    static std::string conf = R"(maxIndexRowCount: 1000
                                index_metas: <
                                  fieldID: 100
                                  collectionID: 1001
                                  index_name: "test-index"
                                  type_params: <
                                    key: "dim"
                                    value: "16"
                                  >
                                  index_params: <
                                    key: "index_type"
                                    value: "IVF_FLAT"
                                  >
                                  index_params: <
                                   key: "metric_type"
                                   value: "L2"
                                  >
                                  index_params: <
                                   key: "nlist"
                                   value: "128"
                                  >
                                >)";
    return conf.c_str();
}

auto
generate_data(int N) {
    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine e(42);
    std::normal_distribution<> dis(0.0, 1.0);
    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        float vec[DIM];
        for (auto& x : vec) {
            x = dis(e);
        }
        raw_data.insert(raw_data.end(),
                        (const char*)std::begin(vec),
                        (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(),
                        (const char*)&age,
                        ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}

template <class TraitType>
std::string
generate_collection_schema(std::string metric_type, int dim) {
    namespace schema = milvus::proto::schema;
    GET_SCHEMA_DATA_TYPE_FOR_VECTOR_TRAIT

    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    vec_field_schema->set_data_type(schema_data_type);
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(metric_type);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(dim));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);
    other_field_schema->set_is_primary_key(true);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    std::string schema_string;
    auto marshal = google::protobuf::TextFormat::PrintToString(
        collection_schema, &schema_string);
    assert(marshal);
    return schema_string;
}

IndexBasePtr
generate_index(void* raw_data,
               DataType field_type,
               MetricType metric_type,
               IndexType index_type,
               int64_t dim,
               int64_t N) {
    auto engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    CreateIndexInfo create_index_info{
        field_type, index_type, metric_type, engine_version};
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto database = knowhere::GenDataSet(N, dim, raw_data);
    auto build_config = generate_build_conf(index_type, metric_type);
    indexing->BuildWithDataset(database, build_config);

    auto vec_indexing = dynamic_cast<VectorIndex*>(indexing.get());
    EXPECT_EQ(vec_indexing->Count(), N);
    EXPECT_EQ(vec_indexing->GetDim(), dim);

    return indexing;
}
}  // namespace

TEST(CApiTest, CollectionTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    DeleteCollection(collection);
}

TEST(CApiTest, UpdateSchemaTest) {
    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    // CCollection
    auto collection = NewCollection(schema_string.c_str());

    // create updated schema with extra field
    namespace schema = milvus::proto::schema;
    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    vec_field_schema->set_data_type(schema::DataType::FloatVector);
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(knowhere::metric::L2);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(DIM));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);
    other_field_schema->set_is_primary_key(true);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    auto added_field = collection_schema.add_fields();
    added_field->set_name("added_field");
    added_field->set_fieldid(103);
    added_field->set_data_type(schema::DataType::Int64);
    added_field->set_nullable(true);

    std::string updated_schema_string;
    auto marshal = collection_schema.SerializeToString(&updated_schema_string);
    ASSERT_TRUE(marshal);

    // Call UpdateSchema CApi here
    // UpdateSchema(CCollection, const void*, const int64_t)
    auto status = UpdateSchema(collection,
                               updated_schema_string.c_str(),
                               updated_schema_string.length(),
                               100);
    ASSERT_EQ(status.error_code, Success);

    auto col = static_cast<milvus::segcore::Collection*>(collection);
    auto updated_schema = col->get_schema();
    auto add_field = updated_schema->operator[](FieldId(103));

    ASSERT_EQ(add_field.get_name().get(), "added_field");

    // Test failure case, no panicking with failure code
    status = UpdateSchema(collection, nullptr, 0, 200);
    ASSERT_NE(status.error_code, Success);

    DeleteCollection(collection);
    // free error msg, which shall be responsible for go side to call C.free()
    free(const_cast<char*>(status.error_msg));
}

TEST(CApiTest, LoadInfoTest) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());
    AppendLoadFieldInfo(c_load_info, 100, 100);
    EnableMmap(c_load_info, 100, true);

    EXPECT_TRUE(load_info->field_infos.at(100).enable_mmap);
}

TEST(CApiTest, SetIndexMetaTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());

    milvus::proto::segcore::CollectionIndexMeta indexMeta;
    indexMeta.ParseFromString(get_default_index_meta());
    char buffer[indexMeta.ByteSizeLong()];
    indexMeta.SerializeToArray(buffer, indexMeta.ByteSizeLong());
    SetIndexMeta(collection, buffer, indexMeta.ByteSizeLong());
    DeleteCollection(collection);
}

TEST(CApiTest, GetCollectionNameTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    auto name = GetCollectionName(collection);
    ASSERT_EQ(strcmp(name, "default-collection"), 0);
    DeleteCollection(collection);
    free((void*)(name));
}

TEST(CApiTest, SegmentTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    CSegmentInterface a_segment;
    status = NewSegment(collection, Invalid, -1, &a_segment, false);
    ASSERT_NE(status.error_code, Success);
    DeleteCollection(collection);
    DeleteSegment(segment);
    free((char*)status.error_msg);
}

template <class TraitType>
void
Test_CPlan(const knowhere::MetricType& metric_type) {
    std::string schema_string =
        generate_collection_schema<TraitType>(knowhere::metric::JACCARD, DIM);
    auto collection = NewCollection(schema_string.c_str());

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(TraitType::vector_type);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    void* plan = nullptr;
    auto status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    int64_t field_id = -1;
    status = GetFieldID(plan, &field_id);
    ASSERT_EQ(status.error_code, Success);

    auto col = static_cast<Collection*>(collection);
    for (auto& [target_field_id, field_meta] :
         col->get_schema()->get_fields()) {
        if (field_meta.is_vector()) {
            ASSERT_EQ(field_id, target_field_id.get());
        }
    }
    ASSERT_NE(field_id, -1);

    DeleteSearchPlan(plan);
    DeleteCollection(collection);
}

TEST(CApiTest, CPlan) {
    Test_CPlan<milvus::BinaryVector>(knowhere::metric::JACCARD);
    Test_CPlan<milvus::FloatVector>(knowhere::metric::L2);
    Test_CPlan<milvus::Float16Vector>(knowhere::metric::L2);
    Test_CPlan<milvus::BFloat16Vector>(knowhere::metric::L2);
    Test_CPlan<milvus::Int8Vector>(knowhere::metric::L2);
}

TEST(CApiTest, InsertTest) {
    auto c_collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)c_collection;

    int N = 10000;
    auto dataset = DataGen(col->get_schema(), N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    DeleteCollection(c_collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    std::vector<int64_t> delete_row_ids = {100000, 100001, 100002};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    uint64_t delete_timestamps[] = {0, 0, 0};

    auto del_res = Delete(
        segment, 3, delete_data.data(), delete_data.size(), delete_timestamps);
    ASSERT_EQ(del_res.error_code, Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, MultiDeleteGrowingSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 10;
    auto dataset = DataGen(col->get_schema(), N);
    auto insert_data = serialize(dataset.raw_);

    // insert, pks= {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    int64_t offset;
    PreInsert(segment, N, &offset);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    // delete data pks = {1}
    std::vector<int64_t> delete_pks = {1};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_pks.begin(),
                                               delete_pks.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(1, dataset.timestamps_[N - 1]);
    auto del_res = Delete(segment,
                          1,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks = {1}
    std::vector<proto::plan::GenericValue> retrive_pks;
    {
        proto::plan::GenericValue value;
        value.set_int64_val(1);
        retrive_pks.push_back(value);
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_pks);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;
    auto max_ts = dataset.timestamps_[N - 1] + 10;

    CRetrieveResult* retrieve_result = nullptr;
    res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // retrieve pks = {2}
    {
        proto::plan::GenericValue value;
        value.set_int64_val(2);
        retrive_pks.push_back(value);
    }
    term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_pks);
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 1);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // delete pks = {2}
    delete_pks = {2};
    ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_pks.begin(),
                                               delete_pks.end());
    delete_data = serialize(ids.get());
    delete_timestamps[0]++;
    del_res = Delete(segment,
                     1,
                     delete_data.data(),
                     delete_data.size(),
                     delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks in {2}
    res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetIndexListSizeAndFeatures) {
    int size = GetIndexListSize();
    ASSERT_GT(size, 0);

    std::vector<const char*> index_keys(size);
    std::vector<uint64_t> index_features(size);

    GetIndexFeatures(index_keys.data(), index_features.data());

    for (int i = 0; i < size; i++) {
        ASSERT_NE(index_keys[i], nullptr);
        ASSERT_GT(strlen(index_keys[i]), 0);
        ASSERT_GT(index_features[i], 0);
    }
}

TEST(CApiTest, MultiDeleteSealedSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Sealed, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 10;
    auto dataset = DataGen(col->get_schema(), N);

    auto segment_interface = reinterpret_cast<SegmentInterface*>(segment);
    auto sealed_segment = dynamic_cast<SegmentSealed*>(segment_interface);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareInsertBinlog(
        kCollectionID, kPartitionID, kSegmentID, dataset, cm);
    sealed_segment->LoadFieldData(load_info);

    // delete data pks = {1}
    std::vector<int64_t> delete_pks = {1};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_pks.begin(),
                                               delete_pks.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(1, dataset.timestamps_[N - 1]);
    auto del_res = Delete(segment,
                          1,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks = {1}
    std::vector<proto::plan::GenericValue> retrive_pks;
    {
        proto::plan::GenericValue value;
        value.set_int64_val(1);
        retrive_pks.push_back(value);
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_pks);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();

    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;
    auto max_ts = dataset.timestamps_[N - 1] + 10;

    CRetrieveResult* retrieve_result = nullptr;
    auto res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // retrieve pks = {2}
    {
        proto::plan::GenericValue value;
        value.set_int64_val(2);
        retrive_pks.push_back(value);
    }
    term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_pks);
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 1);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // delete pks = {2}
    delete_pks = {2};
    ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_pks.begin(),
                                               delete_pks.end());
    delete_data = serialize(ids.get());
    delete_timestamps[0]++;
    del_res = Delete(segment,
                     1,
                     delete_data.data(),
                     delete_data.size(),
                     delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks in {2}
    res = CRetrieve(segment, plan.get(), max_ts, &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteRepeatedPksFromGrowingSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 10;
    auto dataset = DataGen(col->get_schema(), N);

    auto insert_data = serialize(dataset.raw_);

    // first insert, pks= {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    int64_t offset;
    PreInsert(segment, N, &offset);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    // second insert, pks= {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    PreInsert(segment, N, &offset);
    res = Insert(segment,
                 offset,
                 N,
                 dataset.row_ids_.data(),
                 dataset.timestamps_.data(),
                 insert_data.data(),
                 insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    // create retrieve plan pks in {1, 2, 3}
    std::vector<proto::plan::GenericValue> retrive_row_ids;
    {
        for (auto v : {1, 2, 3}) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            retrive_row_ids.push_back(val);
        }
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_row_ids);

    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 3);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // delete data pks = {1, 2, 3}
    std::vector<int64_t> delete_row_ids = {1, 2, 3};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(3, dataset.timestamps_[N - 1]);

    auto del_res = Delete(segment,
                          3,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks in {1, 2, 3}
    res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    query_result = std::make_unique<proto::segcore::RetrieveResults>();
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteRepeatedPksFromSealedSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Sealed, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 20;
    auto dataset = DataGen(col->get_schema(), N, 42, 0, 2);

    auto segment_interface = reinterpret_cast<SegmentInterface*>(segment);
    auto sealed_segment = dynamic_cast<SegmentSealed*>(segment_interface);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareInsertBinlog(
        kCollectionID, kPartitionID, kSegmentID, dataset, cm);
    sealed_segment->LoadFieldData(load_info);

    std::vector<proto::plan::GenericValue> retrive_row_ids;
    // create retrieve plan pks in {1, 2, 3}
    {
        for (auto v : {1, 2, 3}) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            retrive_row_ids.push_back(val);
        }
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_row_ids);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    auto res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 6);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // delete data pks = {1, 2, 3}
    std::vector<int64_t> delete_row_ids = {1, 2, 3};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(3, dataset.timestamps_[N - 1]);

    auto del_res = Delete(segment,
                          3,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // retrieve pks in {1, 2, 3}
    res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    query_result = std::make_unique<proto::segcore::RetrieveResults>();
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTestWhenNullable) {
    auto c_collection = NewCollection(get_default_schema_config_nullable());
    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)c_collection;

    int N = 10000;
    auto dataset = DataGen(col->get_schema(), N);
    int64_t ts_offset = 1000;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        c_collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    CSearchResult search_result;
    auto res = CSearch(segment, plan, placeholderGroup, {}, &search_result);
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 = CSearch(segment, plan, placeholderGroup, {}, &search_result2);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(c_collection);
    DeleteSegment(segment);
}

TEST(CApiTest, InsertSamePkAfterDeleteOnGrowingSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, 111, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 10;
    auto dataset = DataGen(col->get_schema(), N);
    auto insert_data = serialize(dataset.raw_);

    // first insert data
    // insert data with pks = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9} , timestamps = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    int64_t offset;
    PreInsert(segment, N, &offset);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    // delete data pks = {1, 2, 3}, timestamps = {9, 9, 9}
    std::vector<int64_t> delete_row_ids = {1, 2, 3};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(3, dataset.timestamps_[N - 1]);

    auto del_res = Delete(segment,
                          3,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // create retrieve plan pks in {1, 2, 3}, timestamp = 9
    std::vector<proto::plan::GenericValue> retrive_row_ids;
    {
        for (auto v : {1, 2, 3}) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            retrive_row_ids.push_back(val);
        }
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_row_ids);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 0);
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    // second insert data
    // insert data with pks = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9} , timestamps = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
    dataset = DataGen(col->get_schema(), N, 42, N);
    insert_data = serialize(dataset.raw_);
    PreInsert(segment, N, &offset);
    res = Insert(segment,
                 offset,
                 N,
                 dataset.row_ids_.data(),
                 dataset.timestamps_.data(),
                 insert_data.data(),
                 insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    // retrieve pks in {1, 2, 3}, timestamp = 19
    res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    query_result = std::make_unique<proto::segcore::RetrieveResults>();
    suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                       retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 3);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, InsertSamePkAfterDeleteOnSealedSegment) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Sealed, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)collection;

    int N = 10;
    auto dataset = DataGen(col->get_schema(), N, 42, 0, 2);

    // insert data with pks = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4} , timestamps = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    auto segment_interface = reinterpret_cast<SegmentInterface*>(segment);
    auto sealed_segment = dynamic_cast<SegmentSealed*>(segment_interface);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareInsertBinlog(
        kCollectionID, kPartitionID, kSegmentID, dataset, cm);
    sealed_segment->LoadFieldData(load_info);

    // delete data pks = {1, 2, 3}, timestamps = {4, 4, 4}
    std::vector<int64_t> delete_row_ids = {1, 2, 3};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    std::vector<uint64_t> delete_timestamps(3, dataset.timestamps_[4]);

    auto del_res = Delete(segment,
                          3,
                          delete_data.data(),
                          delete_data.size(),
                          delete_timestamps.data());
    ASSERT_EQ(del_res.error_code, Success);

    // create retrieve plan pks in {1, 2, 3}, timestamp = 9
    std::vector<proto::plan::GenericValue> retrive_row_ids;
    {
        for (auto v : {1, 2, 3}) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            retrive_row_ids.push_back(val);
        }
    }
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        retrive_row_ids);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    auto res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->ids().int_id().data().size(), 4);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);
    retrieve_result = nullptr;

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTest) {
    auto c_collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)c_collection;

    int N = 10000;
    auto dataset = DataGen(col->get_schema(), N);
    int64_t ts_offset = 1000;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        c_collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    CSearchResult search_result;
    auto res =
        CSearch(segment, plan, placeholderGroup, ts_offset, &search_result);
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 =
        CSearch(segment, plan, placeholderGroup, ts_offset, &search_result2);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(c_collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTestWithExpr) {
    auto c_collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)c_collection;

    int N = 10000;
    auto dataset = DataGen(col->get_schema(), N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: 10
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                         >)";

    int num_queries = 10;
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        c_collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    CSearchResult search_result;
    auto res = CSearch(segment,
                       plan,
                       placeholderGroup,
                       dataset.timestamps_[0],
                       &search_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(c_collection);
    DeleteSegment(segment);
}

TEST(CApiTest, RetrieveTestWithExpr) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(schema);

    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    // create retrieve plan "age in [0]"
    std::vector<proto::plan::GenericValue> values;
    {
        for (auto v : {1, 0}) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            FieldId(101), DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids{FieldId(100), FieldId(101)};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    auto res = CRetrieve(
        segment, plan.get(), dataset.timestamps_[0], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    // Test Retrieve by offsets.
    int64_t offsets[] = {0, 1, 2};
    CRetrieveResult* retrieve_by_offsets_result = nullptr;
    res = CRetrieveByOffsets(
        segment, plan.get(), offsets, 3, &retrieve_by_offsets_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);
    DeleteRetrieveResult(retrieve_by_offsets_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetMemoryUsageInBytesTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto old_memory_usage_size = GetMemoryUsageInBytes(segment);
    // std::cout << "old_memory_usage_size = " << old_memory_usage_size << std::endl;
    ASSERT_EQ(old_memory_usage_size, 0);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetDeletedCountTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    std::vector<int64_t> delete_row_ids = {100000, 100001, 100002};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    uint64_t delete_timestamps[] = {0, 0, 0};

    auto del_res = Delete(
        segment, 3, delete_data.data(), delete_data.size(), delete_timestamps);
    ASSERT_EQ(del_res.error_code, Success);

    // TODO: assert(deleted_count == len(delete_row_ids))
    auto deleted_count = GetDeletedCount(segment);
    ASSERT_EQ(deleted_count, 0);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetRowCountTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    auto row_count = GetRowCount(segment);
    ASSERT_EQ(row_count, N);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetRealCount) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto res = Insert(segment,
                      offset,
                      N,
                      dataset.row_ids_.data(),
                      dataset.timestamps_.data(),
                      insert_data.data(),
                      insert_data.size());
    ASSERT_EQ(res.error_code, Success);

    auto pks = dataset.get_col<int64_t>(schema->get_primary_field_id().value());
    std::vector<int64_t> delete_row_ids(pks.begin(), pks.begin() + 3);
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(delete_row_ids.begin(),
                                               delete_row_ids.end());
    auto delete_data = serialize(ids.get());
    uint64_t delete_timestamps[] = {dataset.timestamps_[N - 1] + 1,
                                    dataset.timestamps_[N - 1] + 2,
                                    dataset.timestamps_[N - 1] + 3};

    auto del_res = Delete(
        segment, 3, delete_data.data(), delete_data.size(), delete_timestamps);
    ASSERT_EQ(del_res.error_code, Success);

    auto real_count = GetRealCount(segment);
    ASSERT_EQ(real_count, N - delete_row_ids.size());

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceNullResult) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);
    int64_t offset;

    PreInsert(segment, N, &offset);
    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;

    auto blob = generate_max_float_query_data(num_queries, num_queries / 2);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    {
        auto slice_nqs = std::vector<int64_t>{10};
        auto slice_topKs = std::vector<int64_t>{1};
        std::vector<CSearchResult> results;
        CSearchResult res;
        status = CSearch(segment, plan, placeholderGroup, 1L << 63, &res);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res);
        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);

        auto search_result = (SearchResult*)results[0];
        auto size = search_result->result_offsets_.size();
        EXPECT_EQ(size, num_queries / 2);

        DeleteSearchResult(res);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceRemoveDuplicates) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;
    int topK = 10;

    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    {
        auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
        auto slice_topKs = std::vector<int64_t>{topK / 2, topK};
        std::vector<CSearchResult> results;
        CSearchResult res1, res2;
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res2);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res1);
        results.push_back(res2);

        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);
        // TODO:: insert no duplicate pks and check reduce results
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }
    {
        int nq1 = num_queries / 3;
        int nq2 = num_queries / 3;
        int nq3 = num_queries - nq1 - nq2;
        auto slice_nqs = std::vector<int64_t>{nq1, nq2, nq3};
        auto slice_topKs = std::vector<int64_t>{topK / 2, topK, topK};
        std::vector<CSearchResult> results;
        CSearchResult res1, res2, res3;
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res2);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res3);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res1);
        results.push_back(res2);
        results.push_back(res3);
        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);
        // TODO:: insert no duplicate pks and check reduce results
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
        DeleteSearchResult(res3);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

template <class TraitType>
void
testReduceSearchWithExpr(int N,
                         int topK,
                         int num_queries,
                         bool filter_all = false) {
    std::cerr << "testReduceSearchWithExpr(" << N << ", " << topK << ", "
              << num_queries << ")" << std::endl;

    auto collection =
        NewCollection(get_default_schema_config<TraitType>().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
               topK;

    // construct the predicate that filter out all data
    if (filter_all) {
        fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                                unary_range_expr: <
                                                    column_info: <
                                                    field_id: 101
                                                    data_type: Int64
                                                    >
                                                    op: GreaterThan
                                                    value: <
                                                    int64_val: %2%
                                                    >
                                                >
                                            >
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
              topK % N;
    }
    auto serialized_expr_plan = fmt.str();
    auto blob = generate_query_data<TraitType>(num_queries);

    void* plan = nullptr;
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    std::vector<CSearchResult> results;
    CSearchResult res1;
    CSearchResult res2;
    auto res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res1);
    ASSERT_EQ(res.error_code, Success);
    res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res2);
    ASSERT_EQ(res.error_code, Success);
    results.push_back(res1);
    results.push_back(res2);

    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    if (num_queries == 1) {
        slice_nqs = std::vector<int64_t>{num_queries};
    }
    auto slice_topKs = std::vector<int64_t>{topK / 2, topK};
    if (topK == 1) {
        slice_topKs = std::vector<int64_t>{topK, topK};
    }

    // 1. reduce
    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    auto search_result_data_blobs =
        reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(
            cSearchResultData);

    // check result
    for (size_t i = 0; i < slice_nqs.size(); i++) {
        milvus::proto::schema::SearchResultData search_result_data;
        auto suc = search_result_data.ParseFromArray(
            search_result_data_blobs->blobs[i].data(),
            search_result_data_blobs->blobs[i].size());
        ASSERT_TRUE(suc);
        ASSERT_EQ(search_result_data.num_queries(), slice_nqs[i]);
        ASSERT_EQ(search_result_data.top_k(), slice_topKs[i]);
        ASSERT_EQ(search_result_data.ids().int_id().data_size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);
        ASSERT_EQ(search_result_data.scores().size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);

        // check real topks
        ASSERT_EQ(search_result_data.topks().size(), slice_nqs[i]);
        for (auto real_topk : search_result_data.topks()) {
            ASSERT_LE(real_topk, slice_topKs[i]);
            if (filter_all) {
                ASSERT_EQ(real_topk, 0);
            }
        }
    }

    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceSearchWithExpr) {
    // float32
    testReduceSearchWithExpr<milvus::FloatVector>(2, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::FloatVector>(100, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(100, 10, 10);
    testReduceSearchWithExpr<milvus::FloatVector>(10000, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(10000, 10, 10);
    // float16
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::Float16Vector>(100, 10, 10);
    // bfloat16
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::BFloat16Vector>(100, 10, 10);
    // int8
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::Int8Vector>(100, 10, 10);
}

TEST(CApiTest, ReduceSearchWithExprFilterAll) {
    // float32
    testReduceSearchWithExpr<milvus::FloatVector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::FloatVector>(2, 10, 10, true);
    // float16
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 10, 10, true);
    // bfloat16
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 10, 10, true);
    // int8
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 10, 10, true);
}

TEST(CApiTest, LoadIndexInfo) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    auto indexing = get_index_obj.value();
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, DIM},
                       {knowhere::meta::TOPK, TOPK},
                       {knowhere::indexparam::NLIST, 100},
                       {knowhere::indexparam::NPROBE, 4}};

    auto database = knowhere::GenDataSet(N, DIM, raw_data.data());
    indexing.Train(database, conf);
    indexing.Add(database, conf);
    EXPECT_EQ(indexing.Count(), N);
    EXPECT_EQ(indexing.Dim(), DIM);
    knowhere::BinarySet binary_set;
    indexing.Serialize(binary_set);
    CBinarySet c_binary_set = (CBinarySet)&binary_set;

    void* c_load_index_info = nullptr;
    auto status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_param_key1 = "index_type";
    std::string index_param_value1 = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
    status = AppendIndexParam(
        c_load_index_info, index_param_key1.data(), index_param_value1.data());
    std::string index_param_key2 = knowhere::meta::METRIC_TYPE;
    std::string index_param_value2 = knowhere::metric::L2;
    status = AppendIndexParam(
        c_load_index_info, index_param_key2.data(), index_param_value2.data());
    ASSERT_EQ(status.error_code, Success);
    std::string field_name = "field0";
    status = AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 0, CDataType::FloatVector, false, "");
    ASSERT_EQ(status.error_code, Success);
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    status = AppendIndex(c_load_index_info, c_binary_set);
    ASSERT_EQ(status.error_code, Success);
    DeleteLoadIndexInfo(c_load_index_info);
}

TEST(CApiTest, LoadIndexSearch) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto num_query = 100;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    auto indexing = get_index_obj.value();
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, DIM},
                       {knowhere::meta::TOPK, TOPK},
                       {knowhere::indexparam::NLIST, 100},
                       {knowhere::indexparam::NPROBE, 4}};

    auto database = knowhere::GenDataSet(N, DIM, raw_data.data());
    indexing.Train(database, conf);
    indexing.Add(database, conf);

    EXPECT_EQ(indexing.Count(), N);
    EXPECT_EQ(indexing.Dim(), DIM);

    // serializ index to binarySet
    knowhere::BinarySet binary_set;
    indexing.Serialize(binary_set);

    // fill loadIndexInfo
    milvus::segcore::LoadIndexInfo load_index_info;
    auto& index_params = load_index_info.index_params;
    index_params["index_type"] = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
    auto index = std::make_unique<VectorMemIndex<float>>(
        index_params["index_type"],
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    index->Load(binary_set);
    index_params = GenIndexParams(index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(index));

    // search
    auto query_dataset =
        knowhere::GenDataSet(num_query, DIM, raw_data.data() + BIAS * DIM);

    auto result = indexing.Search(query_dataset, conf, nullptr);
}

template <class TraitType>
void
Test_Indexing_Without_Predicate() {
    GET_ELEM_TYPE_FOR_VECTOR_TRAIT

    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<TraitType>(knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<elem_type>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(TraitType::vector_type);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(5);
    query_info->set_round_decimal(-1);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob<TraitType>(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    Timestamp timestmap = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestmap,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   TraitType::data_type,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, TraitType::c_data_type, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);
    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestmap,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_raw_index_json =
        SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json =
        SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));

    ASSERT_EQ(search_result_on_raw_index_json.dump(1),
              search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Without_Predicate) {
    Test_Indexing_Without_Predicate<milvus::FloatVector>();
    Test_Indexing_Without_Predicate<milvus::Float16Vector>();
    Test_Indexing_Without_Predicate<milvus::BFloat16Vector>();
    Test_Indexing_Without_Predicate<milvus::Int8Vector>();
}

TEST(CApiTest, Indexing_Expr_Without_Predicate) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                             field_id: 100
                                             query_info: <
                                                 topk: 5
                                                 round_decimal: -1
                                                 metric_type: "L2"
                                                 search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
                                          >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_raw_index_json =
        SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json =
        SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));

    ASSERT_EQ(search_result_on_raw_index_json.dump(1),
              search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_float_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: GreaterEqual
                                        value: <
                                          int64_val: 4200
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
                                          int64_val: 4210
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 5
                                  round_decimal: -1
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_float_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = 1000 * 10;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    {
        int64_t offset;
        PreInsert(segment, N, &offset);

        auto insert_data = serialize(dataset.raw_);
        auto ins_res = Insert(segment,
                              offset,
                              N,
                              dataset.row_ids_.data(),
                              dataset.timestamps_.data(),
                              insert_data.data(),
                              insert_data.size());
        ASSERT_EQ(ins_res.error_code, Success);
    }

    const char* serialized_expr_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               binary_expr: <
                                                 op: LogicalAnd
                                                 left: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: GreaterEqual
                                                     value: <
                                                       int64_val: 4200
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
                                                       int64_val: 4210
                                                     >
                                                   >
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "L2"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_float_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               term_expr: <
                                                 column_info: <
                                                   field_id: 101
                                                   data_type: Int64
                                                 >
                                                 values: <
                                                   int64_val: 4200
                                                 >
                                                 values: <
                                                   int64_val: 4201
                                                 >
                                                 values: <
                                                   int64_val: 4202
                                                 >
                                                 values: <
                                                   int64_val: 4203
                                                 >
                                                 values: <
                                                   int64_val: 4204
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "L2"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_float_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = 1000 * 10;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(
 vector_anns: <
   field_id: 100
   predicates: <
     term_expr: <
       column_info: <
         field_id: 101
         data_type: Int64
       >
       values: <
         int64_val: 4200
       >
       values: <
         int64_val: 4201
       >
       values: <
         int64_val: 4202
       >
       values: <
         int64_val: 4203
       >
       values: <
         int64_val: 4204
       >
     >
   >
   query_info: <
     topk: 5
     round_decimal: -1
     metric_type: "L2"
     search_params: "{\"nprobe\": 10}"
   >
   placeholder_tag: "$0"
 >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_binary_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, DIM);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               binary_expr: <
                                                 op: LogicalAnd
                                                 left: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: GreaterEqual
                                                     value: <
                                                       int64_val: 4200
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
                                                       int64_val: 4210
                                                     >
                                                   >
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "JACCARD"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment

    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_binary_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, DIM);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              binary_expr: <
                                                op: LogicalAnd
                                                left: <
                                                  unary_range_expr: <
                                                    column_info: <
                                                      field_id: 101
                                                      data_type: Int64
                                                    >
                                                    op: GreaterEqual
                                                    value: <
                                                      int64_val: 4200
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
                                                      int64_val: 4210
                                                    >
                                                  >
                                                >
                                              >
                                            >
                                            query_info: <
                                              topk: 5
                                              round_decimal: -1
                                              metric_type: "JACCARD"
                                              search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                        >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_TRUE(res_before_load_index.error_code == Success)
        << res_before_load_index.error_msg;

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, DIM);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               term_expr: <
                                                 column_info: <
                                                   field_id: 101
                                                   data_type: Int64
                                                 >
                                                 values: <
                                                   int64_val: 4200
                                                 >
                                                 values: <
                                                   int64_val: 4201
                                                 >
                                                 values: <
                                                   int64_val: 4202
                                                 >
                                                 values: <
                                                   int64_val: 4203
                                                 >
                                                 values: <
                                                   int64_val: 4204
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "JACCARD"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    int topK = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);

    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    //    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    //    ASSERT_EQ(status.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        ASSERT_EQ(search_result_on_bigIndex->topk_per_nq_prefix_sum_.size(),
                  search_result_on_bigIndex->total_nq_ + 1);
        auto offset = search_result_on_bigIndex->topk_per_nq_prefix_sum_[i];
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[i * TOPK]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
    DeleteSearchResultDataBlobs(cSearchResultData);
}

TEST(CApiTest, Indexing_Expr_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, DIM);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              term_expr: <
                                                column_info: <
                                                  field_id: 101
                                                  data_type: Int64
                                                >
                                                values: <
                                                  int64_val: 4200
                                                >
                                                values: <
                                                  int64_val: 4201
                                                >
                                                values: <
                                                  int64_val: 4202
                                                >
                                                values: <
                                                  int64_val: 4203
                                                >
                                                values: <
                                                  int64_val: 4204
                                                >
                                              >
                                            >
                                            query_info: <
                                              topk: 5
                                              round_decimal: -1
                                              metric_type: "JACCARD"
                                              search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                        >)";

    // create place_holder_group
    int num_queries = 5;
    int topK = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);

    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        ASSERT_EQ(search_result_on_bigIndex->topk_per_nq_prefix_sum_.size(),
                  search_result_on_bigIndex->total_nq_ + 1);
        auto offset = search_result_on_bigIndex->topk_per_nq_prefix_sum_[i];
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[i * TOPK]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
    DeleteSearchResultDataBlobs(cSearchResultData);
}

TEST(CApiTest, SealedSegment_search_float_Predicate_Range) {
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Sealed, -1, &segment, true);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: GreaterEqual
                                        value: <
                                          int64_val: 4200
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
                                          int64_val: 4210
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 5
                                  round_decimal: -1
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);
    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    SearchInfo search_info;
    search_info.topk_ = TOPK;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.search_params_ = generate_search_conf(
        IndexEnum::INDEX_FAISS_IVFSQ8, knowhere::metric::L2);
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    EXPECT_EQ(result_on_index.distances_.size(), num_queries * TOPK);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto excluded_field_ids = GetExcludedFieldIds(dataset.schema_, {0, 1, 101});
    auto load_info = PrepareInsertBinlog(kCollectionID,
                                         kPartitionID,
                                         kSegmentID,
                                         dataset,
                                         cm,
                                         "",
                                         excluded_field_ids);
    status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_without_predicates) {
    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Sealed, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    uint64_t ts_offset = 1000;
    auto dataset = DataGen(schema, ROW_COUNT, ts_offset);

    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                query_info: <
                                  topk: 5
                                  round_decimal: -1
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto excluded_field_ids =
        GetExcludedFieldIds(dataset.schema_, {0, 1, 100, 101});
    auto load_info = PrepareInsertBinlog(kCollectionID,
                                         kPartitionID,
                                         kSegmentID,
                                         dataset,
                                         cm,
                                         "",
                                         excluded_field_ids);
    status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);

    int num_queries = 10;
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    CSearchResult search_result;
    auto res = CSearch(
        segment, plan, placeholderGroup, ROW_COUNT + ts_offset, &search_result);
    std::cout << res.error_msg << std::endl;
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 = CSearch(segment,
                        plan,
                        placeholderGroup,
                        ROW_COUNT + ts_offset,
                        &search_result2);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_float_With_Expr_Predicate_Range) {
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    auto counter_col = dataset.get_col<int64_t>(FieldId(101));

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              binary_expr: <
                                                op: LogicalAnd
                                                left: <
                                                  unary_range_expr: <
                                                    column_info: <
                                                      field_id: 101
                                                      data_type: Int64
                                                    >
                                                    op: GreaterEqual
                                                    value: <
                                                      int64_val: 4200
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
                                                      int64_val: 4210
                                                    >
                                                  >
                                                >
                                              >
                                            >
                                            query_info: <
                                              topk: 5
                                              round_decimal: -1
                                              metric_type: "L2"
                                              search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                        >)";

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto segment = CreateSealedWithFieldDataLoaded(schema, dataset);

    // load vec index
    status = UpdateSealedSegmentIndex(segment.get(), c_load_index_info);
    ASSERT_EQ(status.error_code, Success);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(query_dataset, search_info, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
}

TEST(CApiTest, GrowingSegment_Load_Field_Data) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto str_fid = schema->AddDebugField("string", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "vector_float", DataType::VECTOR_FLOAT, DIM, "L2");
    schema->set_primary_field_id(str_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta).release();

    int N = ROW_COUNT;
    auto raw_data = DataGen(schema, N);

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(1, 2, 3, raw_data, cm);

    auto status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), ROW_COUNT);
    ASSERT_NE(segment->get_field_avg_size(str_fid), 0);

    DeleteSegment(segment);
}

TEST(CApiTest, GrowingSegment_Load_Field_Data_Lack_Binlog_Rows) {
    double double_default_value = 20;
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto str_fid = schema->AddDebugField("string", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "vector_float", DataType::VECTOR_FLOAT, DIM, "L2");
    schema->set_primary_field_id(str_fid);

    int N = ROW_COUNT;
    auto raw_data = DataGen(schema, N);

    auto lack_null_binlog_id =
        schema->AddDebugField("lack_null_binlog", DataType::INT8, true);
    DefaultValueType default_value;
    default_value.set_double_data(double_default_value);

    auto lack_default_value_binlog_id = schema->AddDebugFieldWithDefaultValue(
        "lack_default_value_binlog", DataType::DOUBLE, default_value);

    raw_data.schema_ = schema;

    auto segment = CreateGrowingSegment(schema, empty_index_meta).release();

    std::vector<int8_t> data1(N / 2);
    FixedVector<bool> valid_data1(N / 2);
    for (int i = 0; i < N / 2; i++) {
        data1[i] = random() % N;
        valid_data1[i] = rand() % 2 == 0 ? true : false;
    }

    auto field_meta = schema->operator[](lack_null_binlog_id);
    auto array = milvus::segcore::CreateDataArrayFrom(
        data1.data(), valid_data1.data(), N / 2, field_meta);

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(1, 2, 3, raw_data, cm);
    raw_data.raw_->mutable_fields_data()->AddAllocated(array.release());

    load_info.field_infos.emplace(
        lack_null_binlog_id.get(),
        FieldBinlogInfo{lack_null_binlog_id.get(),
                        static_cast<int64_t>(ROW_COUNT),
                        std::vector<int64_t>{int64_t(0)},
                        std::vector<int64_t>{0},
                        false,
                        std::vector<std::string>{}});

    load_info.field_infos.emplace(
        lack_default_value_binlog_id.get(),
        FieldBinlogInfo{lack_default_value_binlog_id.get(),
                        static_cast<int64_t>(ROW_COUNT),
                        std::vector<int64_t>{int64_t(0)},
                        std::vector<int64_t>{0},
                        false,
                        std::vector<std::string>{}});

    auto status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), ROW_COUNT);
    ASSERT_NE(segment->get_field_avg_size(str_fid), 0);

    DeleteSegment(segment);
}

TEST(CApiTest, DISABLED_SealedSegment_Load_Field_Data_Lack_Binlog_Rows) {
    double double_default_value = 20;
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto str_fid = schema->AddDebugField("string", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "vector_float", DataType::VECTOR_FLOAT, DIM, "L2");
    schema->set_primary_field_id(str_fid);

    int N = ROW_COUNT;
    auto raw_data = DataGen(schema, N);

    auto lack_null_binlog_id =
        schema->AddDebugField("lack_null_binlog", DataType::INT8, true);
    DefaultValueType default_value;
    default_value.set_double_data(double_default_value);

    auto lack_default_value_binlog_id = schema->AddDebugFieldWithDefaultValue(
        "lack_default_value_binlog", DataType::DOUBLE, default_value);

    raw_data.schema_ = schema;

    auto segment = CreateSealedSegment(schema, empty_index_meta).release();

    std::vector<int8_t> data1(N / 2);
    FixedVector<bool> valid_data1(N / 2);
    for (int i = 0; i < N / 2; i++) {
        data1[i] = random() % N;
        valid_data1[i] = rand() % 2 == 0 ? true : false;
    }

    auto field_meta = schema->operator[](lack_null_binlog_id);
    auto array = milvus::segcore::CreateDataArrayFrom(
        data1.data(), valid_data1.data(), N / 2, field_meta);

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(1, 2, 3, raw_data, cm);
    raw_data.raw_->mutable_fields_data()->AddAllocated(array.release());

    load_info.field_infos.emplace(
        lack_null_binlog_id.get(),
        FieldBinlogInfo{lack_null_binlog_id.get(),
                        static_cast<int64_t>(ROW_COUNT),
                        std::vector<int64_t>{int64_t(0)},
                        std::vector<int64_t>{0},
                        false,
                        std::vector<std::string>{}});

    load_info.field_infos.emplace(
        lack_default_value_binlog_id.get(),
        FieldBinlogInfo{lack_default_value_binlog_id.get(),
                        static_cast<int64_t>(ROW_COUNT),
                        std::vector<int64_t>{int64_t(0)},
                        std::vector<int64_t>{0},
                        false,
                        std::vector<std::string>{}});

    auto status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), ROW_COUNT);
    ASSERT_NE(segment->get_field_avg_size(str_fid), 0);

    DeleteSegment(segment);
}

TEST(CApiTest, RetrieveScalarFieldFromSealedSegmentWithIndex) {
    auto schema = std::make_shared<Schema>();
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto segment = CreateSealedSegment(schema).release();

    int N = ROW_COUNT;
    auto raw_data = DataGen(schema, N);
    LoadIndexInfo load_index_info;

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto excluded_field_ids =
        GetExcludedFieldIds(raw_data.schema_, {0, 1, i64_fid.get()});
    auto load_info = PrepareInsertBinlog(kCollectionID,
                                         kPartitionID,
                                         kSegmentID,
                                         raw_data,
                                         cm,
                                         "",
                                         excluded_field_ids);
    auto status = LoadFieldData(segment, &load_info);
    ASSERT_EQ(status.error_code, Success);

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_fid);
    GenScalarIndexing(N, age8_col.data());
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data());
    load_index_info.field_id = i8_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index_params = GenIndexParams(age8_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age8_index));
    segment->LoadIndex(load_index_info);

    // load index for 16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_fid);
    GenScalarIndexing(N, age16_col.data());
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data());
    load_index_info.field_id = i16_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index_params = GenIndexParams(age16_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age16_index));
    segment->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    GenScalarIndexing(N, age32_col.data());
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data());
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    segment->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    GenScalarIndexing(N, age64_col.data());
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    segment->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_fid);
    GenScalarIndexing(N, age_float_col.data());
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data());
    load_index_info.field_id = float_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_float_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_float_index));
    segment->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_fid);
    GenScalarIndexing(N, age_double_col.data());
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data());
    load_index_info.field_id = double_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_double_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_double_index));
    segment->LoadIndex(load_index_info);

    // create retrieve plan
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    std::vector<proto::plan::GenericValue> retrive_row_ids;
    proto::plan::GenericValue val;
    val.set_int64_val(age64_col[0]);
    retrive_row_ids.push_back(val);
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            i64_fid, DataType::INT64, std::vector<std::string>()),
        retrive_row_ids);
    plan->plan_node_->plannodes_ = CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_field_ids;

    // retrieve value
    target_field_ids = {
        i8_fid, i16_fid, i32_fid, i64_fid, float_fid, double_fid};
    plan->field_ids_ = target_field_ids;

    CRetrieveResult* retrieve_result = nullptr;
    auto res = CRetrieve(
        segment, plan.get(), raw_data.timestamps_[N - 1], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);
    auto query_result = std::make_unique<proto::segcore::RetrieveResults>();
    auto suc = query_result->ParseFromArray(retrieve_result->proto_blob,
                                            retrieve_result->proto_size);
    ASSERT_TRUE(suc);
    ASSERT_EQ(query_result->fields_data().size(), 6);
    auto fields_data = query_result->fields_data();
    for (auto iter = fields_data.begin(); iter < fields_data.end(); ++iter) {
        switch (iter->type()) {
            case proto::schema::DataType::Int8: {
                ASSERT_EQ(iter->scalars().int_data().data(0), age8_col[0]);
                break;
            }
            case proto::schema::DataType::Int16: {
                ASSERT_EQ(iter->scalars().int_data().data(0), age16_col[0]);
                break;
            }
            case proto::schema::DataType::Int32: {
                ASSERT_EQ(iter->scalars().int_data().data(0), age32_col[0]);
                break;
            }
            case proto::schema::DataType::Int64: {
                ASSERT_EQ(iter->scalars().long_data().data(0), age64_col[0]);
                break;
            }
            case proto::schema::DataType::Float: {
                ASSERT_EQ(iter->scalars().float_data().data(0),
                          age_float_col[0]);
                break;
            }
            case proto::schema::DataType::Double: {
                ASSERT_EQ(iter->scalars().double_data().data(0),
                          age_double_col[0]);
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid, "not supported type");
            }
        }
    }

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(retrieve_result);

    DeleteSegment(segment);
}

template <class TraitType>
void
Test_Range_Search_With_Radius_And_Range_Filter() {
    auto c_collection =
        NewCollection(get_default_schema_config<TraitType>().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto col = (milvus::segcore::Collection*)c_collection;

    int N = 10000;
    auto dataset = DataGen(col->get_schema(), N);
    int64_t ts_offset = 1000;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             query_info: <
                                               topk: 10
                                               round_decimal: 3
                                               metric_type: "L2"
                                               search_params: "{\"nprobe\": 10,\"radius\": 20, \"range_filter\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    int num_queries = 10;
    auto blob = generate_query_data<TraitType>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        c_collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    CSearchResult search_result;
    auto res =
        CSearch(segment, plan, placeholderGroup, ts_offset, &search_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(c_collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Range_Search_With_Radius_And_Range_Filter) {
    Test_Range_Search_With_Radius_And_Range_Filter<milvus::FloatVector>();
    Test_Range_Search_With_Radius_And_Range_Filter<milvus::Float16Vector>();
    Test_Range_Search_With_Radius_And_Range_Filter<milvus::BFloat16Vector>();
    Test_Range_Search_With_Radius_And_Range_Filter<milvus::Int8Vector>();
}

std::vector<SegOffset>
search_id(const BitsetType& bitset,
          Timestamp* timestamps,
          Timestamp timestamp,
          bool use_find) {
    std::vector<SegOffset> dst_offset;
    if (use_find) {
        auto i = bitset.find_first();
        while (i.has_value()) {
            auto offset = SegOffset(i.value());
            if (timestamps[offset.get()] <= timestamp) {
                dst_offset.push_back(offset);
            }

            i = bitset.find_next(i.value());
        }

        return dst_offset;
    } else {
        for (int i = 0; i < bitset.size(); i++) {
            if (bitset[i]) {
                auto offset = SegOffset(i);
                if (timestamps[offset.get()] <= timestamp) {
                    dst_offset.push_back(offset);
                }
            }
        }
    }
    return dst_offset;
}

TEST(CApiTest, SearchIdTest) {
    //    using BitsetType = boost::dynamic_bitset<>;

    auto test = [&](int NT) {
        BitsetType bitset(1000000);
        Timestamp* timestamps = new Timestamp[1000000];
        srand(time(NULL));
        for (int i = 0; i < 1000000; i++) {
            timestamps[i] = i;
            bitset[i] = false;
        }
        for (int i = 0; i < NT; i++) {
            bitset[1000000 * ((double)rand() / RAND_MAX)] = true;
        }
        auto start = std::chrono::steady_clock::now();
        auto res1 = search_id(bitset, timestamps, 1000000, true);
        std::cout << "search id cost:"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << "us" << std::endl;
        start = std::chrono::steady_clock::now();
        auto res2 = search_id(bitset, timestamps, 1000000, false);
        std::cout << "search id origin cost:"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << "us" << std::endl;
        ASSERT_EQ(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            if (res1[i].get() != res2[i].get()) {
                std::cout << "error:" << i;
            }
        }
        start = std::chrono::steady_clock::now();
        bitset.flip();
        std::cout << "bit set flip cost:"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << "us" << std::endl;
        delete[] timestamps;
    };

    int test_nt[] = {10, 50, 100};
    for (auto nt : test_nt) {
        test(nt);
    }
}

TEST(CApiTest, IsLoadWithDisk) {
    ASSERT_TRUE(IsLoadWithDisk(INVERTED_INDEX_TYPE, 0));
}

TEST(CApiTest, TestGetValueFromConfig) {
    nlohmann::json cfg = nlohmann::json::parse(
        R"({"a" : 100, "b" : true, "c" : "true", "d" : 1.234, "e" : null})");
    auto a_value = GetValueFromConfig<int64_t>(cfg, "a");
    ASSERT_EQ(a_value.value(), 100);

    auto b_value = GetValueFromConfig<bool>(cfg, "b");
    ASSERT_TRUE(b_value.value());

    auto c_value = GetValueFromConfig<bool>(cfg, "c");
    ASSERT_TRUE(c_value.value());

    auto d_value = GetValueFromConfig<double>(cfg, "d");
    ASSERT_NEAR(d_value.value(), 1.234, 0.001);

    try {
        GetValueFromConfig<std::string>(cfg, "d");
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        ASSERT_EQ(std::string(e.what()).find("config type error for key") !=
                      std::string::npos,
                  true);
    }

    auto e_value = GetValueFromConfig<std::string>(cfg, "e");
    ASSERT_FALSE(e_value.has_value());
}

TEST(CApiTest, TestGetValueFromConfig_Without_Type_Check) {
    nlohmann::json cfg = nlohmann::json::parse(
        R"({"a" : 100, "b" : true, "c" : "true", "d" : 1.234, "e" : "1.234", "f" : null})");
    SetDefaultConfigParamTypeCheck(false);
    auto a_value = GetValueFromConfig<int64_t>(cfg, "a");
    ASSERT_EQ(a_value.value(), 100);
    std::cout << "a_value: " << a_value.value() << std::endl;

    auto b_value = GetValueFromConfig<bool>(cfg, "b");
    ASSERT_TRUE(b_value.value());
    std::cout << "b_value: " << b_value.value() << std::endl;
    auto c_value = GetValueFromConfig<bool>(cfg, "c");
    ASSERT_TRUE(c_value.value());
    std::cout << "c_value: " << c_value.value() << std::endl;
    auto d_value = GetValueFromConfig<double>(cfg, "d");
    ASSERT_NEAR(d_value.value(), 1.234, 0.001);
    std::cout << "d_value: " << d_value.value() << std::endl;
    auto e_value = GetValueFromConfig<double>(cfg, "e");
    ASSERT_FALSE(e_value.has_value());
    auto f_value = GetValueFromConfig<bool>(cfg, "f");
    ASSERT_FALSE(f_value.has_value());
}
