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

#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "segcore/Collection.h"
#include "segcore/plan_c.h"
#include "common/CGoHelper.h"

CStatus
CreateSearchPlan(CCollection c_col, const char* dsl, CSearchPlan* res_plan) {
    try {
        auto col = (milvus::segcore::Collection*)c_col;
        auto res = milvus::query::CreatePlan(*col->get_schema(), dsl);
        auto plan = (CSearchPlan)res.release();
        *res_plan = plan;
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    } catch (std::exception& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

// Note: serialized_expr_plan is of binary format
CStatus
CreateSearchPlanByExpr(CCollection c_col, const void* serialized_expr_plan, const int64_t size, CSearchPlan* res_plan) {
    auto col = (milvus::segcore::Collection*)c_col;

    try {
        auto res = milvus::query::CreateSearchPlanByExpr(*col->get_schema(), serialized_expr_plan, size);
        auto plan = (CSearchPlan)res.release();
        *res_plan = plan;
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    } catch (std::exception& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
ParsePlaceholderGroup(CSearchPlan c_plan,
                      const void* placeholder_group_blob,
                      const int64_t blob_size,
                      CPlaceholderGroup* res_placeholder_group) {
    std::string blob_str((char*)placeholder_group_blob, blob_size);
    auto plan = (milvus::query::Plan*)c_plan;
    try {
        auto res = milvus::query::ParsePlaceholderGroup(plan, blob_str);
        auto group = (CPlaceholderGroup)res.release();
        *res_placeholder_group = group;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetNumOfQueries(CPlaceholderGroup placeholder_group, int64_t* nq) {
    auto res = milvus::query::GetNumOfQueries((milvus::query::PlaceholderGroup*)placeholder_group);
    return res;
}

CStatus
GetTopK(CSearchPlan plan, int64_t* topK) {
    auto res = milvus::query::GetTopK((milvus::query::Plan*)plan);
    return res;
}

CStatus
GetMetricType(CSearchPlan plan) {
    auto search_plan = static_cast<milvus::query::Plan*>(plan);
    auto metric_str = milvus::MetricTypeToName(search_plan->plan_node_->search_info_.metric_type_);
    return strdup(metric_str.c_str());
}

CStatus
DeleteSearchPlan(CSearchPlan cPlan) {
    auto plan = (milvus::query::Plan*)cPlan;
    delete plan;
}

CStatus
DeletePlaceholderGroup(CPlaceholderGroup cPlaceholder_group) {
    auto placeHolder_group = (milvus::query::PlaceholderGroup*)cPlaceholder_group;
    delete placeHolder_group;
}

CStatus
CreateRetrievePlanByExpr(CCollection c_col,
                         const void* serialized_expr_plan,
                         const int64_t size,
                         CRetrievePlan* res_plan) {
    auto col = (milvus::segcore::Collection*)c_col;

    try {
        auto res = milvus::query::CreateRetrievePlanByExpr(*col->get_schema(), serialized_expr_plan, size);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto plan = (CRetrievePlan)res.release();
        *res_plan = plan;
        return status;
    } catch (milvus::SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    }
}

CStatus
DeleteRetrievePlan(CRetrievePlan c_plan) {
    auto plan = (milvus::query::RetrievePlan*)c_plan;
    delete plan;
}
