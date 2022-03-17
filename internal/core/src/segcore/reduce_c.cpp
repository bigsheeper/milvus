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

#include <limits>
#include <unordered_set>
#include <vector>
#include <common/CGoHelper.h>

#include "common/Consts.h"
#include "common/Types.h"
#include "common/SearchResult.h"
#include "exceptions/EasyAssert.h"
#include "log/Log.h"
#include "pb/milvus.pb.h"
#include "query/Plan.h"
#include "segcore/Reduce.h"
#include "segcore/ReduceStructure.h"
#include "segcore/SegmentInterface.h"
#include "segcore/reduce_c.h"

using SearchResult = milvus::SearchResult;

int
MergeInto(int64_t num_queries, int64_t topk, float* distances, int64_t* uids, float* new_distances, int64_t* new_uids) {
    auto status = milvus::segcore::merge_into(num_queries, topk, distances, uids, new_distances, new_uids);
    return status.code();
}

struct MarshaledHitsPerGroup {
    std::vector<std::string> hits_;
    std::vector<int64_t> blob_length_;
};

struct MarshaledHits {
    explicit MarshaledHits(int64_t num_group) {
        marshaled_hits_.resize(num_group);
    }

    int
    get_num_group() {
        return marshaled_hits_.size();
    }

    std::vector<MarshaledHitsPerGroup> marshaled_hits_;
};

void
DeleteMarshaledHits(CMarshaledHits c_marshaled_hits) {
    auto hits = (MarshaledHits*)c_marshaled_hits;
    delete hits;
}

// void
// PrintSearchResult(char* buf, const milvus::SearchResult* result, int64_t seg_idx, int64_t from, int64_t to) {
//    const int64_t MAXLEN = 32;
//    snprintf(buf + strlen(buf), MAXLEN, "{ seg No.%ld ", seg_idx);
//    for (int64_t i = from; i < to; i++) {
//        snprintf(buf + strlen(buf), MAXLEN, "(%ld, %ld, %f), ", i, result->primary_keys_[i], result->distances_[i]);
//    }
//    snprintf(buf + strlen(buf), MAXLEN, "} ");
//}

void
ReduceResultData(std::vector<SearchResult*>& search_results, int64_t nq, int64_t topk) {
    AssertInfo(topk > 0, "topk must greater than 0");
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        AssertInfo(search_result->primary_keys_.size() == nq * topk, "incorrect search result primary key size");
        AssertInfo(search_result->distances_.size() == nq * topk, "incorrect search result distance size");
    }

    std::vector<std::vector<int64_t>> search_records(num_segments);
    std::unordered_set<int64_t> pk_set;
    int64_t skip_dup_cnt = 0;

    // reduce search results
    for (int64_t qi = 0; qi < nq; qi++) {
        std::vector<SearchResultPair> result_pairs;
        int64_t base_offset = qi * topk;
        for (int i = 0; i < num_segments; i++) {
            auto search_result = search_results[i];
            auto primary_key = search_result->primary_keys_[base_offset];
            auto distance = search_result->distances_[base_offset];
            result_pairs.push_back(
                SearchResultPair(primary_key, distance, search_result, i, base_offset, base_offset + topk));
        }
        int64_t curr_offset = base_offset;

#if 0
        for (int i = 0; i < topk; ++i) {
            result_pairs[0].reset_distance();
            std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
            auto& result_pair = result_pairs[0];
            auto index = result_pair.index_;
            result_pair.search_result_->result_offsets_.push_back(loc_offset++);
            search_records[index].push_back(result_pair.offset_++);
        }
#else
        pk_set.clear();
        while (curr_offset - base_offset < topk) {
            std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
            auto& pilot = result_pairs[0];
            auto index = pilot.index_;
            int64_t curr_pk = pilot.primary_key_;
            // remove duplicates
            if (curr_pk == INVALID_ID || pk_set.count(curr_pk) == 0) {
                pilot.search_result_->result_offsets_.push_back(curr_offset++);
                // when inserted data are dirty, it's possible that primary keys are duplicated,
                // in this case, "offset_" may be greater than "offset_rb_" (#10530)
                search_records[index].push_back(pilot.offset_ < pilot.offset_rb_ ? pilot.offset_ : INVALID_OFFSET);
                if (curr_pk != INVALID_ID) {
                    pk_set.insert(curr_pk);
                }
            } else {
                // skip entity with same primary key
                skip_dup_cnt++;
            }
            pilot.reset();
        }
#endif
    }
    LOG_SEGCORE_DEBUG_ << "skip duplicated search result, count = " << skip_dup_cnt;

    // after reduce, remove redundant values in primary_keys, distances and ids
    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        if (search_result->result_offsets_.size() == 0) {
            continue;
        }

        std::vector<int64_t> primary_keys;
        std::vector<float> distances;
        std::vector<int64_t> ids;
        for (int j = 0; j < search_records[i].size(); j++) {
            auto& offset = search_records[i][j];
            primary_keys.push_back(offset != INVALID_OFFSET ? search_result->primary_keys_[offset] : INVALID_ID);
            distances.push_back(offset != INVALID_OFFSET ? search_result->distances_[offset]
                                                         : std::numeric_limits<float>::max());
            ids.push_back(offset != INVALID_OFFSET ? search_result->ids_[offset] : INVALID_ID);
        }

        search_result->primary_keys_ = primary_keys;
        search_result->distances_ = distances;
        search_result->ids_ = ids;
    }
}

void
ReorganizeSearchResultsV2(std::vector<SearchResult*>& search_results,
                          int32_t nq, int32_t topK,
                          milvus::aligned_vector<int64_t>& result_ids,
                          std::vector<float>& result_distances,
                          std::vector<milvus::aligned_vector<char>>& result_output_fields_data) {
    auto num_segments = search_results.size();
    auto results_count = 0;

    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        AssertInfo(search_result != nullptr, "null search result when reorganize");
        AssertInfo(search_result->output_fields_meta_.size() == result_output_fields_data.size(),
                   "illegal fields meta size"
                   ", fields_meta_size = " + std::to_string(search_result->output_fields_meta_.size()) +
                   ", expected_size = " + std::to_string(result_output_fields_data.size()));
        auto num_results = search_result->result_offsets_.size();
        if (num_results == 0) {
            continue;
        }
#pragma omp parallel for
        for (int j = 0; j < num_results; j++) {
            auto loc = search_result->result_offsets_[j];
            AssertInfo(loc < nq * topK, "result location of out range, location = " + std::to_string(loc));
            // set result ids
            memcpy(&result_ids[loc], &search_result->ids_data_[j * sizeof(int64_t)], sizeof(int64_t));
            // set result distances
            result_distances[loc] = search_result->distances_[j];
            // set result output fields data
            for (int k = 0; k < search_result->output_fields_meta_.size(); k++) {
                auto ele_size = search_result->output_fields_meta_[k].get_sizeof();
                memcpy(&result_output_fields_data[k][loc * ele_size], &search_result->output_fields_data_[k][j * ele_size], ele_size);
            }
        }
        results_count += num_results;
    }

    AssertInfo(results_count == nq * topK, "size of reduce result is less than nq * topK");
}

CProto
GetSearchResultDataSlice(milvus::aligned_vector<int64_t>& result_ids,
                         std::vector<float>& result_distances,
                         std::vector<milvus::aligned_vector<char>>& result_output_fields_data,
                         int32_t nq, int32_t topK,
                         int32_t nq_begin, int32_t nq_end,
                         std::vector<milvus::FieldMeta> output_fields_meta) {
    auto search_result_data = std::make_unique<milvus::proto::schema::SearchResultData>();
    // set topK and nq
    search_result_data->set_top_k(topK);
    search_result_data->set_num_queries(nq);

    auto offset_begin = nq_begin * topK;
    auto offset_end = nq_end * topK;
    AssertInfo(offset_begin <= offset_end, "illegal offsets when GetSearchResultDataSlice"
                                     ", offset_begin = " + std::to_string(offset_begin) +
                                     ", offset_end = " + std::to_string(offset_end));
    AssertInfo(offset_end <= topK * nq, "illegal offset_end when GetSearchResultDataSlice"
                                   ", offset_end = " + std::to_string(offset_end) +
                                   ", nq = " + std::to_string(nq) +
                                   ", topK = " + std::to_string(topK));

    // set ids
    auto proto_ids = std::make_unique<milvus::proto::schema::IDs>();
    auto ids = std::make_unique<milvus::proto::schema::LongArray>();
    *ids->mutable_data() = {result_ids.begin() + offset_begin, result_ids.begin() + offset_end};
    proto_ids->set_allocated_int_id(ids.release());
    search_result_data->set_allocated_ids(proto_ids.release());
    AssertInfo(search_result_data->ids().int_id().data_size() == offset_end - offset_begin, "wrong ids size"
                                                         ", size = " + std::to_string(search_result_data->ids().int_id().data_size()) +
                                                         ", expected size = " + std::to_string(offset_end - offset_begin));

    // set scores
    *search_result_data->mutable_scores() = {result_distances.begin() + offset_begin, result_distances.begin() + offset_end};
    AssertInfo(search_result_data->scores_size() == offset_end - offset_begin, "wrong scores size"
                                                         ", size = " + std::to_string(search_result_data->scores_size()) +
                                                         ", expected size = " + std::to_string(offset_end - offset_begin));

    // set output fields
    for (int i = 0; i < result_output_fields_data.size(); i++) {
        auto& field_meta = output_fields_meta[i];
        auto field_size = field_meta.get_sizeof();
        auto array = milvus::segcore::CreateDataArrayFrom(
            result_output_fields_data[i].data() + offset_begin * field_size,
            offset_end - offset_begin, field_meta);
        search_result_data->mutable_fields_data()->AddAllocated(array.release());
    }

    // SearchResultData to blob
    auto size = search_result_data->ByteSize();
    void* buffer = malloc(size);
    search_result_data->SerializePartialToArray(buffer, size);

    CProto proto;
    proto.proto_blob = buffer;
    proto.proto_size = size;
    return proto;
}

CStatus
Marshal(CSearchResultDataBlobs* cSearchResultDataBlobs,
        CSearchResult* c_search_results,
        int32_t num_segments,
        int32_t* nq_slice_sizes,
        int32_t num_slices) {
    try {
        // parse search results and get topK, nq
        std::vector<SearchResult*> search_results(num_segments);
        for (int i = 0; i < num_segments; ++i) {
            search_results[i] = static_cast<SearchResult*>(c_search_results[i]);
        }
        AssertInfo(search_results.size() > 0, "empty search result when Marshal");
        auto topK = search_results[0]->topk_;
        auto nq = search_results[0]->num_queries_;

        // init result ids, distances
        auto result_ids = milvus::aligned_vector<int64_t>(nq * topK);
        auto result_distances = std::vector<float>(nq * topK);

        // init result output fields data
        auto output_fields_meta = search_results[0]->output_fields_meta_;
        auto num_output_fields = output_fields_meta.size();
        auto result_output_fields_data = std::vector<milvus::aligned_vector<char>>(num_output_fields);
        for(int i = 0; i < num_output_fields; i++) {
            auto size = output_fields_meta[i].get_sizeof();
            result_output_fields_data[i].resize(size * nq * topK);
        }

        // Reorganize search results, get result ids, distances and output fields data
        ReorganizeSearchResultsV2(search_results, nq, topK, result_ids, result_distances, result_output_fields_data);

        // prefix sum, get slices offsets
        AssertInfo(num_slices > 0, "empty nq_slice_sizes is not allowed");
        auto slice_offsets_size = num_slices + 1;
        auto slice_offsets = std::vector<int32_t>(slice_offsets_size);
        slice_offsets[0] = 0;
        slice_offsets[1] = nq_slice_sizes[0];
        for (int i = 2; i < slice_offsets_size; i++) {
            slice_offsets[i] = slice_offsets[i - 1] + nq_slice_sizes[i - 1];
        }
        AssertInfo(slice_offsets[num_slices] == nq, "illegal req sizes"
                                                    ", slice_offsets[last] = " + std::to_string(slice_offsets[num_slices]) +
                                                    ", nq = " + std::to_string(nq));

        // get search result data blobs by slices
        auto blobs = std::make_unique<std::vector<CProto>>(num_slices);
#pragma omp parallel for
        for (int i = 0; i < num_slices; i++) {
            auto cProto = GetSearchResultDataSlice(result_ids,
                                                   result_distances,
                                                   result_output_fields_data,
                                                   nq, topK,
                                                   slice_offsets[i], slice_offsets[i + 1],
                                                   output_fields_meta);
            blobs->at(i) = cProto;
        }

        // set final result ptr
        cSearchResultDataBlobs->num_blobs = blobs->size();
        cSearchResultDataBlobs->blobs = &blobs.release()->data()[0];
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetSearchResultDataBlob(CProto* searchResultDataBlob,
                        CSearchResultDataBlobs* cSearchResultDataBlobs,
                        int32_t blob_index) {
    try {
        AssertInfo(blob_index < cSearchResultDataBlobs->num_blobs, "blob_index out of range");
        searchResultDataBlob->proto_blob = cSearchResultDataBlobs->blobs[blob_index].proto_blob;
        searchResultDataBlob->proto_size = cSearchResultDataBlobs->blobs[blob_index].proto_size;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

void
DeleteSearchResultDataBlobs(CSearchResultDataBlobs* cSearchResultDataBlobs) {
    if (cSearchResultDataBlobs == nullptr) {
        return;
    }
    for (int i = 0; i < cSearchResultDataBlobs->num_blobs; i++) {
        std::free(cSearchResultDataBlobs->blobs[i].proto_blob);
    }
    std::free(cSearchResultDataBlobs->blobs);
}

CStatus
ReduceSearchResultsAndFillData(CSearchPlan c_plan, CSearchResult* c_search_results, int64_t num_segments) {
    try {
        auto plan = (milvus::query::Plan*)c_plan;
        std::vector<SearchResult*> search_results;
        for (int i = 0; i < num_segments; ++i) {
            search_results.push_back((SearchResult*)c_search_results[i]);
        }
        auto topk = search_results[0]->topk_;
        auto num_queries = search_results[0]->num_queries_;

        // get primary keys for duplicates removal
        for (auto& search_result : search_results) {
            auto segment = (milvus::segcore::SegmentInterface*)(search_result->segment_);
            segment->FillPrimaryKeys(plan, *search_result);
        }

        ReduceResultData(search_results, num_queries, topk);

        // fill in other entities
        for (auto& search_result : search_results) {
            auto segment = (milvus::segcore::SegmentInterface*)(search_result->segment_);
            segment->FillTargetEntry(plan, *search_result);
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
ReorganizeSearchResults(CMarshaledHits* c_marshaled_hits, CSearchResult* c_search_results, int64_t num_segments) {
    try {
        auto marshaledHits = std::make_unique<MarshaledHits>(1);
        auto sr = (SearchResult*)c_search_results[0];
        auto topk = sr->topk_;
        auto num_queries = sr->num_queries_;

        std::vector<float> result_distances(num_queries * topk);
        std::vector<std::vector<char>> row_datas(num_queries * topk);

        std::vector<int64_t> counts(num_segments);
        for (int i = 0; i < num_segments; i++) {
            auto search_result = (SearchResult*)c_search_results[i];
            AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
            auto size = search_result->result_offsets_.size();
            if (size == 0) {
                continue;
            }
#pragma omp parallel for
            for (int j = 0; j < size; j++) {
                auto loc = search_result->result_offsets_[j];
                result_distances[loc] = search_result->distances_[j];
                row_datas[loc] = search_result->row_data_[j];
            }
            counts[i] = size;
        }

        int64_t total_count = 0;
        for (int i = 0; i < num_segments; i++) {
            total_count += counts[i];
        }
        AssertInfo(total_count == num_queries * topk, "the reduces result's size less than total_num_queries*topk");

        MarshaledHitsPerGroup& hits_per_group = (*marshaledHits).marshaled_hits_[0];
        hits_per_group.hits_.resize(num_queries);
        hits_per_group.blob_length_.resize(num_queries);
        std::vector<milvus::proto::milvus::Hits> hits(num_queries);
#pragma omp parallel for
        for (int m = 0; m < num_queries; m++) {
            for (int n = 0; n < topk; n++) {
                int64_t result_offset = m * topk + n;
                hits[m].add_scores(result_distances[result_offset]);
                auto& row_data = row_datas[result_offset];
                hits[m].add_row_data(row_data.data(), row_data.size());
                hits[m].add_ids(*(int64_t*)row_data.data());
            }
        }

#pragma omp parallel for
        for (int j = 0; j < num_queries; j++) {
            auto blob = hits[j].SerializeAsString();
            hits_per_group.hits_[j] = blob;
            hits_per_group.blob_length_[j] = blob.size();
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto marshaled_res = (CMarshaledHits)marshaledHits.release();
        *c_marshaled_hits = marshaled_res;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *c_marshaled_hits = nullptr;
        return status;
    }
}

int64_t
GetHitsBlobSize(CMarshaledHits c_marshaled_hits) {
    int64_t total_size = 0;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    for (int i = 0; i < num_group; i++) {
        auto& length_vector = marshaled_hits->marshaled_hits_[i].blob_length_;
        for (int j = 0; j < length_vector.size(); j++) {
            total_size += length_vector[j];
        }
    }
    return total_size;
}

void
GetHitsBlob(CMarshaledHits c_marshaled_hits, const void* hits) {
    auto byte_hits = (char*)hits;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    int offset = 0;
    for (int i = 0; i < num_group; i++) {
        auto& hits = marshaled_hits->marshaled_hits_[i];
        auto num_queries = hits.hits_.size();
        for (int j = 0; j < num_queries; j++) {
            auto blob_size = hits.blob_length_[j];
            memcpy(byte_hits + offset, hits.hits_[j].data(), blob_size);
            offset += blob_size;
        }
    }
}

int64_t
GetNumQueriesPerGroup(CMarshaledHits c_marshaled_hits, int64_t group_index) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& hits = marshaled_hits->marshaled_hits_[group_index].hits_;
    return hits.size();
}

void
GetHitSizePerQueries(CMarshaledHits c_marshaled_hits, int64_t group_index, int64_t* hit_size_peer_query) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& blob_lens = marshaled_hits->marshaled_hits_[group_index].blob_length_;
    for (int i = 0; i < blob_lens.size(); i++) {
        hit_size_peer_query[i] = blob_lens[i];
    }
}
