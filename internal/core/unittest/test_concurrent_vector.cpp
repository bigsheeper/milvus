// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "segcore/ConcurrentVector.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/AckResponder.h"

using namespace milvus::segcore;
using std::vector;

TEST(ConcurrentVector, TestSingle) {
    auto dim = 8;
    ConcurrentVectorImpl<int, false> c_vec(dim, 32);
    std::default_random_engine e(42);
    int data = 0;
    auto total_count = 0;
    for (int i = 0; i < 10000; ++i) {
        int insert_size = e() % 150;
        vector<int> vec(insert_size * dim);
        for (auto& x : vec) {
            x = data++;
        }
        c_vec.grow_to_at_least(total_count + insert_size);
        c_vec.set_data(total_count, vec.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(c_vec.num_chunk(), (total_count + 31) / 32);
    for (int i = 0; i < total_count; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto std_data = d + i * dim;
            ASSERT_EQ(c_vec.get_element(i)[d], std_data);
        }
    }
}

TEST(ConcurrentVector, TestMultithreads) {
    auto dim = 8;
    constexpr int threads = 16;
    std::vector<int64_t> total_counts(threads);

    ConcurrentVectorImpl<int64_t, false> c_vec(dim, 32);
    std::atomic<int64_t> ack_counter = 0;

    auto executor = [&](int thread_id) {
        std::default_random_engine e(42 + thread_id);
        int64_t data = 0;
        int64_t total_count = 0;
        for (int i = 0; i < 2000; ++i) {
            int insert_size = e() % 150;
            vector<int64_t> vec(insert_size * dim);
            for (auto& x : vec) {
                x = data++ * threads + thread_id;
            }
            auto offset = ack_counter.fetch_add(insert_size);
            c_vec.grow_to_at_least(offset + insert_size);
            c_vec.set_data(offset, vec.data(), insert_size);
            total_count += insert_size;
        }
        assert(data == total_count * dim);
        total_counts[thread_id] = total_count;
    };
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) {
        pool.emplace_back(executor, i);
    }
    for (auto& thread : pool) {
        thread.join();
    }

    std::vector<int64_t> counts(threads);
    auto N = ack_counter.load();
    for (int64_t i = 0; i < N; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto data = c_vec.get_element(i)[d];
            auto thread_id = data % threads;
            auto raw_data = data / threads;
            auto std_data = counts[thread_id]++;
            ASSERT_EQ(raw_data, std_data) << data;
        }
    }
}

TEST(ConcurrentVector, TestAckSingle) {
    std::vector<std::tuple<int64_t, int64_t, int64_t>> raw_data;
    std::default_random_engine e(42);
    AckResponder ack;
    int N = 10000;
    for (int i = 0; i < 10000; ++i) {
        auto weight = i + e() % 100;
        raw_data.emplace_back(weight, i, (i + 1));
    }
    std::sort(raw_data.begin(), raw_data.end());
    for (auto [_, b, e] : raw_data) {
        EXPECT_LE(ack.GetAck(), b);
        ack.AddSegment(b, e);
        auto seg = ack.GetAck();
        EXPECT_GE(seg + 100, b);
    }
    EXPECT_EQ(ack.GetAck(), N);
}

TEST(ConcurrentVector, TestPermutation) {
    std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< scalar <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
    ConcurrentVectorImpl<int, true> ids(1, 32);
    ConcurrentVectorImpl<int, true> unordered_timestamp(1, 32);
    std::map<int, int> records;
    std::default_random_engine e(42);
    auto total_count = 0;
    int data = 0;
    for (int i = 0; i < 100; ++i) {
        int insert_size = e() % 20000;
        vector<int> vec;
        vector<int> timestamps;
        while (vec.size() < insert_size) {
            int v = e() % 10000000;
            int t = e() % 100000000;
//            int v = data;
//            int t = data;
            data++;
            if (!records.count(t)) {
                records.insert(std::pair<int, int>(t, v));
                vec.emplace_back(v);
                timestamps.emplace_back(t);
            }
        }
        ASSERT_EQ(vec.size(), insert_size);
        ids.grow_to_at_least(total_count + insert_size);
        ids.set_data(total_count, vec.data(), insert_size);
        unordered_timestamp.grow_to_at_least(total_count + insert_size);
        unordered_timestamp.set_data(total_count, timestamps.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(unordered_timestamp.num_chunk(), (total_count + 31) / 32);

    auto start = std::chrono::high_resolution_clock::now();

    auto timestamp_vec = unordered_timestamp.to_vector(total_count);
    auto t_to_vec = std::chrono::high_resolution_clock::now();
    auto d_to_vec = std::chrono::duration_cast<std::chrono::milliseconds>(t_to_vec - start);
    std::cout << "to_vector taken: " << d_to_vec.count() << " ms" << ", total_insert: " << total_count << std::endl;

    ASSERT_EQ(timestamp_vec.size(), total_count);
    auto permutation = unordered_timestamp.sort_permutation(timestamp_vec);
    auto t_sort_permutation = std::chrono::high_resolution_clock::now();
    auto d_sort_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_sort_permutation - t_to_vec);
    std::cout << "sort_permutation taken: " << d_sort_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    ids.apply_permutation(permutation);
    unordered_timestamp.apply_permutation(permutation);
//    auto vv = unordered_timestamp.to_vector(total_count);
    auto t_apply_permutation = std::chrono::high_resolution_clock::now();
    auto d_apply_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_apply_permutation - t_sort_permutation);
    std::cout << "apply_permutation taken: " << d_apply_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Total Time taken: " << duration.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    // check
    auto pre = unordered_timestamp[0];
    ASSERT_TRUE(records.count(unordered_timestamp[0]));
    ASSERT_EQ(records.at(unordered_timestamp[0]), ids[0]);
    for (int i = 1; i < timestamp_vec.size(); ++i) {
        ASSERT_TRUE(unordered_timestamp[i]>pre);
        ASSERT_TRUE(records.count(unordered_timestamp[i]));
        ASSERT_EQ(records.at(unordered_timestamp[i]), ids[i]);
        pre = unordered_timestamp[i];
    }
}

TEST(ConcurrentVector, TestPermutation_vector) {
    auto dim = 128;
    std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< vector " << dim << " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
    ConcurrentVectorImpl<int, false> vecs(dim, 32);
    ConcurrentVectorImpl<int, true> unordered_timestamp(1, 32);
    std::map<int, int> records;
    std::default_random_engine e(42);
    auto total_count = 0;
    for (int i = 0; i < 100; ++i) {
        int insert_size = e() % 20000;
        vector<int> vec;
        vector<int> timestamps;
        while (timestamps.size() < insert_size) {
            int t = e() % 100000000;
            if (!records.count(t)) {
                auto tmp = 0;
                for (int j = 0; j < dim; j++) {
                    int v = e() % 10000000;
                    vec.emplace_back(v);
                    tmp+=v;
                }
                records.insert(std::pair<int, int>(t, tmp));
                timestamps.emplace_back(t);
            }
        }
        ASSERT_EQ(vec.size(), insert_size*dim);
        vecs.grow_to_at_least(total_count + insert_size);
        vecs.set_data(total_count, vec.data(), insert_size);
        unordered_timestamp.grow_to_at_least(total_count + insert_size);
        unordered_timestamp.set_data(total_count, timestamps.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(unordered_timestamp.num_chunk(), (total_count + 31) / 32);

    auto start = std::chrono::high_resolution_clock::now();

    auto timestamp_vec = unordered_timestamp.to_vector(total_count);
    auto t_to_vec = std::chrono::high_resolution_clock::now();
    auto d_to_vec = std::chrono::duration_cast<std::chrono::milliseconds>(t_to_vec - start);
    std::cout << "to_vector taken: " << d_to_vec.count() << " ms" << ", total_insert: " << total_count << std::endl;

    ASSERT_EQ(timestamp_vec.size(), total_count);
    auto permutation = unordered_timestamp.sort_permutation(timestamp_vec);
    auto t_sort_permutation = std::chrono::high_resolution_clock::now();
    auto d_sort_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_sort_permutation - t_to_vec);
    std::cout << "sort_permutation taken: " << d_sort_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

//    auto before = vecs.to_vector(total_count);
    vecs.apply_permutation_to_vec_field(permutation);
//    auto after = vecs.to_vector(total_count);
    unordered_timestamp.apply_permutation(permutation);
//    auto vv = unordered_timestamp.to_vector(total_count);
    auto t_apply_permutation = std::chrono::high_resolution_clock::now();
    auto d_apply_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_apply_permutation - t_sort_permutation);
    std::cout << "apply_permutation taken: " << d_apply_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Total Time taken: " << duration.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    // check
    auto pre = unordered_timestamp[0];
    ASSERT_TRUE(records.count(unordered_timestamp[0]));

    std::vector<int> vecSum;
    for (int i = 0; i < total_count; i++) {
        int tmp_sum = 0;
        auto ptr = &vecs[i];
        for (int j = 0; j < dim; j++) {
            tmp_sum += *ptr;
            ptr++;
        }
        vecSum.emplace_back(tmp_sum);
    }

    ASSERT_EQ(records.at(unordered_timestamp[0]), vecSum[0]);
    for (int i = 1; i < timestamp_vec.size(); ++i) {
        ASSERT_TRUE(unordered_timestamp[i]>pre);
        ASSERT_TRUE(records.count(unordered_timestamp[i]));
        ASSERT_EQ(records.at(unordered_timestamp[i]), vecSum[i]);
        pre = unordered_timestamp[i];
    }
}

TEST(ConcurrentVector, TestPermutation_string) {
    std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< string <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
    ConcurrentVectorImpl<std::string, true> strings(1, 32);
    ConcurrentVectorImpl<int, true> unordered_timestamp(1, 32);
    std::map<int, std::string> records;
    std::default_random_engine e(42);
    auto total_count = 0;
    for (int i = 0; i < 100; ++i) {
        int insert_size = e() % 20000;
        vector<std::string> vec;
        vector<int> timestamps;
        while (timestamps.size() < insert_size) {
            int t = e() % 100000000;
            int v = e() % 10000000;
            if (!records.count(t)) {
                records.insert(std::pair<int, std::string>(t, std::to_string(v)));
                vec.emplace_back(std::to_string(v));
                timestamps.emplace_back(t);
            }
        }
        ASSERT_EQ(vec.size(), insert_size);
        strings.grow_to_at_least(total_count + insert_size);
        strings.set_data(total_count, vec.data(), insert_size);
        unordered_timestamp.grow_to_at_least(total_count + insert_size);
        unordered_timestamp.set_data(total_count, timestamps.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(unordered_timestamp.num_chunk(), (total_count + 31) / 32);

    auto start = std::chrono::high_resolution_clock::now();

    auto timestamp_vec = unordered_timestamp.to_vector(total_count);
    auto t_to_vec = std::chrono::high_resolution_clock::now();
    auto d_to_vec = std::chrono::duration_cast<std::chrono::milliseconds>(t_to_vec - start);
    std::cout << "to_vector taken: " << d_to_vec.count() << " ms" << ", total_insert: " << total_count << std::endl;

    ASSERT_EQ(timestamp_vec.size(), total_count);
    auto permutation = unordered_timestamp.sort_permutation(timestamp_vec);
    auto t_sort_permutation = std::chrono::high_resolution_clock::now();
    auto d_sort_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_sort_permutation - t_to_vec);
    std::cout << "sort_permutation taken: " << d_sort_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

//    auto before = strings.to_vector(total_count);
    strings.apply_permutation(permutation);
//    auto after = strings.to_vector(total_count);
    unordered_timestamp.apply_permutation(permutation);
//    auto vv = unordered_timestamp.to_vector(total_count);
    auto t_apply_permutation = std::chrono::high_resolution_clock::now();
    auto d_apply_permutation = std::chrono::duration_cast<std::chrono::microseconds>(t_apply_permutation - t_sort_permutation);
    std::cout << "apply_permutation taken: " << d_apply_permutation.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Total Time taken: " << duration.count()/1000 << " ms" << ", total_insert: " << total_count << std::endl;

    // check
    auto pre = unordered_timestamp[0];
    ASSERT_TRUE(records.count(unordered_timestamp[0]));

    ASSERT_EQ(records.at(unordered_timestamp[0]), strings[0]);
    for (int i = 1; i < timestamp_vec.size(); ++i) {
        ASSERT_TRUE(unordered_timestamp[i]>pre);
        ASSERT_TRUE(records.count(unordered_timestamp[i]));
        ASSERT_EQ(records.at(unordered_timestamp[i]), strings[i]);
        pre = unordered_timestamp[i];
    }
}
