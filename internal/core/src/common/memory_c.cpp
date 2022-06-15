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

#ifdef __linux__
#include <malloc.h>
#include <rapidxml/rapidxml.hpp>
#endif

#include <iostream>
#include "common/CGoHelper.h"
#include "common/memory_c.h"
#include "log/Log.h"

void
DoMallocTrim() {
#ifdef __linux__
    malloc_trim(0);
#endif
}

uint64_t
ParseMallocInfo() {
#ifdef __linux__
    char* mem_buffer;
    size_t buffer_size;
    FILE* stream;
    stream = open_memstream(&mem_buffer, &buffer_size);
    //    malloc_info(0, stdout);
    malloc_info(0, stream);
    fflush(stream);

    rapidxml::xml_document<> doc;  // character type defaults to char
    doc.parse<0>(mem_buffer);      // 0 means default parse flags

    uint64_t fast_and_rest_total = 0;

    rapidxml::xml_node<>* malloc_root_node = doc.first_node();
    //    std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> malloc_root_node: " << malloc_root_node->name() << "\n";
    for (auto heap_node = malloc_root_node->first_node("heap"); heap_node;
         heap_node = heap_node->next_sibling("heap")) {
        //        auto heap_id = std::atoi(heap_node->first_attribute("nr")->value());
        //        std::cout << heap_node->name() << heap_id << " ";
        for (auto total_node = heap_node->first_node("total"); total_node;
             total_node = total_node->next_sibling("total")) {
            auto size = std::stoll(total_node->first_attribute("size")->value());
            //            std::cout << total_node->first_attribute("type")->value() << " " << size << " ";
            fast_and_rest_total += size;
        }
        //        std::cout << std::endl;
    }

    //    std::cout << "fast_and_rest_total " << fast_and_rest_total << " ";
    //    std::cout << std::endl;

    fclose(stream);
    free(mem_buffer);

    return fast_and_rest_total;
#else
    return 0;  // malloc_trim is unnecessary
#endif
}

CStatus
PurgeMemory(uint64_t max_bins_size) {
    try {
        auto fast_and_rest_total = ParseMallocInfo();
        if (fast_and_rest_total >= max_bins_size) {
            LOG_SEGCORE_DEBUG_ << "Purge memory fragmentation, max_bins_size(bytes) = " << max_bins_size
                               << ", fast_and_rest_total(bytes) = " << fast_and_rest_total;
            DoMallocTrim();
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
