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

#pragma once

#include "storage/ChunkManager.h"
#include "common/lru_cache.h"
#include "mmap/Column.h"

namespace milvus::storage {

class VectorChunkManager : public ChunkManager {
public:
    explicit VectorChunkManager(std::string path, const bool cache_enabled, const size_t cache_size)
    :path_prefix_(std::move(path)), cache_enabled_(cache_enabled), cache_size_(cache_size) {
//        lru_cache_ // TODO
    }

public:
    ~VectorChunkManager() = default;

    bool
    Exist(const std::string& filepath) override;

    uint64_t
    Size(const std::string& filepath) override;

    uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len) override;

    void
    Write(const std::string& filepath, void* buf, uint64_t len) override;

    uint64_t
    Read(const std::string& filepath, uint64_t offset, void* buf, uint64_t len) override;

    void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) override;

    std::vector<std::string>
    ListWithPrefix(const std::string& filepath) override;

    void
    Remove(const std::string& filepath) override;

    std::string
    GetName() const override {
        return "VectorChunkManager";
    }

    std::string
    GetRootPath() const override {
        return path_prefix_;
    }

private:
    std::shared_ptr<Column>
    Mmap(const std::string& filepath, const FieldDataPtr& field_data);

private:
    std::string path_prefix_;
    bool cache_enabled_;
    size_t cache_size_;
    lru_cache<std::string, std::shared_ptr<Column>> lru_cache_;
};

using VectorChunkManagerSPtr =
        std::shared_ptr<milvus::storage::VectorChunkManager>;

}  // namespace milvus::storage
