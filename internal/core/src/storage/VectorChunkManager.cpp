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

#include "VectorChunkManager.h"
#include "RemoteChunkManagerSingleton.h"
#include "Exception.h"

#define THROWVECTORERROR(FUNCTION)                                 \
    do {                                                          \
        std::stringstream err_msg;                                \
        err_msg << "Error:" << #FUNCTION << ":" << err.message(); \
        throw VectorChunkManagerException(err_msg.str());          \
    } while (0)

namespace milvus::storage {

bool
VectorChunkManager::Exist(const std::string& filepath) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    return rcm->Exist(filepath);
}

uint64_t
VectorChunkManager::Size(const std::string& filepath) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    return rcm->Size(filepath);
}

void
VectorChunkManager::Remove(const std::string& filepath) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    rcm->Remove(filepath);
    if (cache_enabled_) {
        lru_cache_.remove(filepath);
    }
}

uint64_t
VectorChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    return Read(filepath, 0, buf, size);
}

std::shared_ptr<Column>
VectorChunkManager::Mmap(const std::string& filepath, const FieldDataPtr& field_data) {
    auto num_rows = field_data->get_num_rows();
    auto data_type = field_data->get_data_type();

    int fd =
            open(filepath.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    AssertInfo(fd != -1,
               fmt::format("failed to create mmap file {}", filepath.c_str()));

    // write the field data to disk
    size_t total_written{0};
    auto data_size = 0;
    std::vector<uint64_t> indices{};

    data_size += field_data->Size();
    auto written = WriteFieldData(fd, data_type, field_data);
    AssertInfo(
            written == data_size,
            fmt::format(
                    "failed to write data file {}, written {} but total {}, err: {}",
                    filepath.c_str(),
                    total_written,
                    data_size,
                    strerror(errno)));
    auto ok = fsync(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to fsync mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    auto column = std::make_shared<Column>(fd, total_written, num_rows, data_type);
    column->Append(static_cast<const char*>(field_data->Data()),
                   field_data->Size());
    return column;
}

void
GetVectorFromData(DataType data_type, const void* data, uint64_t offset, void* buf, uint64_t size) {
    auto span = column->Span();
    std::memcpy(buf, span.data(), size);
}

uint64_t
VectorChunkManager::Read(const std::string& filepath,
                        uint64_t offset,
                        void* buf,
                        uint64_t size) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    if (!cache_enabled_) {
        auto object_data = GetObjectData(rcm.get(), std::vector<std::string>{filepath});
        AssertInfo(object_data.size() == 1, "[VectorChunkManager] GetObjectData failed");
        auto field_data = object_data[0];
        auto data_type = field_data->get_data_type();
        AssertInfo(datatype_is_vector(data_type), "[VectorChunkManager] Data type is not vector");
        std::memcpy(buf, field_data->Data(), size);
    }
    std::shared_ptr<Column> column{};
    auto ok = lru_cache_.try_get(filepath, column);
    if (ok) {
        std::memcpy(buf, column->Span().data(), size);
        return size;
    }
    auto object_data = GetObjectData(rcm.get(), std::vector<std::string>{filepath});
    AssertInfo(object_data.size() == 1, "[VectorChunkManager] GetObjectData failed");
    auto field_data = object_data[0];

    column = Mmap(filepath, field_data);
    lru_cache_.put(filepath, column);
    std::memcpy(buf, column->Span().data(), size);
    return size;
}

void
VectorChunkManager::Write(const std::string& absPathStr,
                         void* buf,
                         uint64_t size) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    rcm->Write(absPathStr, buf, size);
}

void
VectorChunkManager::Write(const std::string& absPathStr,
                         uint64_t offset,
                         void* buf,
                         uint64_t size) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    rcm->Write(absPathStr, offset, buf, size);
}

std::vector<std::string>
VectorChunkManager::ListWithPrefix(const std::string& filepath) {
    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    return rcm->ListWithPrefix(filepath);
}

}  // namespace milvus::storage
