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

#include "ChunkCache.h"
#include "Exception.h"
#include "RemoteChunkManagerSingleton.h"

namespace milvus::storage {

std::shared_ptr<Column>
ChunkCache::Read(const std::string& filepath) {
//    if (columns_.find(filepath) != columns_.end()) {
//        return columns_.at(filepath);
//    }
    ColumnTable::const_accessor ca;
    if (columns_.find(ca, filepath)) {
        return ca->second;
    }

    auto rcm = RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    auto object_data = GetObjectData(rcm.get(), std::vector<std::string>{filepath});
    AssertInfo(object_data.size() == 1, "[ChunkCache] GetObjectData failed");
    auto field_data = object_data[0];

    auto column = Mmap(filepath, field_data);
//    columns_.insert(filepath, column);
    columns_.emplace(filepath, column);
    return column;
}

void
ChunkCache::Remove(const std::string& filepath) {
    columns_.erase(filepath);
}

void
ChunkCache::Prefetch(const std::string &filepath) {
//    if (columns_.find(filepath) == columns_.end()) {
//        return;
//    }
//    auto column = columns_.at(filepath);
    ColumnTable::const_accessor ca;
    if (!columns_.find(ca, filepath)) {
        return;
    }
    auto column = ca->second;
    auto ok = madvise(reinterpret_cast<void *>(const_cast<char *>(column->Data())), column->Size(), MADV_RANDOM);
    AssertInfo(ok == 0,
               fmt::format("[ChunkCache] failed to madvise to the data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
}

std::shared_ptr<Column>
ChunkCache::Mmap(const std::string& filepath, const FieldDataPtr& field_data) {
    auto num_rows = field_data->get_num_rows();
    auto data_type = field_data->get_data_type();

    int fd =
            open(filepath.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    AssertInfo(fd != -1,
               fmt::format("[ChunkCache] failed to create mmap file {}", filepath.c_str()));

    // write the field data to disk
    size_t total_written{0};
    auto data_size = 0;

    data_size += field_data->Size();
    auto written = WriteFieldData(fd, data_type, field_data);
    AssertInfo(
            written == data_size,
            fmt::format(
                    "[ChunkCache] failed to write data file {}, written {} but total {}, err: {}",
                    filepath.c_str(),
                    total_written,
                    data_size,
                    strerror(errno)));
    auto ok = fsync(fd);
    AssertInfo(ok == 0,
               fmt::format("[ChunkCache] failed to fsync mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    auto column = std::make_shared<Column>(fd, total_written, num_rows, data_type);
    column->Append(static_cast<const char*>(field_data->Data()),
                   field_data->Size());

    // unlink and close
    ok = unlink(filepath.c_str());
    AssertInfo(ok == 0,
               fmt::format("[ChunkCache] failed to unlink mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
    ok = close(fd);
    AssertInfo(ok == 0,
               fmt::format("[ChunkCache] failed to close data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    return column;
}

}  // namespace milvus::storage
