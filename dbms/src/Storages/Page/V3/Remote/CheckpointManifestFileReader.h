// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/Remote/Proto/manifest_file.pb.h>
#include <google/protobuf/util/json_util.h>

#include <memory>
#include <string>
#include <unordered_set>

namespace DB::PS::V3
{

template <typename PSDirTrait>
class CheckpointManifestFileReader;

template <typename PSDirTrait>
using CheckpointManifestFileReaderPtr = std::unique_ptr<CheckpointManifestFileReader<PSDirTrait>>;

template <typename PSDirTrait>
class CheckpointManifestFileReader
{
public:
    struct Options
    {
        const std::string & file_path;
    };

    static CheckpointManifestFileReaderPtr<PSDirTrait> create(Options options)
    {
        return std::make_unique<CheckpointManifestFileReader>(std::move(options));
    }

    explicit CheckpointManifestFileReader(Options options)
        : file_reader(std::make_unique<ReadBufferFromFile>(options.file_path))
    {}

    // Note: Currently you cannot call read multiple times.
    // And this function will just read out everything in the manifest,
    // consuming a lot of memory. It needs to be improved.
    typename PSDirTrait::PageEntriesEdit read()
    {
        RUNTIME_CHECK(!has_read);
        has_read = true;

        std::string json;
        readStringBinary(json, *file_reader, 512 * 1024 * 1024 /* 512MB */);

        Remote::ManifestFile file_content;
        google::protobuf::util::JsonStringToMessage(json, &file_content);

        typename PSDirTrait::PageEntriesEdit edit;
        auto & records = edit.getMutRecords();
        for (const auto & remote_rec : file_content.records())
        {
            records.emplace_back(PSDirTrait::EditRecord::fromRemote(remote_rec));
        }

        return edit;
    }

    std::unordered_set<String> readLocks()
    {
        RUNTIME_CHECK(!has_read);
        has_read = true;

        String json;
        readStringBinary(json, *file_reader, 512 * 1024 * 1024 /* 512MB */);
        Remote::ManifestFile file_content;
        google::protobuf::util::JsonStringToMessage(json, &file_content);

        std::unordered_set<String> locks;
        locks.reserve(file_content.locks_size());
        for (const auto & lock : file_content.locks())
            locks.emplace(lock.name());
        return locks;
    }

private:
    const std::unique_ptr<ReadBufferFromFile> file_reader;

    bool has_read = false;
};

} // namespace DB::PS::V3
