// Copyright 2023 PingCAP, Inc.
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

#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <Storages/Page/V3/CheckpointFile/fwd.h>
#include <Storages/Page/V3/PageEntriesEdit.h>


namespace DB::PS::V3
{

class CPManifestFileReader : private boost::noncopyable
{
public:
    struct Options
    {
        RandomAccessFilePtr plain_file;
    };

    static CPManifestFileReaderPtr create(Options options)
    {
        return std::make_unique<CPManifestFileReader>(std::move(options));
    }

    explicit CPManifestFileReader(Options options)
        : file_reader(std::make_unique<ReadBufferFromRandomAccessFile>(options.plain_file))
        , compressed_reader(std::make_unique<CompressedReadBuffer<true>>(*file_reader))
    {}

    CheckpointProto::ManifestFilePrefix readPrefix();

    /// You should call this function multiple times to read out all edits, until it returns nullopt.
    std::optional<universal::PageEntriesEdit> readEdits(CheckpointProto::StringsInternMap & strings_map);

    /// You should call this function multiple times to read out all locks, until it returns nullopt.
    std::optional<std::unordered_set<String>> readLocks();

private:
    enum class ReadStage
    {
        ReadingPrefix,
        ReadingEdits,
        ReadingEditsFinished,
        ReadingLocks,
        ReadingLocksFinished,
    };

    // compressed<plain_file>
    const ReadBufferFromRandomAccessFilePtr file_reader;
    const ReadBufferPtr compressed_reader;

    ReadStage read_stage = ReadStage::ReadingPrefix;
};

} // namespace DB::PS::V3
