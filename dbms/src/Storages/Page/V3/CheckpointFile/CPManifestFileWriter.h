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

#include <Common/Exception.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <Storages/Page/V3/CheckpointFile/fwd.h>
#include <Storages/Page/V3/PageEntriesEdit.h>

#include <string>

namespace DB::PS::V3
{

class CPManifestFileWriter : private boost::noncopyable
{
public:
    struct Options
    {
        const std::string & file_path;
        UInt64 max_edit_records_per_part = 100000;
    };

    static CPManifestFileWriterPtr create(Options options)
    {
        return std::make_unique<CPManifestFileWriter>(std::move(options));
    }

    explicit CPManifestFileWriter(Options options_)
        : options(options_)
        , file_writer(std::make_unique<WriteBufferFromFile>(options.file_path))
        , compressed_writer(std::make_unique<CompressedWriteBuffer<true>>(*file_writer, CompressionSettings()))
        , max_edit_records_per_part(options.max_edit_records_per_part)
    {
        RUNTIME_CHECK(max_edit_records_per_part > 0, max_edit_records_per_part);
    }

    ~CPManifestFileWriter() { flush(); }

    /// Must be called first.
    void writePrefix(const CheckpointProto::ManifestFilePrefix & prefix);

    /// You can call this function multiple times. It must be called after `writePrefix`.
    void writeEdits(const universal::PageEntriesEdit & edit);
    void writeEditsFinish();

    /// You can call this function multiple times. It must be called after `writeEdits`.
    void writeLocks(const std::unordered_set<String> & lock_files);
    void writeLocksFinish();

    void writeSuffix();

    void flush();

    void abort();

private:
    void writeEditsPart(const universal::PageEntriesEdit & edit, UInt64 start, UInt64 limit);

    enum class WriteStage
    {
        WritingPrefix = 0,
        WritingEdits,
        WritingEditsFinished,
        WritingLocks,
        WritingLocksFinished,
        WritingFinished,
    };

    Options options;
    // compressed<plain_file>
    const std::unique_ptr<WriteBufferFromFile> file_writer;
    const WriteBufferPtr compressed_writer;
    const UInt64 max_edit_records_per_part;

    WriteStage write_stage = WriteStage::WritingPrefix;
};

} // namespace DB::PS::V3
