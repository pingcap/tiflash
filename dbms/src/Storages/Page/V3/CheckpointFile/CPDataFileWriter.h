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

#include <IO/Buffer/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/V3/CheckpointFile/Proto/data_file.pb.h>
#include <Storages/Page/V3/CheckpointFile/ProtoHelper.h>
#include <Storages/Page/V3/CheckpointFile/fwd.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <google/protobuf/util/json_util.h>

#include <magic_enum.hpp>
#include <string>

namespace DB::PS::V3
{

class CPDataFileWriter
{
public:
    struct Options
    {
        const std::string & file_path;
        const std::string & file_id;
    };

    static CPDataFileWriterPtr create(Options options) { return std::make_unique<CPDataFileWriter>(options); }

    explicit CPDataFileWriter(Options options)
        : file_writer(std::make_unique<WriteBufferFromFile>(options.file_path))
        , file_id(std::make_shared<std::string>(options.file_id))
    {
        // TODO: FramedChecksumWriteBuffer does not support random access for arbitrary frame sizes.
        //   So currently we use checksum = false.
        //   Need to update FramedChecksumWriteBuffer first.
        // TODO: Support compressed data file.
    }

    ~CPDataFileWriter() { flush(); }

    void writePrefix(const CheckpointProto::DataFilePrefix & prefix);

    CheckpointLocation write(UniversalPageId page_id, PageVersion version, const char * data, size_t n);

    void writeSuffix();

    void flush()
    {
        file_writer->next();
        file_writer->sync();
    }

    size_t writtenRecords() const { return file_suffix.records_size(); }

private:
    enum class WriteStage
    {
        WritingPrefix,
        WritingRecords,
        WritingFinished,
    };

    const std::unique_ptr<WriteBufferFromFile> file_writer;
    const std::shared_ptr<const std::string> file_id; // Shared in each write result

    CheckpointProto::DataFileSuffix file_suffix;
    WriteStage write_stage = WriteStage::WritingPrefix;
};

} // namespace DB::PS::V3
