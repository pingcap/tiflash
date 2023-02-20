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

#include <IO/IOSWrapper.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/Remote/Proto/Helper.h>
#include <Storages/Page/V3/Remote/Proto/data_file.pb.h>
#include <google/protobuf/util/json_util.h>

#include <string>

namespace DB::PS::V3
{

template <typename PSDirTrait>
class CheckpointDataFileWriter;

template <typename PSDirTrait>
using CheckpointDataFileWriterPtr = std::unique_ptr<CheckpointDataFileWriter<PSDirTrait>>;

template <typename PSDirTrait>
class CheckpointDataFileWriter
{
public:
    struct Options
    {
        const std::string & file_path;
        const std::string & file_id;
        // WriteLimiterPtr write_limiter;

        // TODO: When adding split file support, create a new class over this one.
    };

    static CheckpointDataFileWriterPtr<PSDirTrait> create(Options options)
    {
        return std::make_unique<CheckpointDataFileWriter>(options);
    }

    explicit CheckpointDataFileWriter(Options options)
        : file_writer(std::make_unique<WriteBufferFromFile>(options.file_path))
        , file_id(std::make_shared<std::string>(options.file_id))
    {
        // TODO: FramedChecksumWriteBuffer does not support random access for arbitrary frame sizes.
        //   So currently we use checksum = false.
        //   Need to update FramedChecksumWriteBuffer first.
        // TODO: Support compressed data file.
    }

    void writePrefix(const Remote::DataFilePrefix & prefix)
    {
        std::string json;
        google::protobuf::util::MessageToJsonString(prefix, &json);
        writeStringBinary(json, *file_writer);
    }

    void writeSuffix()
    {
        std::string json;
        google::protobuf::util::MessageToJsonString(file_suffix, &json);
        writeStringBinary(json, *file_writer);
    }

    RemoteDataLocation write(typename PSDirTrait::PageId page_id, PageVersion version, const char * data, size_t n)
    {
        // Every record is prefixed with the length, so that this data file can be parsed standalone.
        writeIntBinary(n, *file_writer);

        // TODO: getMaterializedBytes only works for FramedChecksumWriteBuffer, but does not work for a normal WriteBufferFromFile.
        //       There must be something wrong and should be fixed.
        // uint64_t file_offset = file_writer->getMaterializedBytes();
        // file_writer->write(data, n);
        // uint64_t write_n = file_writer->getMaterializedBytes() - file_offset;

        uint64_t file_offset = file_writer->count();
        file_writer->write(data, n);
        uint64_t write_n = file_writer->count() - file_offset;

        auto * suffix_record = file_suffix.add_records();
        suffix_record->mutable_page_id()->CopyFrom(Remote::toRemote(page_id));
        suffix_record->set_version_sequence(version.sequence);
        suffix_record->set_version_epoch(version.epoch);
        suffix_record->set_offset_in_file(file_offset);
        suffix_record->set_size_in_file(write_n);

        return RemoteDataLocation{
            .data_file_id = file_id,
            .offset_in_file = file_offset,
            .size_in_file = write_n,
        };
    }

    void flush()
    {
        file_writer->next();
        file_writer->sync();
    }

    size_t writtenRecords() const
    {
        return file_suffix.records_size();
    }

private:
    const std::unique_ptr<WriteBufferFromFile> file_writer;
    Remote::DataFileSuffix file_suffix;

    // File ID is shared in each write result
    const std::shared_ptr<const std::string> file_id;
};

} // namespace DB::PS::V3
