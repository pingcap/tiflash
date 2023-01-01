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

#include <IO/ChecksumBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/IOSWrapper.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromOStream.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/Remote/Proto/manifest_file.pb.h>

#include <memory>
#include <string>

namespace DB::PS::V3
{

template <typename PSDirTrait>
class CheckpointManifestFileWriter;

template <typename PSDirTrait>
using CheckpointManifestFileWriterPtr = std::unique_ptr<CheckpointManifestFileWriter<PSDirTrait>>;

template <typename PSDirTrait>
class CheckpointManifestFileWriter
{
public:
    struct Options
    {
        const std::string & file_path;
        const std::string & file_id;
    };

    static CheckpointManifestFileWriterPtr<PSDirTrait> create(Options options)
    {
        return std::make_unique<CheckpointManifestFileWriter>(std::move(options));
    }

    explicit CheckpointManifestFileWriter(Options options)
        : file_writer(std::make_unique<WriteBufferFromFile>(options.file_path))
        //        , compressed_writer(std::unique_ptr<WriteBuffer>(new CompressedWriteBuffer<true>(
        //              *file_writer,
        //              CompressionSettings())))
        , file_id(options.file_id)
    {}

    // Note: Currently you cannot call write multiple times.
    // You must feed everything you want to write all at once.
    // But we definitely want to change it to some streaming thing.
    void write(const Remote::ManifestFilePrefix & prefix, const typename PSDirTrait::PageEntriesEdit & edit)
    {
        RUNTIME_CHECK(!has_written);
        has_written = true;

        Remote::ManifestFile file_content;
        file_content.mutable_file_prefix()->CopyFrom(prefix);

        for (const auto & edit_record : edit.getRecords())
        {
            auto * out_record = file_content.add_records();
            *out_record = edit_record.toRemote();
        }

        // To be changed: Currently we write out JSON instead of a binary data,
        // for easier debugging.

        // OutputStreamWrapper ostream{*compressed_writer};
        // file_content.SerializeToOstream(&ostream);

        std::string json;
        google::protobuf::util::MessageToJsonString(file_content, &json);
        writeStringBinary(json, *file_writer);
    }

    void flush()
    {
        //        compressed_writer->next();
        file_writer->next();
        file_writer->sync();
    }

    const std::string & getFileId() const
    {
        return file_id;
    }

private:
    // compressed_buf -> plain_file
    const std::unique_ptr<WriteBufferFromFile> file_writer;
    //    WriteBufferPtr compressed_writer;

    const std::string file_id;

    bool has_written = false;
};

} // namespace DB::PS::V3
