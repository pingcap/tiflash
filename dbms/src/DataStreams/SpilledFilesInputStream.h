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

#include <Core/Spiller.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>

namespace DB
{
struct SpilledFileInfo
{
    String path;
    std::unique_ptr<SpilledFile> file;
    explicit SpilledFileInfo(const String path_)
        : path(path_)
    {}
};

class SpilledFilesInputStream : public IProfilingBlockInputStream
{
public:
    SpilledFilesInputStream(
        std::vector<SpilledFileInfo> && spilled_file_infos,
        const Block & header,
        const Block & header_without_constants,
        const std::vector<size_t> & const_column_indexes,
        const FileProviderPtr & file_provider,
        Int64 max_supported_spill_version);
    Block getHeader() const override;
    String getName() const override;

protected:
    Block readImpl() override;
    Block readInternal();

private:
    struct SpilledFileStream
    {
        SpilledFileInfo spilled_file_info;
        ReadBufferFromRandomAccessFile file_in;
        CompressedReadBuffer<> compressed_in;
        BlockInputStreamPtr block_in;

        SpilledFileStream(
            SpilledFileInfo && spilled_file_info_,
            const Block & header,
            const FileProviderPtr & file_provider,
            Int64 max_supported_spill_version)
            : spilled_file_info(std::move(spilled_file_info_))
            , file_in(ReadBufferFromRandomAccessFileBuilder::build(
                  file_provider,
                  spilled_file_info.path,
                  EncryptionPath(spilled_file_info.path, "")))
            , compressed_in(file_in)
        {
            Int64 file_spill_version = 0;
            readVarInt(file_spill_version, compressed_in);
            if (file_spill_version > max_supported_spill_version)
                throw Exception(fmt::format(
                    "Spiller meet spill files that is not supported, max supported version {}, file version {}",
                    max_supported_spill_version,
                    file_spill_version));
            block_in = std::make_shared<NativeBlockInputStream>(compressed_in, header, file_spill_version);
        }
    };

    std::vector<SpilledFileInfo> spilled_file_infos;
    size_t current_reading_file_index;
    Block header;
    Block header_without_constants;
    std::vector<size_t> const_column_indexes;
    FileProviderPtr file_provider;
    Int64 max_supported_spill_version;
    std::unique_ptr<SpilledFileStream> current_file_stream;
};

} // namespace DB
