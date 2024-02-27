// Copyright 2024 PingCAP, Inc.
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

#include <DataStreams/MarkInCompressedFile.h>
#include <IO/FileProvider/CompressedReadBufferFromFileBuilder.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <common/types.h>

#include <map>
#include <memory>

namespace DB::DM
{
class DMFileReader;

// The stream for reading one column data or its substream (nullmap/size0)
// in the DMFile
class ColumnReadStream
{
public:
    ColumnReadStream(
        DMFileReader & reader,
        ColId col_id,
        const String & file_name_base,
        size_t max_read_buffer_size,
        const LoggerPtr & log,
        const ReadLimiterPtr & read_limiter);

    size_t getOffsetInFile(size_t i) const { return (*marks)[i].offset_in_compressed_file; }

    size_t getOffsetInDecompressedBlock(size_t i) const { return (*marks)[i].offset_in_decompressed_block; }

    double avg_size_hint;
    MarksInCompressedFilePtr marks;
    std::unique_ptr<CompressedSeekableReaderBuffer> buf;

private:
    std::unique_ptr<CompressedSeekableReaderBuffer> buildColDataReadBuffWithoutChecksum(
        DMFileReader & reader,
        ColId col_id,
        const String & file_name_base,
        size_t packs,
        size_t max_read_buffer_size,
        const ReadLimiterPtr & read_limiter,
        const LoggerPtr & log) const;
    static std::unique_ptr<CompressedSeekableReaderBuffer> buildColDataReadBuffWitChecksum(
        DMFileReader & reader,
        ColId col_id,
        const String & file_name_base,
        const ReadLimiterPtr & read_limiter);
    static std::unique_ptr<CompressedSeekableReaderBuffer> buildColDataReadBuffByMetaV2(
        DMFileReader & reader,
        ColId col_id,
        const String & file_name_base,
        const ReadLimiterPtr & read_limiter);
};
using ColumnReadStreamPtr = std::unique_ptr<ColumnReadStream>;
// stream_name/substream_name -> stream_ptr
using ColumnReadStreamMap = std::map<String, ColumnReadStreamPtr>;


} // namespace DB::DM
