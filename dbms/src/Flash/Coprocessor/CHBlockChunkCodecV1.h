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

#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/MppVersion.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/Compression/CompressionMethod.h>

namespace DB
{
size_t ApproxBlockHeaderBytes(const Block & block);
using CompressedCHBlockChunkReadBuffer = CompressedReadBuffer<false>;
using CompressedCHBlockChunkWriteBuffer = CompressedWriteBuffer<false>;
void DecodeColumns(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size = 0);
Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & rows);
CompressionMethod ToInternalCompressionMethod(tipb::CompressionMode compression_mode);
extern void WriteColumnData(
    const IDataType & type,
    const ColumnPtr & column,
    WriteBuffer & ostr,
    size_t offset,
    size_t limit);

struct CHBlockChunkCodecV1 : boost::noncopyable
{
    using Self = CHBlockChunkCodecV1;
    using EncodeRes = std::string;

    const Block & header;
    const size_t header_size;
    MppVersion mpp_version;

    size_t encoded_rows{};
    size_t original_size{};
    size_t compressed_size{};

    void clear();
    explicit CHBlockChunkCodecV1(const Block & header_, MppVersion mpp_version_);
    //
    EncodeRes encode(const MutableColumns & columns, CompressionMethod compression_method);
    EncodeRes encode(std::vector<MutableColumns> && columns, CompressionMethod compression_method);
    EncodeRes encode(const std::vector<MutableColumns> & columns, CompressionMethod compression_method);
    EncodeRes encode(const Columns & columns, CompressionMethod compression_method);
    EncodeRes encode(const std::vector<Columns> & columns, CompressionMethod compression_method);
    EncodeRes encode(std::vector<Columns> && columns, CompressionMethod compression_method);
    EncodeRes encode(const Block & block, CompressionMethod compression_method, bool check_schema = true);
    EncodeRes encode(const std::vector<Block> & blocks, CompressionMethod compression_method, bool check_schema = true);
    EncodeRes encode(std::vector<Block> && blocks, CompressionMethod compression_method, bool check_schema = true);
    //
    static EncodeRes encode(std::string_view str, CompressionMethod compression_method);
    static Block decode(const Block & header, std::string_view str);
};

} // namespace DB
