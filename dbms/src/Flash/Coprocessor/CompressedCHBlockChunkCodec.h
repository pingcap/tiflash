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

#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedStream.h>

namespace mpp
{
enum CompressMethod : int;
}

namespace DB
{
class CompressedCHBlockChunkCodec final
{
public:
    using CompressedReadBuffer = CompressedReadBuffer<false>;
    CompressedCHBlockChunkCodec() = default;
    explicit CompressedCHBlockChunkCodec(const Block & header_);
    explicit CompressedCHBlockChunkCodec(const DAGSchema & schema);

    static Block decode(const String &, const DAGSchema & schema);
    static Block decode(const String &, const Block & header);
    static std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types, CompressionMethod compress_method);

private:
    void readColumnMeta(size_t i, CompressedReadBuffer & istr, ColumnWithTypeAndName & column);
    void readBlockMeta(CompressedReadBuffer & istr, size_t & columns, size_t & rows) const;
    static void readData(const IDataType & type, IColumn & column, CompressedReadBuffer & istr, size_t rows);
    /// 'reserve_size' used for Squash usage, and takes effect when 'reserve_size' > 0
    Block decodeImpl(CompressedReadBuffer & istr, size_t reserve_size = 0);

    CHBlockChunkCodec chunk_codec;
};

CompressionMethod ToInternalCompressionMethod(mpp::CompressMethod compress_method);

} // namespace DB
