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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/CodecUtils.h>

namespace DB
{
class CHBlockChunkDecodeAndSquash;

class CHBlockChunkCodec final : public ChunkCodec
{
public:
    CHBlockChunkCodec() = default;
    explicit CHBlockChunkCodec(const Block & header_);
    explicit CHBlockChunkCodec(const DAGSchema & schema);

    Block decode(const String &, const DAGSchema & schema) override;
    static Block decode(const String &, const Block & header);
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;

private:
    friend class CHBlockChunkDecodeAndSquash;
    void readColumnMeta(size_t i, ReadBuffer & istr, ColumnWithTypeAndName & column);
    void readBlockMeta(ReadBuffer & istr, size_t & columns, size_t & rows) const;
    static void readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows);
    /// 'reserve_size' used for Squash usage, and takes effect when 'reserve_size' > 0
    Block decodeImpl(ReadBuffer & istr, size_t reserve_size = 0);

    Block header;
    std::vector<CodecUtils::DataTypeWithTypeName> header_datatypes;
    std::vector<String> output_names;
};

} // namespace DB
