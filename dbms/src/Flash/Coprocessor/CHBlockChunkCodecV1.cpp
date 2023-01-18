

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

#include <DataTypes/DataTypeFactory.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>


namespace DB
{

size_t ApproxBlockHeaderBytes(const Block & block)
{
    size_t size = 8 + 8; /// to hold some length of structures, such as column number, row number...
    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        size += column.name.size();
        size += 8;
        size += column.type->getName().size();
        size += 8;
    }
    return size;
}

void EncodeHeader(WriteBuffer & ostr, const Block & header, size_t rows)
{
    size_t columns = header.columns();
    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; i++)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);
        writeStringBinary(column.name, ostr);
        writeStringBinary(column.type->getName(), ostr);
    }
}

Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & total_rows)
{
    Block res;

    assert(!istr.eof());

    size_t columns = 0;
    {
        readVarUInt(columns, istr);
        readVarUInt(total_rows, istr);
    }
    if (header)
        CodecUtils::checkColumnSize(header.columns(), columns);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;
        {
            readBinary(column.name, istr);
            if (header)
                column.name = header.getByPosition(i).name;
            String type_name;
            readBinary(type_name, istr);
            if (header)
            {
                CodecUtils::checkDataTypeName(i, header.getByPosition(i).type->getName(), type_name);
                column.type = header.getByPosition(i).type;
            }
            else
            {
                const auto & data_type_factory = DataTypeFactory::instance();
                column.type = data_type_factory.get(type_name);
            }
        }
        res.insert(std::move(column));
    }

    return res;
}

static inline void decodeColumnsByBlock(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size)
{
    if (!rows_to_read)
        return;

    auto && mutable_columns = res.mutateColumns();
    for (auto && column : mutable_columns)
    {
        if (reserve_size > 0)
            column->reserve(std::max(rows_to_read, reserve_size));
        else
            column->reserve(rows_to_read + column->size());
    }

    // Contain columns of multi blocks
    size_t decode_rows = 0;
    for (size_t sz = 0; decode_rows < rows_to_read; decode_rows += sz)
    {
        readVarUInt(sz, istr);

        assert(sz > 0);

        // Decode columns of one block
        for (size_t i = 0; i < res.columns(); ++i)
        {
            /// Data
            res.getByPosition(i).type->deserializeBinaryBulkWithMultipleStreams(
                *mutable_columns[i],
                [&](const IDataType::SubstreamPath &) {
                    return &istr;
                },
                sz,
                0,
                {},
                {});
        }
    }

    assert(decode_rows == rows_to_read);

    res.setColumns(std::move(mutable_columns));
}

void DecodeColumns(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size)
{
    return decodeColumnsByBlock(istr, res, rows_to_read, reserve_size);
}

CompressionMethod ToInternalCompressionMethod(tipb::CompressionMode compression_mode)
{
    switch (compression_mode)
    {
    case tipb::CompressionMode::NONE:
        return CompressionMethod::NONE;
    case tipb::CompressionMode::FAST:
        return CompressionMethod::LZ4; // use LZ4 method as fast mode
    case tipb::CompressionMode::HIGH_COMPRESSION:
        return CompressionMethod::ZSTD; // use ZSTD method as HC mode
    default:
        return CompressionMethod::NONE;
    }
}

} // namespace DB