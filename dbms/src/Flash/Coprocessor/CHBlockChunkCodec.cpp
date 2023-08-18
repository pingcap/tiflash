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

#include <Common/TiFlashException.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{
class CHBlockChunkCodecStream : public ChunkCodecStream
{
public:
    explicit CHBlockChunkCodecStream(const std::vector<tipb::FieldType> & field_types)
        : ChunkCodecStream(field_types)
    {
        for (const auto & field_type : field_types)
        {
            expected_types.emplace_back(getDataTypeByFieldTypeForComputingLayer(field_type));
        }
    }

    String getString() override
    {
        if (output == nullptr)
        {
            throw Exception("The output should not be null in getString()");
        }
        return output->releaseStr();
    }

    void clear() override { output = nullptr; }
    void encode(const Block & block, size_t start, size_t end) override;
    std::unique_ptr<WriteBufferFromOwnString> output;
    DataTypes expected_types;
};

size_t getExtraInfoSize(const Block & block)
{
    size_t size = 64; /// to hold some length of structures, such as column number, row number...
    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        size += column.name.size();
        size += column.type->getName().size();
        if (column.column->isColumnConst())
        {
            size += column.column->byteSize() * column.column->size();
        }
    }
    return size;
}

void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column;

    if (ColumnPtr converted = column->convertToFullColumnIfConst())
        full_column = converted;
    else
        full_column = column;

    IDataType::OutputStreamGetter output_stream_getter = [&](const IDataType::SubstreamPath &) {
        return &ostr;
    };
    type.serializeBinaryBulkWithMultipleStreams(*full_column, output_stream_getter, offset, limit, false, {});
}

void CHBlockChunkCodecStream::encode(const Block & block, size_t start, size_t end)
{
    /// only check block schema in CHBlock codec because for both
    /// Default codec and Arrow codec, it implicitly convert the
    /// input to the target output types.
    assertBlockSchema(expected_types, block, "CHBlockChunkCodecStream");
    // Encode data in chunk by chblock encode
    if (start != 0 || end != block.rows())
        throw TiFlashException("CHBlock encode only support encode whole block", Errors::Coprocessor::Internal);

    assert(output == nullptr);
    output = std::make_unique<WriteBufferFromOwnString>(block.bytes() + getExtraInfoSize(block));

    block.checkNumberOfRows();
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, *output);
    writeVarUInt(rows, *output);

    for (size_t i = 0; i < columns; i++)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);

        writeStringBinary(column.name, *output);
        writeStringBinary(column.type->getName(), *output);

        if (rows)
            writeData(*column.type, column.column, *output, 0, 0);
    }
}

std::unique_ptr<ChunkCodecStream> CHBlockChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<CHBlockChunkCodecStream>(field_types);
}

Block CHBlockChunkCodec::decode(const String & str, const DAGSchema & schema)
{
    ReadBufferFromString read_buffer(str);
    std::vector<String> output_names;
    for (const auto & c : schema)
        output_names.push_back(c.first);
    NativeBlockInputStream block_in(read_buffer, 0, std::move(output_names));
    return block_in.read();
}

Block CHBlockChunkCodec::decode(const String & str, const Block & header)
{
    ReadBufferFromString read_buffer(str);
    NativeBlockInputStream block_in(read_buffer, header, 0, /*align_column_name_with_header=*/true);
    return block_in.read();
}

} // namespace DB
