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
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
class CHBlockChunkCodecStream : public ChunkCodecStream
{
public:
    explicit CHBlockChunkCodecStream(const std::vector<tipb::FieldType> & field_types, MppVersion mpp_version_)
        : ChunkCodecStream(field_types)
        , mpp_version(mpp_version_)
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
    const MppVersion mpp_version;
};

CHBlockChunkCodec::CHBlockChunkCodec(const Block & header_)
    : header(header_)
{
    for (const auto & column : header)
        header_datatypes.emplace_back(column.type, column.type->getName());
}

CHBlockChunkCodec::CHBlockChunkCodec(const DAGSchema & schema)
{
    for (const auto & c : schema)
        output_names.push_back(c.first);
}

size_t getExtraInfoSize(const Block & block)
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
        if (column.column->isColumnConst())
        {
            size += column.column->byteSize() * column.column->size();
        }
    }
    return size;
}

void WriteColumnData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
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
    type.serializeBinaryBulkWithMultipleStreams(
        *full_column,
        output_stream_getter,
        offset,
        limit,
        /*position_independent_encoding=*/true,
        {});
}

void CHBlockChunkCodec::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows)
{
    IDataType::InputStreamGetter input_stream_getter = [&](const IDataType::SubstreamPath &) {
        return &istr;
    };
    type.deserializeBinaryBulkWithMultipleStreams(
        column,
        input_stream_getter,
        rows,
        0,
        /*position_independent_encoding=*/true,
        {});
}

size_t ApproxBlockBytes(const Block & block)
{
    return block.bytes() + getExtraInfoSize(block);
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
    output = std::make_unique<WriteBufferFromOwnString>(ApproxBlockBytes(block));

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
            WriteColumnData(*column.type, column.column, *output, 0, 0);
    }
}

std::unique_ptr<ChunkCodecStream> CHBlockChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<CHBlockChunkCodecStream>(field_types);
}

Block CHBlockChunkCodec::decodeImpl(ReadBuffer & istr, size_t reserve_size)
{
    Block res;
    if (istr.eof())
    {
        return res;
    }

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;
    readBlockMeta(istr, columns, rows);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;
        readColumnMeta(i, istr, column);

        /// Data
        MutableColumnPtr read_column = column.type->createColumn();
        if (column.type->haveMaximumSizeOfValue())
        {
            if (reserve_size > 0)
                read_column->reserve(std::max(rows, reserve_size));
            else if (rows)
                read_column->reserve(rows);
        }

        if (rows) /// If no rows, nothing to read.
            readData(*column.type, *read_column, istr, rows);

        column.column = std::move(read_column);
        res.insert(std::move(column));
    }
    return res;
}

void CHBlockChunkCodec::readBlockMeta(ReadBuffer & istr, size_t & columns, size_t & rows) const
{
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    if (header)
        CodecUtils::checkColumnSize("CHBlockChunkCodec", header.columns(), columns);
    else if (!output_names.empty())
        CodecUtils::checkColumnSize("CHBlockChunkCodec", output_names.size(), columns);
}

void CHBlockChunkCodec::readColumnMeta(size_t i, ReadBuffer & istr, ColumnWithTypeAndName & column)
{
    /// Name
    readBinary(column.name, istr);
    if (header)
        column.name = header.getByPosition(i).name;
    else if (!output_names.empty())
        column.name = output_names[i];

    /// Type
    String type_name;
    readBinary(type_name, istr);
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    if (header)
    {
        CodecUtils::checkDataTypeName("CHBlockChunkCodec", i, header_datatypes[i].name, type_name);
        column.type = header_datatypes[i].type;
    }
    else
    {
        column.type = data_type_factory.get(type_name);
    }
}

Block CHBlockChunkCodec::decode(const String & str, const DAGSchema & schema)
{
    ReadBufferFromString read_buffer(str);
    return CHBlockChunkCodec(schema).decodeImpl(read_buffer);
}

Block CHBlockChunkCodec::decode(const String & str, const Block & header)
{
    ReadBufferFromString read_buffer(str);
    return CHBlockChunkCodec(header).decodeImpl(read_buffer);
}

Block CHBlockChunkCodec::decode(const String & str)
{
    ReadBufferFromString read_buffer(str);
    return decodeImpl(read_buffer);
}

} // namespace DB
