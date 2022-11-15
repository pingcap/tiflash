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

#include <Common/TiFlashException.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>

#include <memory>

#include "Flash/Coprocessor/tzg-metrics.h"

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

CHBlockChunkCodec::CHBlockChunkCodec(
    const Block & header_)
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

/*
class SnappyCompressWriteBuffer final : public BufferWithOwnMemory<WriteBuffer>
{
    WriteBuffer & out;
    PODArray<char> compressed_buffer;

    explicit SnappyCompressWriteBuffer(WriteBuffer & out_)
        : out(out_)
    {
    }

    void nextImpl() override
    {
        if (!offset())
            return;
        size_t uncompressed_size = offset();
        size_t compressed_size = snappy::MaxCompressedLength(uncompressed_size);
        compressed_buffer.resize(compressed_size);
        snappy::RawCompress(working_buffer.begin(), uncompressed_size, compressed_buffer.data(), &compressed_size);
        out.write(compressed_buffer.data(), compressed_size);
    };
};

class SnappyUncompressWriteBuffer final : public BufferWithOwnMemory<ReadBuffer>
{
    ReadBuffer * compressed_in;
    PODArray<char> own_compressed_buffer;

    explicit SnappyUncompressWriteBuffer(ReadBuffer * in_)
        : BufferWithOwnMemory<ReadBuffer>(0)
        , in(in_)
    {}

    bool nextImpl() override
    {
        if (compressed_in->eof())
            return false;
        own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
        compressed_in->readStrict(&own_compressed_buffer[0], COMPRESSED_BLOCK_HEADER_SIZE);
        auto method = own_compressed_buffer[0];
        auto size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
        auto size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw Exception("Too large size_compressed. Most likely corrupted data.");

        if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE
            && compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
        {
            compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
            compressed_buffer = compressed_in->position();
            compressed_in->position() += size_compressed;
        }
        else
        {
            own_compressed_buffer.resize(size_compressed);
            compressed_buffer = &own_compressed_buffer[0];
            compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
        }

        if constexpr (has_checksum)
        {
            if (!disable_checksum[0] && checksum != CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed))
                throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);
            return size_compressed + sizeof(checksum);
        }
        else
        {
            return size_compressed;
        }
    }
};
*/

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

void CHBlockChunkCodec::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows)
{
    IDataType::InputStreamGetter input_stream_getter = [&](const IDataType::SubstreamPath &) {
        return &istr;
    };
    type.deserializeBinaryBulkWithMultipleStreams(column, input_stream_getter, rows, 0, false, {});
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
    size_t init_size = block.bytes() + getExtraInfoSize(block);
    output = std::make_unique<WriteBufferFromOwnString>(init_size);
    std::unique_ptr<WriteBuffer> compress_buffer;
    WriteBuffer * ostr_ptr = output.get();
    auto mm = static_cast<CompressionMethod>(tzg::SnappyStatistic::globalInstance().getMethod());
    if (mm != CompressionMethod::NONE)
    {
        compress_buffer = std::make_unique<CompressedWriteBuffer<false>>(*output, CompressionSettings(mm), init_size);
        ostr_ptr = compress_buffer.get();
    }

    block.checkNumberOfRows();
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, *ostr_ptr);
    writeVarUInt(rows, *ostr_ptr);


    for (size_t i = 0; i < columns; i++)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);

        writeStringBinary(column.name, *ostr_ptr);
        writeStringBinary(column.type->getName(), *ostr_ptr);

        if (rows)
            writeData(*column.type, column.column, *ostr_ptr, 0, 0);
    }
}

std::unique_ptr<ChunkCodecStream> CHBlockChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<CHBlockChunkCodecStream>(field_types);
}

Block CHBlockChunkCodec::decodeImpl(ReadBuffer & istr, size_t reserve_size)
{
    Block res;

    std::unique_ptr<ReadBuffer> compress_buffer;
    ReadBuffer * istr_ptr = &istr;
    auto mm = static_cast<CompressionMethod>(tzg::SnappyStatistic::globalInstance().getMethod());
    if (mm != CompressionMethod::NONE)
    {
        compress_buffer = std::make_unique<CompressedReadBuffer<false>>(istr);
        istr_ptr = compress_buffer.get();
    }

    if (istr_ptr->eof())
    {
        return res;
    }

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;
    readBlockMeta(*istr_ptr, columns, rows);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;
        readColumnMeta(i, *istr_ptr, column);

        /// Data
        MutableColumnPtr read_column = column.type->createColumn();
        if (reserve_size > 0)
            read_column->reserve(std::max(rows, reserve_size));
        else if (rows)
            read_column->reserve(rows);

        if (rows) /// If no rows, nothing to read.
            readData(*column.type, *read_column, *istr_ptr, rows);

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
        CodecUtils::checkColumnSize(header.columns(), columns);
    else if (!output_names.empty())
        CodecUtils::checkColumnSize(output_names.size(), columns);
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
        CodecUtils::checkDataTypeName(i, header_datatypes[i].name, type_name);
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
} // namespace DB
