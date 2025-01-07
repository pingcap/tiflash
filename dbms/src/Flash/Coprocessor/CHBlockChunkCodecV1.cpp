

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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressionCodecFactory.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

namespace DB
{
static size_t ApproxBlockHeaderBytes(const Block & block)
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

void EncodeHeader(WriteBuffer & ostr, const Block & header, size_t rows, MppVersion mpp_version)
{
    size_t columns = header.columns();
    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; i++)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);
        writeStringBinary(column.name, ostr);
        const auto & ser_type = CodecUtils::convertDataTypeByMppVersion(*column.type, mpp_version);
        writeStringBinary(ser_type.getName(), ostr);
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
        CodecUtils::checkColumnSize("CHBlockChunkCodecV1", header.columns(), columns);

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
                CodecUtils::checkDataTypeName(
                    "CHBlockChunkCodecV1",
                    i,
                    header.getByPosition(i).type->getName(),
                    type_name);
            column.type = DataTypeFactory::instance().get(type_name); // Respect the type name from encoder
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
    const auto & name_and_type_list = res.getColumnsWithTypeAndName();
    size_t column_size = mutable_columns.size();
    for (size_t i = 0; i < column_size; ++i)
    {
        auto && column = mutable_columns[i];
        if (name_and_type_list[i].type->haveMaximumSizeOfValue())
        {
            if (reserve_size > 0)
                column->reserve(std::max(rows_to_read, reserve_size));
            else
                column->reserve(rows_to_read + column->size());
        }
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
                [&](const IDataType::SubstreamPath &) { return &istr; },
                sz,
                0,
                /*position_independent_encoding=*/true,
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

template <typename ColumnsHolder>
constexpr static bool isBlockType()
{
    return std::is_same_v<std::remove_const_t<std::remove_reference_t<ColumnsHolder>>, Block>;
}

template <typename ColumnsHolder>
static void calcColumnEncodeInfoImpl(ColumnsHolder && columns_holder, size_t & bytes, size_t & total_rows)
{
    bytes += 8 /*rows*/;

    if constexpr (isBlockType<ColumnsHolder>())
    {
        const Block & block = columns_holder;
        if (const auto rows = block.rows(); rows)
        {
            block.checkNumberOfRows();
            total_rows += rows;
            bytes += block.bytes();
        }
    }
    else
    {
        // check each column
        if likely (columns_holder.front())
        {
            const auto rows = columns_holder.front()->size();
            total_rows += rows;
            for (const auto & column : columns_holder)
            {
                RUNTIME_ASSERT(column);
                RUNTIME_ASSERT(rows == column->size());
                bytes += column->byteSize();
            }
        }
        else
        {
            for (const auto & column : columns_holder)
            {
                RUNTIME_ASSERT(!column);
            }
        }
    }
}

static void calcColumnEncodeInfo(const std::vector<MutableColumns> & batch_columns, size_t & bytes, size_t & rows)
{
    for (auto && columns : batch_columns)
    {
        calcColumnEncodeInfoImpl(columns, bytes, rows);
    }
}
static void calcColumnEncodeInfo(const std::vector<Columns> & batch_columns, size_t & bytes, size_t & rows)
{
    for (auto && columns : batch_columns)
    {
        calcColumnEncodeInfoImpl(columns, bytes, rows);
    }
}
static void calcColumnEncodeInfo(const std::vector<Block> & blocks, size_t & bytes, size_t & rows)
{
    for (auto && block : blocks)
    {
        calcColumnEncodeInfoImpl(block, bytes, rows);
    }
}
static void calcColumnEncodeInfo(const MutableColumns & columns, size_t & bytes, size_t & rows)
{
    calcColumnEncodeInfoImpl(columns, bytes, rows);
}
static void calcColumnEncodeInfo(const Columns & columns, size_t & bytes, size_t & rows)
{
    calcColumnEncodeInfoImpl(columns, bytes, rows);
}
static void calcColumnEncodeInfo(const Block & block, size_t & bytes, size_t & rows)
{
    calcColumnEncodeInfoImpl(block, bytes, rows);
}

struct CHBlockChunkCodecV1Impl
{
    CHBlockChunkCodecV1 & inner;

    explicit CHBlockChunkCodecV1Impl(CHBlockChunkCodecV1 & inner_)
        : inner(inner_)
    {}

    CHBlockChunkCodecV1::EncodeRes encode(const Block & block, CompressionMethod compression_method)
    {
        return encodeImpl(block, compression_method);
    }
    CHBlockChunkCodecV1::EncodeRes encode(const std::vector<Block> & blocks, CompressionMethod compression_method)
    {
        return encodeImpl(blocks, compression_method);
    }
    CHBlockChunkCodecV1::EncodeRes encode(std::vector<Block> && blocks, CompressionMethod compression_method)
    {
        return encodeImpl(std::move(blocks), compression_method);
    }

    static const ColumnPtr & toColumnPtr(const Columns & c, size_t index) { return c[index]; }
    static ColumnPtr toColumnPtr(Columns && c, size_t index) { return std::move(c[index]); }
    static ColumnPtr toColumnPtr(MutableColumns && c, size_t index) { return std::move(c[index]); }
    static ColumnPtr toColumnPtr(const MutableColumns & c, size_t index) { return c[index]->getPtr(); }
    static const ColumnPtr & toColumnPtr(const Block & block, size_t index)
    {
        return block.getByPosition(index).column;
    }
    static ColumnPtr toColumnPtr(Block && block, size_t index) { return std::move(block.getByPosition(index).column); }

    template <typename ColumnsHolder>
    static size_t getRows(ColumnsHolder && columns_holder)
    {
        if constexpr (isBlockType<ColumnsHolder>())
        {
            const Block & block = columns_holder;
            size_t rows = block.rows();
            return rows;
        }
        else
        {
            if unlikely (!columns_holder.front())
                return 0;
            size_t rows = columns_holder.front()->size();
            return rows;
        }
    }

    template <typename ColumnsHolder>
    void encodeColumnImpl(ColumnsHolder && columns_holder, WriteBuffer * ostr_ptr)
    {
        size_t rows = getRows(std::forward<ColumnsHolder>(columns_holder));
        if (!rows)
            return;

        // Encode row count for next columns
        writeVarUInt(rows, *ostr_ptr);

        // Encode columns data
        for (size_t col_index = 0; col_index < inner.header.columns(); ++col_index)
        {
            auto && col_type_name = inner.header.getByPosition(col_index);
            auto && column_ptr = toColumnPtr(std::forward<ColumnsHolder>(columns_holder), col_index);
            const auto & ser_type = CodecUtils::convertDataTypeByMppVersion(*col_type_name.type, inner.mpp_version);
            CHBlockChunkCodec::WriteColumnData(ser_type, column_ptr, *ostr_ptr, 0, 0);
        }

        inner.encoded_rows += rows;
    }
    void encodeColumn(const MutableColumns & columns, WriteBuffer * ostr_ptr)
    {
        return encodeColumnImpl(columns, ostr_ptr);
    }
    void encodeColumn(const Columns & columns, WriteBuffer * ostr_ptr) { return encodeColumnImpl(columns, ostr_ptr); }
    void encodeColumn(const std::vector<MutableColumns> & batch_columns, WriteBuffer * ostr_ptr)
    {
        for (auto && batch : batch_columns)
        {
            encodeColumnImpl(batch, ostr_ptr);
        }
    }
    void encodeColumn(std::vector<MutableColumns> && batch_columns, WriteBuffer * ostr_ptr)
    {
        for (auto && batch : batch_columns)
        {
            encodeColumnImpl(std::move(batch), ostr_ptr);
        }
    }
    void encodeColumn(const std::vector<Columns> & batch_columns, WriteBuffer * ostr_ptr)
    {
        for (auto && batch : batch_columns)
        {
            encodeColumnImpl(batch, ostr_ptr);
        }
    }
    void encodeColumn(std::vector<Columns> && batch_columns, WriteBuffer * ostr_ptr)
    {
        for (auto && batch : batch_columns)
        {
            encodeColumnImpl(std::move(batch), ostr_ptr);
        }
    }
    void encodeColumn(const Block & block, WriteBuffer * ostr_ptr) { return encodeColumnImpl(block, ostr_ptr); }
    void encodeColumn(const std::vector<Block> & blocks, WriteBuffer * ostr_ptr)
    {
        for (auto && block : blocks)
        {
            encodeColumnImpl(block, ostr_ptr);
        }
    }
    void encodeColumn(std::vector<Block> && blocks, WriteBuffer * ostr_ptr)
    {
        for (auto && block : blocks)
        {
            encodeColumnImpl(std::move(block), ostr_ptr);
        }
    }
    template <typename VecColumns>
    CHBlockChunkCodecV1::EncodeRes encodeImpl(VecColumns && batch_columns, CompressionMethod compression_method)
    {
        size_t column_encode_bytes = 0;
        const size_t rows = ({
            size_t rows = 0;
            // Calculate total rows and check data valid
            calcColumnEncodeInfo(batch_columns, column_encode_bytes, rows);
            rows;
        });

        if unlikely (0 == rows)
        {
            // no rows and no need to encode header
            return {};
        }

        // compression method flag; NONE, LZ4, ZSTD, defined in `CompressionMethodByte`
        // ... encoded by compression pattern ...
        // header meta:
        //     columns count;
        //     total row count (multi parts);
        //     for each column:
        //         column name;
        //         column type;
        // for each part:
        //     row count;
        //     columns data;

        size_t init_size = column_encode_bytes + inner.header_size + 1 /*compression method*/;
        auto output_buffer = std::make_unique<WriteBufferFromOwnString>(init_size);
        std::unique_ptr<CompressedCHBlockChunkWriteBuffer> compress_codec{};
        WriteBuffer * ostr_ptr = output_buffer.get();

        // Init compression writer
        if (compression_method != CompressionMethod::NONE)
        {
            // CompressedWriteBuffer will encode compression method flag as first byte
            compress_codec = std::make_unique<CompressedCHBlockChunkWriteBuffer>(
                *output_buffer,
                CompressionSettings(compression_method),
                init_size);
            ostr_ptr = compress_codec.get();
        }
        else
        {
            // Write compression method flag
            output_buffer->write(static_cast<char>(CompressionMethodByte::NONE));
        }

        // Encode header
        EncodeHeader(*ostr_ptr, inner.header, rows, inner.mpp_version);
        // Encode column data
        encodeColumn(std::forward<VecColumns>(batch_columns), ostr_ptr);

        // Flush rest buffer
        if (compress_codec)
        {
            compress_codec->next();
            inner.original_size += compress_codec->getUncompressedBytes();
            inner.compressed_size += compress_codec->getCompressedBytes();
        }
        else
        {
            inner.original_size += output_buffer->count();
        }

        return output_buffer->releaseStr();
    }
};

CHBlockChunkCodecV1::CHBlockChunkCodecV1(const Block & header_, MppVersion mpp_version_)
    : header(header_)
    , header_size(ApproxBlockHeaderBytes(header))
    , mpp_version(mpp_version_)
{}

static void checkSchema(const Block & header, const Block & block)
{
    CodecUtils::checkColumnSize("CHBlockChunkCodecV1", header.columns(), block.columns());
    for (size_t column_index = 0; column_index < header.columns(); ++column_index)
    {
        auto && type_name = block.getByPosition(column_index).type->getName();
        CodecUtils::checkDataTypeName(
            "CHBlockChunkCodecV1",
            column_index,
            header.getByPosition(column_index).type->getName(),
            type_name);
    }
}

CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const Block & block,
    CompressionMethod compression_method,
    bool check_schema)
{
    if (check_schema)
    {
        checkSchema(header, block);
    }
    return CHBlockChunkCodecV1Impl{*this}.encode(block, compression_method);
}

void CHBlockChunkCodecV1::clear()
{
    encoded_rows = 0;
    original_size = 0;
    compressed_size = 0;
}

CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const MutableColumns & columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(columns, compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const Columns & columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(columns, compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const std::vector<MutableColumns> & columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(columns, compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    std::vector<MutableColumns> && columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(std::move(columns), compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const std::vector<Columns> & columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(std::move(columns), compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    std::vector<Columns> && columns,
    CompressionMethod compression_method)
{
    return CHBlockChunkCodecV1Impl{*this}.encodeImpl(std::move(columns), compression_method);
}
CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    const std::vector<Block> & blocks,
    CompressionMethod compression_method,
    bool check_schema)
{
    if (check_schema)
    {
        for (auto && block : blocks)
        {
            checkSchema(header, block);
        }
    }

    return CHBlockChunkCodecV1Impl{*this}.encode(blocks, compression_method);
}

CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(
    std::vector<Block> && blocks,
    CompressionMethod compression_method,
    bool check_schema)
{
    if (check_schema)
    {
        for (auto && block : blocks)
        {
            checkSchema(header, block);
        }
    }

    return CHBlockChunkCodecV1Impl{*this}.encode(std::move(blocks), compression_method);
}

static Block decodeCompression(const Block & header, ReadBuffer & istr)
{
    size_t decoded_rows{};
    auto decoded_block = DecodeHeader(istr, header, decoded_rows);
    DecodeColumns(istr, decoded_block, decoded_rows, 0);
    assert(decoded_rows == decoded_block.rows());
    return decoded_block;
}

template <typename Buffer>
extern size_t CompressionEncode(
    std::string_view source,
    const CompressionSettings & compression_settings,
    Buffer & compressed_buffer);

CHBlockChunkCodecV1::EncodeRes CHBlockChunkCodecV1::encode(std::string_view str, CompressionMethod compression_method)
{
    assert(compression_method != CompressionMethod::NONE);

    String compressed_buffer;
    auto codec = CompressionCodecFactory::create(CompressionSetting(compression_method));
    compressed_buffer.resize(codec->getCompressedReserveSize(str.size()));
    size_t compressed_size = codec->compress(str.data(), str.size(), compressed_buffer.data());
    compressed_buffer.resize(compressed_size);
    return compressed_buffer;
}

Block CHBlockChunkCodecV1::decode(const Block & header, std::string_view str)
{
    assert(!str.empty());

    // read first byte of compression method flag which defined in `CompressionMethodByte`
    if (static_cast<CompressionMethodByte>(str[0]) == CompressionMethodByte::NONE)
    {
        str = str.substr(1, str.size() - 1);
        ReadBufferFromString buff_str(str);
        return decodeCompression(header, buff_str);
    }
    ReadBufferFromString buff_str(str);
    auto && istr = CompressedCHBlockChunkReadBuffer(buff_str);
    return decodeCompression(header, istr);
}

} // namespace DB
