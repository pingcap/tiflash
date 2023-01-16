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
#include <IO/CompressedWriteBuffer.h>

namespace DB
{
size_t ApproxBlockHeaderBytes(const Block & block);
using CompressedCHBlockChunkReadBuffer = CompressedReadBuffer<false>;
using CompressedCHBlockChunkWriteBuffer = CompressedWriteBuffer<false>;
void EncodeHeader(WriteBuffer & ostr, const Block & header, size_t rows);
void DecodeColumns(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size = 0);
Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & rows);
CompressionMethod ToInternalCompressionMethod(tipb::CompressionMode compression_mode);
extern void WriteColumnData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

struct CHBlockChunkCodecV1
{
    using Self = CHBlockChunkCodecV1;

    const Block & header;
    const size_t header_size;
    size_t encoded_rows{};
    size_t original_size{};
    size_t compressed_size{};
    bool always_keep_header{};

    explicit CHBlockChunkCodecV1(const Block & header_, bool always_keep_header_)
        : header(header_)
        , header_size(ApproxBlockHeaderBytes(header))
        , always_keep_header(always_keep_header_)
    {
    }

    static std::string encode(const Block & block, CompressionMethod compression_method, bool always_keep_header)
    {
        return Self{block, always_keep_header}.encode(compression_method);
    }

    void clear()
    {
        encoded_rows = 0;
        original_size = 0;
        compressed_size = 0;
    }

    std::string encode(CompressionMethod compression_method)
    {
        return encodeImpl(header, compression_method);
    }

    std::string encode(const MutableColumns & columns, CompressionMethod compression_method)
    {
        return encodeImpl(columns, compression_method);
    }
    std::string encode(const Columns & columns, CompressionMethod compression_method)
    {
        return encodeImpl(columns, compression_method);
    }
    std::string encode(const std::vector<MutableColumns> & columns, CompressionMethod compression_method)
    {
        return encodeImpl(columns, compression_method);
    }
    std::string encode(std::vector<MutableColumns> && columns, CompressionMethod compression_method)
    {
        return encodeImpl(std::move(columns), compression_method);
    }

    template <typename ColumnsHolder>
    static void getColumnEncodeInfoImpl(ColumnsHolder && columns_holder, size_t & bytes, size_t & rows)
    {
        bytes += 8 /*rows*/;

        if constexpr (isBlockType<ColumnsHolder>())
        {
            rows += columns_holder.rows();
            for (size_t col_index = 0; col_index < columns_holder.columns(); ++col_index)
            {
                auto && col_type_name = columns_holder.getByPosition(col_index);
                bytes += col_type_name.column->byteSize();
            }
        }
        else
        {
            rows += columns_holder.front()->size();
            for (const auto & elem : columns_holder)
                bytes += elem->byteSize();
        }
    }
    static const ColumnPtr & toColumnPtr(const Columns & c, size_t index)
    {
        return c[index];
    }
    static ColumnPtr toColumnPtr(MutableColumns && c, size_t index)
    {
        return std::move(c[index]);
    }
    static ColumnPtr toColumnPtr(const MutableColumns & c, size_t index)
    {
        return c[index]->getPtr();
    }
    static const ColumnPtr & toColumnPtr(const Block & block, size_t index)
    {
        return block.getByPosition(index).column;
    }

    template <typename ColumnsHolder>
    size_t getRowsByColumns(ColumnsHolder && columns_holder)
    {
        size_t rows = columns_holder.front()->size();
        return rows;
    }

    template <typename ColumnsHolder>
    constexpr static bool isBlockType()
    {
        return std::is_same_v<std::remove_const_t<std::remove_reference_t<ColumnsHolder>>, Block>;
    }

    template <typename ColumnsHolder>
    size_t getRows(ColumnsHolder && columns_holder)
    {
        if constexpr (isBlockType<ColumnsHolder>())
        {
            size_t rows = columns_holder.rows();
            return rows;
        }
        else
        {
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
        for (size_t col_index = 0; col_index < header.columns(); ++col_index)
        {
            auto && col_type_name = header.getByPosition(col_index);
            auto && column_ptr = toColumnPtr(std::forward<ColumnsHolder>(columns_holder), col_index);
            WriteColumnData(*col_type_name.type, column_ptr, *ostr_ptr, 0, 0);
        }

        encoded_rows += rows;
    }
    void encodeColumn(const MutableColumns & columns, WriteBuffer * ostr_ptr)
    {
        return encodeColumnImpl(columns, ostr_ptr);
    }
    void encodeColumn(const Columns & columns, WriteBuffer * ostr_ptr)
    {
        return encodeColumnImpl(columns, ostr_ptr);
    }
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
    void encodeColumn(const Block & block, WriteBuffer * ostr_ptr)
    {
        assert(&block == &header);
        return encodeColumnImpl(block, ostr_ptr);
    }

    static void getColumnEncodeInfo(const std::vector<MutableColumns> & batch_columns, size_t & bytes, size_t & rows)
    {
        for (auto && columns : batch_columns)
        {
            getColumnEncodeInfoImpl(columns, bytes, rows);
        }
    }
    static void getColumnEncodeInfo(const MutableColumns & columns, size_t & bytes, size_t & rows)
    {
        getColumnEncodeInfoImpl(columns, bytes, rows);
    }
    static void getColumnEncodeInfo(const Columns & columns, size_t & bytes, size_t & rows)
    {
        getColumnEncodeInfoImpl(columns, bytes, rows);
    }
    static void getColumnEncodeInfo(const Block & block, size_t & bytes, size_t & rows)
    {
        getColumnEncodeInfoImpl(block, bytes, rows);
    }

    template <typename VecColumns>
    std::string encodeImpl(VecColumns && batch_columns, CompressionMethod compression_method)
    {
        size_t column_encode_bytes = 0;
        size_t rows = 0;

        getColumnEncodeInfo(batch_columns, column_encode_bytes, rows);

        if unlikely (rows <= 0 && !always_keep_header)
        {
            return "";
        }

        // compression method flag; NONE, LZ4, ZSTD, defined in `CompressionMethodByte`
        // ...
        // header meta:
        //     columns count;
        //     total row count (multi parts);
        //     for each column:
        //         column name;
        //         column type;
        // for each part:
        //     row count;
        //     columns data;

        size_t init_size = column_encode_bytes + header_size + 1 /*compression method*/;
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
        EncodeHeader(*ostr_ptr, header, rows);
        if (rows > 0)
            encodeColumn(std::forward<VecColumns>(batch_columns), ostr_ptr);

        // Flush rest buffer
        if (compress_codec)
        {
            compress_codec->next();
            original_size += compress_codec->getUncompressedBytes();
            compressed_size += compress_codec->getCompressedBytes();
        }
        else
        {
            original_size += output_buffer->count();
        }

        return output_buffer->releaseStr();
    }
};

} // namespace DB
