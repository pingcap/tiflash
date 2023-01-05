

#include "CompressCHBlockChunkCodecStream.h"

#include <DataTypes/DataTypeFactory.h>

#include <cassert>
#include <cstddef>
#include <vector>

namespace DB
{
extern void WriteColumnData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

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

void EncodeColumn__(WriteBuffer & ostr, const ColumnPtr & column, const ColumnWithTypeAndName & type_name)
{
    writeVarUInt(column->size(), ostr);
    WriteColumnData(*type_name.type, column, ostr, 0, 0);
}

std::unique_ptr<CompressCHBlockChunkCodecStream> NewCompressCHBlockChunkCodecStream(CompressionMethod compression_method)
{
    return std::make_unique<CompressCHBlockChunkCodecStream>(compression_method);
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

[[maybe_unused]] static inline void DecodeColumns_by_block(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size)
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

[[maybe_unused]] static inline void DecodeColumns_by_col(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size)
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

    std::vector<size_t> column_batch;
    {
        size_t sz{};
        readVarUInt(sz, istr);
        column_batch.resize(sz);
        for (size_t i = 0; i < sz; ++i)
        {
            readVarUInt(column_batch[i], istr);
        }
        assert(std::accumulate(column_batch.begin(), column_batch.end(), 0, [](auto c, auto & e) { return c + e; }) == int(rows_to_read));
    }

    for (size_t i = 0; i < res.columns(); ++i)
    {
        for (const auto & sz : column_batch)
        {
            if (!sz)
                continue;
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

    res.setColumns(std::move(mutable_columns));
}

void DecodeColumns(ReadBuffer & istr, Block & res, size_t rows_to_read, size_t reserve_size)
{
    return DecodeColumns_by_block(istr, res, rows_to_read, reserve_size);
}

} // namespace DB