

#include "CompressCHBlockChunkCodecStream.h"

#include <DataTypes/DataTypeFactory.h>

#include <cassert>
#include <cstddef>

namespace DB
{

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

extern void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

void EncodeColumn(WriteBuffer & ostr, const ColumnPtr & column, const ColumnWithTypeAndName & type_name)
{
    writeVarUInt(column->size(), ostr);
    writeData(*type_name.type, column, ostr, 0, 0);
}

std::unique_ptr<CompressCHBlockChunkCodecStream> NewCompressCHBlockChunkCodecStream(CompressionMethod compress_method)
{
    return std::make_unique<CompressCHBlockChunkCodecStream>(compress_method);
}

Block DecodeHeader(ReadBuffer & istr, const Block & header, size_t & rows)
{
    Block res;

    if (istr.eof())
    {
        return res;
    }

    size_t columns = 0;
    {
        readVarUInt(columns, istr);
        readVarUInt(rows, istr);
    }

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

void DecodeColumns(ReadBuffer & istr, Block & res, size_t columns, size_t rows, size_t reserve_size)
{
    if (!rows)
        return;

    auto && mutable_columns = res.mutateColumns();

    for (size_t i = 0; i < columns; ++i)
    {
        /// Data
        auto && read_column = mutable_columns[i];
        if (reserve_size > 0)
            read_column->reserve(std::max(rows, reserve_size) + read_column->size());
        else
            read_column->reserve(rows + read_column->size());

        size_t read_rows = 0;
        for (size_t sz = 0; read_rows < rows; read_rows += sz)
        {
            readVarUInt(sz, istr);
            if (!sz)
                continue;
            res.getByPosition(i).type->deserializeBinaryBulkWithMultipleStreams(
                *read_column,
                [&](const IDataType::SubstreamPath &) {
                    return &istr;
                },
                sz,
                0,
                {},
                {});
        }
        assert(read_rows == rows);
    }

    res.setColumns(std::move(mutable_columns));
}

} // namespace DB