#include <Flash/Coprocessor/TiDBChunk.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/ArrowColCodec.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

TiDBChunk::TiDBChunk(const std::vector<tipb::FieldType> & field_types)
{
    for (auto & type : field_types)
    {
        columns.emplace_back(getFieldLengthForArrowEncode(type.tp()));
    }
}

void TiDBChunk::buildDAGChunkFromBlock(
    const Block & block, const std::vector<tipb::FieldType> & field_types, size_t start_index, size_t end_index)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        flashColToArrowCol(columns[i], block.getByPosition(i), field_types[i], start_index, end_index);
    }
}

} // namespace DB
