#include <DataTypes/DataTypeString.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>

namespace DB
{
TMTPKType getTMTPKType(const IDataType & rhs)
{
    static const DataTypeInt64 & dataTypeInt64 = {};
    static const DataTypeUInt64 & dataTypeUInt64 = {};
    static const DataTypeString & dataTypeString = {};

    if (rhs.equals(dataTypeInt64))
        return TMTPKType::INT64;
    else if (rhs.equals(dataTypeUInt64))
        return TMTPKType::UINT64;
    else if (rhs.equals(dataTypeString))
        return TMTPKType::STRING;
    return TMTPKType::UNSPECIFIED;
}

Block createBlockSortByColumnID(DecodingStorageSchemaSnapshotConstPtr schema_snapshot)
{
    Block block;
    for (auto iter = schema_snapshot->sorted_column_id_with_pos.begin(); iter != schema_snapshot->sorted_column_id_with_pos.end(); iter++)
    {
        auto col_id = iter->first;
        auto & cd = (*(schema_snapshot->column_defines))[iter->second];
        block.insert({cd.type->createColumn(), cd.type, cd.name, col_id});
    }
    return block;
}

void clearBlockData(Block & block)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        auto * raw_column = const_cast<IColumn *>((block.getByPosition(i)).column.get());
        raw_column->popBack(raw_column->size());
    }
}
} // namespace DB