#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>

namespace DB
{
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
}