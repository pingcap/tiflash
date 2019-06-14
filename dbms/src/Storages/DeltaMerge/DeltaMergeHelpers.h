#pragma once

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline Handle encodeToPK(T v)
{
    if constexpr (std::is_same_v<Int64, T>)
        return v;
    else if constexpr (std::is_same_v<UInt64, T>)
        return static_cast<Int64>(v);
    else
        return static_cast<Int64>(v);
}

inline size_t getPosByColumnId(const Block & block, ColId col_id)
{
    size_t pos = 0;
    for (auto & c : block)
    {
        if (c.column_id == col_id)
            return pos;
        ++pos;
    }
    throw Exception("Column with column id " + DB::toString(col_id) + " not found");
}

inline ColumnWithTypeAndName & getByColumnId(Block & block, ColId col_id)
{
    for (auto & c : block)
    {
        if (c.column_id == col_id)
            return c;
    }
    throw Exception("Column with column id " + DB::toString(col_id) + " not found");
}

// TODO: we should later optimize getByColumnId.
inline const ColumnWithTypeAndName & getByColumnId(const Block & block, ColId col_id)
{
    for (auto & c : block)
    {
        if (c.column_id == col_id)
            return c;
    }
    throw Exception("Column with column id " + DB::toString(col_id) + " not found");
}

inline ColumnWithTypeAndName createColumnWithTypeAndName(const ColumnPtr & column, const DataTypePtr & type, const String & name, ColId id)
{
    ColumnWithTypeAndName c;
    c.column    = column;
    c.type      = type;
    c.name      = name;
    c.column_id = id;
    return c;
}

inline SortDescription getPkSort(const ColumnDefine & handle)
{
    SortDescription sort;
    sort.emplace_back(handle.name, 1, 1);
    sort.emplace_back(VERSION_COLUMN_NAME, 1, 1);
    return sort;
}

using PermutationPtr = std::unique_ptr<IColumn::Permutation>;
inline PermutationPtr sortBlockByPk(const ColumnDefine & handle, Block & block)
{
    SortDescription sort = getPkSort(handle);
    if (isAlreadySorted(block, sort))
        return {};

    auto perm = std::make_unique<IColumn::Permutation>();
    stableGetPermutation(block, sort, *perm);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & c = block.getByPosition(i);
        c.column = c.column->permute(*perm, 0);
    }
    return perm;
}

template <typename T>
inline const PaddedPODArray<T> & getColumnVectorData(const Block & block, size_t pos)
{
    const ColumnVector<T> & c = typeid_cast<const ColumnVector<T> &>(*(block.getByPosition(pos).column));
    return c.getData();
}

template <typename T>
inline PaddedPODArray<T> const * getColumnVectorDataPtr(const Block & block, size_t pos)
{
    return &getColumnVectorData<T>(block, pos);
}

inline void addColumn(Block & block, ColId col_id, String col_name, const DataTypePtr & col_type, const ColumnPtr & col)
{
    ColumnWithTypeAndName column;
    column.column_id = col_id;
    column.name      = col_name;
    column.type      = col_type;
    column.column    = col;
    block.insert(column);
}

inline Block toEmptyBlock(const ColumnDefines & columns)
{
    Block block;
    for (auto & c : columns)
        addColumn(block, c.id, c.name, c.type, c.type->createColumn());
    return block;
}

} // namespace DB