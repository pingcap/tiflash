#include <utility>

#pragma once

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{
namespace DM
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

inline ColumnWithTypeAndName tryGetByColumnId(const Block & block, ColId col_id)
{
    for (auto & c : block)
    {
        if (c.column_id == col_id)
            return c;
    }
    return {};
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

inline SortDescription getPkSort(const ColumnDefine & handle)
{
    SortDescription sort;
    sort.emplace_back(handle.name, 1, 1);
    sort.emplace_back(VERSION_COLUMN_NAME, 1, 1);
    return sort;
}

inline bool sortBlockByPk(const ColumnDefine & handle, Block & block, IColumn::Permutation & perm)
{
    SortDescription sort = getPkSort(handle);
    if (isAlreadySorted(block, sort))
        return false;

    stableGetPermutation(block, sort, perm);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & c = block.getByPosition(i);
        c.column = c.column->permute(perm, 0);
    }
    return true;
}

template <typename T>
inline PaddedPODArray<T> const * toColumnVectorDataPtr(const ColumnPtr & column)
{
    if (column->isColumnConst())
    {
        auto * const_col = static_cast<const ColumnConst *>(column.get());

        const ColumnVector<T> & c = assert_cast<const ColumnVector<T> &>(const_col->getDataColumn());
        return &c.getData();
    }
    else
    {
        const ColumnVector<T> & c = assert_cast<const ColumnVector<T> &>(*(column));
        return &c.getData();
    }
}

template <typename T>
inline PaddedPODArray<T> * toMutableColumnVectorDataPtr(const MutableColumnPtr & column)
{
    ColumnVector<T> & c = assert_cast<ColumnVector<T> &>(*(column));
    return &c.getData();
}

template <typename T>
inline const PaddedPODArray<T> & toColumnVectorData(const IColumn & column)
{
    const ColumnVector<T> & c = assert_cast<const ColumnVector<T> &>(column);
    return c.getData();
}

template <typename T>
inline const PaddedPODArray<T> & toColumnVectorData(const ColumnPtr & column)
{
    const ColumnVector<T> & c = assert_cast<const ColumnVector<T> &>(*(column));
    return c.getData();
}

template <typename T>
inline const PaddedPODArray<T> & toColumnVectorData(const MutableColumnPtr & column)
{
    auto & c = assert_cast<ColumnVector<T> &>(*(column));
    return c.getData();
}

template <typename T>
inline const PaddedPODArray<T> & getColumnVectorData(const Block & block, size_t pos)
{
    return toColumnVectorData<T>(block.getByPosition(pos).column);
}

template <typename T>
inline PaddedPODArray<T> const * getColumnVectorDataPtr(const Block & block, size_t pos)
{
    return toColumnVectorDataPtr<T>(block.getByPosition(pos).column);
}

inline void addColumnToBlock(Block & block,
                             ColId col_id,
                             const String & col_name,
                             const DataTypePtr & col_type,
                             const ColumnPtr & col,
                             const Field & default_value = Field())
{
    ColumnWithTypeAndName column(col, col_type, col_name, col_id, default_value);
    block.insert(std::move(column));
}

/// Generate a block from column_defines
inline Block toEmptyBlock(const ColumnDefines & column_defines)
{
    Block block;
    for (const auto & c : column_defines)
    {
        /// Usually we use this function to get a header block,
        /// maybe columns of all nullptr in this block is enough?
        addColumnToBlock(block, c.id, c.name, c.type, c.type->createColumn(), c.default_value);
    }
    return block;
}

/// Generate a block from column_defines
inline Block genBlock(const ColumnDefines & column_defines, const Columns & columns)
{
    if (unlikely(column_defines.size() != columns.size()))
        throw Exception("column_defines and columns have different size: " + DB::toString(column_defines.size()) + ", "
                        + DB::toString(columns.size()));

    Block block;
    for (size_t i = 0; i < column_defines.size(); ++i)
    {
        auto & c = column_defines[i];
        addColumnToBlock(block, c.id, c.name, c.type, columns[i], c.default_value);
    }
    return block;
}

inline Block getNewBlockByHeader(const Block & header, const Block & block)
{
    Block new_block;
    for (auto & c : header)
        new_block.insert(block.getByName(c.name));
    return new_block;
}

inline ColumnDefines getColumnDefinesFromBlock(const Block & block)
{
    ColumnDefines columns;
    for (const auto & c : block)
        columns.emplace_back(ColumnDefine{c.column_id, c.name, c.type, c.default_value});
    return columns;
}

inline bool hasColumn(const ColumnDefines & columns, const ColId & col_id)
{
    for (auto & c : columns)
    {
        if (c.id == col_id)
            return true;
    }
    return false;
}

template <bool check_default_value = false>
inline bool isSameSchema(const Block & a, const Block & b)
{
    if (a.columns() != b.columns())
        return false;
    for (size_t i = 0; i < a.columns(); ++i)
    {
        auto & ca = a.getByPosition(i);
        auto & cb = b.getByPosition(i);

        bool col_ok = ca.column_id == cb.column_id;
        bool name_ok = ca.name == cb.name;
        bool type_ok = ca.type->equals(*(cb.type));
        bool value_ok = !check_default_value || ca.default_value == cb.default_value;

        if (!col_ok || !name_ok || !type_ok || !value_ok)
            return false;
    }
    return true;
}

/// This method guarantees that the returned valid block is not empty.
inline Block readNextBlock(const BlockInputStreamPtr & in)
{
    while (true)
    {
        Block res = in->read();
        if (!res)
            return Block{};
        if (!res.rows())
            continue;
        return res;
    }
}

void convertColumn(Block & block, size_t pos, const DataTypePtr & to_type, const Context & context);
void appendIntoHandleColumn(ColumnVector<Handle>::Container & handle_column, const DataTypePtr & type, const ColumnPtr & data);

inline void concat(Block & base, const Block & next)
{
    size_t next_rows = next.rows();
    for (size_t i = 0; i < base.columns(); ++i)
    {
        auto & col = base.getByPosition(i).column;
        auto * col_raw = const_cast<IColumn *>(col.get());
        col_raw->insertRangeFrom((*next.getByPosition(i).column), 0, next_rows);
    }
}

/// acc_seq is the accumulative sequence, offset is the pos we need to locate.
/// Returns <item index, the offset inside item>
inline std::pair<size_t, size_t> locatePosByAccumulation(const std::vector<size_t> & acc_seq, size_t offset)
{
    auto it_begin = acc_seq.begin();
    auto it = std::upper_bound(it_begin, acc_seq.end(), offset);
    if (it == acc_seq.end())
        return {acc_seq.size(), 0};
    else
    {
        auto item_offset = it == it_begin ? 0 : *(it - 1);
        return {it - it_begin, offset - item_offset};
    }
}

} // namespace DM
} // namespace DB
