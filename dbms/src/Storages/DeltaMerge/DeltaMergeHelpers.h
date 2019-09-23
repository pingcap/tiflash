#include <utility>

#pragma once

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

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
    const ColumnVector<T> & c = typeid_cast<const ColumnVector<T> &>(*(column));
    return &c.getData();
}

template <typename T>
inline PaddedPODArray<T> * toMutableColumnVectorDataPtr(const MutableColumnPtr & column)
{
    ColumnVector<T> & c = typeid_cast<ColumnVector<T> &>(*(column));
    return &c.getData();
}

template <typename T>
inline const PaddedPODArray<T> & toColumnVectorData(const ColumnPtr & column)
{
    const ColumnVector<T> & c = typeid_cast<const ColumnVector<T> &>(*(column));
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

inline void addColumnToBlock(Block & block, ColId col_id, const String &col_name, const DataTypePtr & col_type, const ColumnPtr & col)
{
    ColumnWithTypeAndName column(col, col_type, col_name, col_id);
    block.insert(std::move(column));
}

inline Block toEmptyBlock(const ColumnDefines & columns)
{
    Block block;
    for (auto & c : columns)
        addColumnToBlock(block, c.id, c.name, c.type, c.type->createColumn());
    return block;
}

inline void convertColumn(Block & block, size_t pos, const DataTypePtr & to_type, const Context & context)
{
    const IDataType * to_type_ptr = to_type.get();

    if (checkDataType<DataTypeUInt8>(to_type_ptr))
        FunctionToUInt8::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt16>(to_type_ptr))
        FunctionToUInt16::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt32>(to_type_ptr))
        FunctionToUInt32::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt64>(to_type_ptr))
        FunctionToUInt64::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt8>(to_type_ptr))
        FunctionToInt8::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt16>(to_type_ptr))
        FunctionToInt16::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt32>(to_type_ptr))
        FunctionToInt32::create(context)->execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt64>(to_type_ptr))
        FunctionToInt64::create(context)->execute(block, {pos}, pos);
    else
        throw Exception("Forgot to support type: " + to_type->getName());
}

inline void appendIntoHandleColumn(ColumnVector<Handle>::Container & handle_column, const DataTypePtr & type, const ColumnPtr & data)
{
    auto * type_ptr = &(*type);
    size_t size     = handle_column.size();

#define APPEND(SHIFT, MARK, DATA_VECTOR)           \
    for (size_t i = 0; i < size; ++i)              \
    {                                              \
        handle_column[i] <<= SHIFT;                \
        handle_column[i] |= MARK & DATA_VECTOR[i]; \
    }

    if (checkDataType<DataTypeUInt8>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt16>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt32>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt64>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeInt8>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeInt16>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt32>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt64>(type_ptr) || checkDataType<DataTypeDateTime>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeDate>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else
        throw Exception("Forgot to support type: " + type->getName());

#undef APPEND
}

inline void concat(Block & base, const Block & next)
{
    size_t next_rows = next.rows();
    for (size_t i = 0; i < base.columns(); ++i)
    {
        auto & col     = base.getByPosition(i).column;
        auto * col_raw = const_cast<IColumn *>(col.get());
        col_raw->insertRangeFrom((*next.getByPosition(i).column), 0, next_rows);
    }
}

inline size_t blockBytes(const Block & block)
{
    size_t bytes = 0;
    for (auto & c : block)
        bytes += c.column->byteSize();
    return bytes;
}

template <class T, bool right_open = true>
inline String rangeToString(T start, T end)
{
    String s = "[" + DB::toString(start) + "," + DB::toString(end);
    if constexpr (right_open)
        s += ")";
    else
        s += "]";
    return s;
}

template <typename T>
struct Range;

template <typename T>
inline String rangeToString(const Range<T> & range)
{
    return rangeToString<T, true>(range.start, range.end);
}

} // namespace DM
} // namespace DB