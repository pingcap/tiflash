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

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>

namespace DB::DM
{

static constexpr size_t NONE_EXIST = std::numeric_limits<size_t>::max();

namespace details
{
inline std::pair<size_t, size_t> minmax(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    size_t offset,
    size_t limit)
{
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    size_t batch_min_idx = NONE_EXIST;
    size_t batch_max_idx = NONE_EXIST;

    for (size_t i = offset; i < offset + limit; ++i)
    {
        // del_mark_data == nullptr || (del_mark_data != nullptr && (*del_mark_data)[i] != 0)
        if (!del_mark_data || !(*del_mark_data)[i])
        {
            if (batch_min_idx == NONE_EXIST || column.compareAt(i, batch_min_idx, column, -1) < 0)
                batch_min_idx = i;
            if (batch_max_idx == NONE_EXIST || column.compareAt(batch_max_idx, i, column, -1) < 0)
                batch_max_idx = i;
        }
    }

    return {batch_min_idx, batch_max_idx};
}

// Calculate the min max value only for the not null values and not deleted values in the nullable column.
inline std::pair<size_t, size_t> minmax(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    const PaddedPODArray<UInt8> & null_mark,
    size_t offset,
    size_t limit)
{
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    size_t batch_min_idx = NONE_EXIST;
    size_t batch_max_idx = NONE_EXIST;

    for (size_t i = offset; i < offset + limit; ++i)
    {
        // (del_mark_data == nullptr || (del_mark_data != nullptr && (*del_mark_data)[i] != 0)) && (null_mark[i] == false)
        if ((!del_mark_data || !(*del_mark_data)[i]) && (!null_mark[i]))
        {
            if (batch_min_idx == NONE_EXIST || column.compareAt(i, batch_min_idx, column, -1) < 0)
                batch_min_idx = i;
            if (batch_max_idx == NONE_EXIST || column.compareAt(batch_max_idx, i, column, -1) < 0)
                batch_max_idx = i;
        }
    }

    return {batch_min_idx, batch_max_idx};
}

// Before v6.4.0, we used null as the minimum value.
// Since v6.4.0, we have excluded null when calculating the maximum and minimum values.
// If the minimum value is null, this minmax index is generated before v6.4.0.
// For compatibility, the filter result of the corresponding pack should be Some,
// and the upper layer will read the pack data to perform the filter calculation.
//
// TODO: avoid hitting this compatibility check when all the fields of a pack are null or deleted.
ALWAYS_INLINE bool minIsNull(const DB::ColumnUInt8 & null_map, size_t i)
{
    return null_map.getElement(i * 2);
}
} // namespace details

void MinMaxIndex::addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark)
{
    auto size = column.size();
    bool has_null = false;

    size_t min_index;
    size_t max_index;


    if (column.isColumnNullable())
    {
        const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

        const auto & nullable_column = static_cast<const ColumnNullable &>(column);
        const auto & null_mark_data = nullable_column.getNullMapColumn().getData();

        for (size_t i = 0; i < size; ++i)
        {
            if ((!del_mark_data || !(*del_mark_data)[i]) && null_mark_data[i])
            {
                has_null = true;
                break;
            }
        }

        if (has_null)
        {
            std::tie(min_index, max_index) = details::minmax(column, del_mark, null_mark_data, 0, column.size());
        }
    }

    if (!has_null)
    {
        std::tie(min_index, max_index) = details::minmax(column, del_mark, 0, column.size());
    }

    if (min_index != NONE_EXIST)
    {
        has_null_marks.push_back(has_null);
        has_value_marks.push_back(1);
        minmaxes->insertFrom(column, min_index);
        minmaxes->insertFrom(column, max_index);
    }
    else
    {
        has_null_marks.push_back(has_null);
        has_value_marks.push_back(0);
        minmaxes->insertDefault();
        minmaxes->insertDefault();
    }
}

void MinMaxIndex::write(const IDataType & type, WriteBuffer & buf)
{
    UInt64 size = has_null_marks.size();
    DB::writeIntBinary(size, buf);
    buf.write(reinterpret_cast<const char *>(has_null_marks.data()), sizeof(UInt8) * size);
    buf.write(reinterpret_cast<const char *>(has_value_marks.data()), sizeof(UInt8) * size);
    type.serializeBinaryBulkWithMultipleStreams(
        *minmaxes, //
        [&](const IDataType::SubstreamPath &) { return &buf; },
        0,
        size * 2,
        true,
        {});
}

MinMaxIndexPtr MinMaxIndex::read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit)
{
    UInt64 size = 0;
    size_t buf_pos = buf.count();
    if (bytes_limit != 0)
    {
        DB::readIntBinary(size, buf);
    }
    PaddedPODArray<UInt8> has_null_marks(size);
    PaddedPODArray<UInt8> has_value_marks(size);
    auto minmaxes = type.createColumn();
    buf.read(reinterpret_cast<char *>(has_null_marks.data()), sizeof(UInt8) * size);
    buf.read(reinterpret_cast<char *>(has_value_marks.data()), sizeof(UInt8) * size);
    type.deserializeBinaryBulkWithMultipleStreams(
        *minmaxes, //
        [&](const IDataType::SubstreamPath &) { return &buf; },
        size * 2,
        0,
        true,
        {});
    size_t bytes_read = buf.count() - buf_pos;
    if (unlikely(bytes_read != bytes_limit))
    {
        throw DB::TiFlashException(
            "Bad file format: expected read index content size: " + std::to_string(bytes_limit)
                + " vs. actual: " + std::to_string(bytes_read),
            Errors::DeltaTree::Internal);
    }
    return std::make_shared<MinMaxIndex>(std::move(has_null_marks), std::move(has_value_marks), std::move(minmaxes));
}

std::pair<Int64, Int64> MinMaxIndex::getIntMinMax(size_t pack_index)
{
    return {minmaxes->getInt(pack_index * 2), minmaxes->getInt(pack_index * 2 + 1)};
}

std::pair<std::string, std::string> MinMaxIndex::getIntMinMaxOrNull(size_t pack_index)
{
    std::string min, max;
    Field min_field, max_field;
    minmaxes->get(pack_index * 2, min_field);
    minmaxes->get(pack_index * 2 + 1, max_field);
    min = min_field.isNull() ? "null" : min_field.toString();
    max = max_field.isNull() ? "null" : max_field.toString();
    return {min, max};
}

std::pair<StringRef, StringRef> MinMaxIndex::getStringMinMax(size_t pack_index)
{
    return {minmaxes->getDataAt(pack_index * 2), minmaxes->getDataAt(pack_index * 2 + 1)};
}

std::pair<UInt64, UInt64> MinMaxIndex::getUInt64MinMax(size_t pack_index)
{
    return {minmaxes->get64(pack_index * 2), minmaxes->get64(pack_index * 2 + 1)};
}

template <typename T>
RSResults MinMaxIndex::checkNullableInImpl(
    const DB::ColumnNullable & column_nullable,
    const DB::ColumnUInt8 & null_map,
    size_t start_pack,
    size_t pack_count,
    const std::vector<Field> & values,
    const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::SomeNull);
    const auto & minmaxes_data = toColumnVectorData<T>(column_nullable.getNestedColumnPtr());
    for (size_t i = start_pack; i < start_pack + pack_count; ++i)
    {
        if (details::minIsNull(null_map, i))
            continue;
        auto min = minmaxes_data[i * 2];
        auto max = minmaxes_data[i * 2 + 1];
        auto value_result = RoughCheck::CheckIn::check<T>(values, type, min, max);
        results[i - start_pack] = addNullIfHasNull(value_result, i);
    }
    return results;
}

RSResults MinMaxIndex::checkNullableIn(
    size_t start_pack,
    size_t pack_count,
    const std::vector<Field> & values,
    const DataTypePtr & type)
{
    const auto & column_nullable = static_cast<const ColumnNullable &>(*minmaxes);
    const auto & null_map = column_nullable.getNullMapColumn();

    RSResults results(pack_count, RSResult::SomeNull);
    const auto * raw_type = type.get();

#define DISPATCH(TYPE)                                 \
    if (typeid_cast<const DataType##TYPE *>(raw_type)) \
        return checkNullableInImpl<TYPE>(column_nullable, null_map, start_pack, pack_count, values, type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        return checkNullableInImpl<DataTypeMyTimeBase::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            values,
            type);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        const auto * string_column = checkAndGetColumn<ColumnString>(column_nullable.getNestedColumnPtr().get());
        const auto & chars = string_column->getChars();
        const auto & offsets = string_column->getOffsets();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (details::minIsNull(null_map, i))
                continue;
            size_t pos = i * 2;
            size_t prev_offset = pos == 0 ? 0 : offsets[pos - 1];
            // todo use StringRef instead of String
            auto min = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
            pos = i * 2 + 1;
            prev_offset = offsets[pos - 1];
            auto max = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
            auto value_result = RoughCheck::CheckIn::check<String>(values, type, min, max);
            results[i - start_pack] = addNullIfHasNull(value_result, i);
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        return checkNullableInImpl<DataTypeDate::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            values,
            type);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        return checkNullableInImpl<DataTypeDateTime::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            values,
            type);
    }
    return results;
}

template <typename T>
RSResults MinMaxIndex::checkInImpl(
    size_t start_pack,
    size_t pack_count,
    const std::vector<Field> & values,
    const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::None);
    const auto & minmaxes_data = toColumnVectorData<T>(minmaxes);
    for (size_t i = start_pack; i < start_pack + pack_count; ++i)
    {
        if (!has_value_marks[i])
            continue;
        auto min = minmaxes_data[i * 2];
        auto max = minmaxes_data[i * 2 + 1];
        auto value_result = RoughCheck::CheckIn::check<T>(values, type, min, max);
        results[i - start_pack] = addNullIfHasNull(value_result, i);
    }
    return results;
}

RSResults MinMaxIndex::checkIn(
    size_t start_pack,
    size_t pack_count,
    const std::vector<Field> & values,
    const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::None);

    const auto * raw_type = type.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
    {
        return checkNullableIn(start_pack, pack_count, values, removeNullable(type));
    }
#define DISPATCH(TYPE)                                 \
    if (typeid_cast<const DataType##TYPE *>(raw_type)) \
        return checkInImpl<TYPE>(start_pack, pack_count, values, type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        return checkInImpl<DataTypeMyTimeBase::FieldType>(start_pack, pack_count, values, type);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        const auto * string_column = checkAndGetColumn<ColumnString>(minmaxes.get());
        const auto & chars = string_column->getChars();
        const auto & offsets = string_column->getOffsets();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (!has_value_marks[i])
                continue;
            size_t pos = i * 2;
            size_t prev_offset = pos == 0 ? 0 : offsets[pos - 1];
            // todo use StringRef instead of String
            // When using String, we should use reinterpret_cast<const char *>(&chars[prev_offset]) instead of chars[prev_offset]
            // so that it will call constructor `constexpr basic_string( const CharT* s, const Allocator& alloc = Allocator())`
            // rather than `constexpr basic_string( size_type count, CharT ch, const Allocator& alloc = Allocator() );`
            auto min = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
            pos = i * 2 + 1;
            prev_offset = offsets[pos - 1];
            auto max = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
            auto value_result = RoughCheck::CheckIn::check<String>(values, type, min, max);
            results[i - start_pack] = addNullIfHasNull(value_result, i);
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
        return checkInImpl<DataTypeDate::FieldType>(start_pack, pack_count, values, type);
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
        return checkInImpl<DataTypeDateTime::FieldType>(start_pack, pack_count, values, type);
    return RSResults(pack_count, RSResult::Some);
}

template <typename Op, typename T>
RSResults MinMaxIndex::checkCmpImpl(size_t start_pack, size_t pack_count, const Field & value, const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::None);
    const auto & minmaxes_data = toColumnVectorData<T>(minmaxes);
    for (size_t i = start_pack; i < start_pack + pack_count; ++i)
    {
        if (!has_value_marks[i])
            continue;
        auto min = minmaxes_data[i * 2];
        auto max = minmaxes_data[i * 2 + 1];
        auto value_result = Op::template check<T>(value, type, min, max);
        results[i - start_pack] = addNullIfHasNull(value_result, i);
    }
    return results;
}

template <typename Op>
RSResults MinMaxIndex::checkCmp(size_t start_pack, size_t pack_count, const Field & value, const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::None);
    if (value.isNull())
        return results;

    const auto * raw_type = type.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
        return checkNullableCmp<Op>(start_pack, pack_count, value, removeNullable(type));

#define DISPATCH(TYPE)                                 \
    if (typeid_cast<const DataType##TYPE *>(raw_type)) \
        return checkCmpImpl<Op, TYPE>(start_pack, pack_count, value, type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        return checkCmpImpl<Op, DataTypeMyTimeBase::FieldType>(start_pack, pack_count, value, type);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        const auto * string_column = checkAndGetColumn<ColumnString>(minmaxes.get());
        const auto & chars = string_column->getChars();
        const auto & offsets = string_column->getOffsets();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (!has_value_marks[i])
                continue;
            size_t pos = i * 2;
            size_t prev_offset = pos == 0 ? 0 : offsets[pos - 1];
            // todo use StringRef instead of String
            // When using String, we should use reinterpret_cast<const char *>(&chars[prev_offset]) instead of chars[prev_offset]
            // so that it will call constructor `constexpr basic_string( const CharT* s, const Allocator& alloc = Allocator())`
            // rather than `constexpr basic_string( size_type count, CharT ch, const Allocator& alloc = Allocator() );`
            auto min = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
            pos = i * 2 + 1;
            prev_offset = offsets[pos - 1];
            auto max = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
            auto value_result = Op::template check<String>(value, type, min, max);
            results[i - start_pack] = addNullIfHasNull(value_result, i);
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
        return checkCmpImpl<Op, DataTypeDate::FieldType>(start_pack, pack_count, value, type);
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
        return checkCmpImpl<Op, DataTypeDateTime::FieldType>(start_pack, pack_count, value, type);
    return RSResults(pack_count, RSResult::Some);
}

template RSResults MinMaxIndex::checkCmp<RoughCheck::CheckEqual>(
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type);
template RSResults MinMaxIndex::checkCmp<RoughCheck::CheckGreater>(
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type);
template RSResults MinMaxIndex::checkCmp<RoughCheck::CheckGreaterEqual>(
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type);

template <typename Op, typename T>
RSResults MinMaxIndex::checkNullableCmpImpl(
    const DB::ColumnNullable & column_nullable,
    const DB::ColumnUInt8 & null_map,
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type)
{
    RSResults results(pack_count, RSResult::SomeNull);
    const auto & minmaxes_data = toColumnVectorData<T>(column_nullable.getNestedColumnPtr());
    for (size_t i = start_pack; i < start_pack + pack_count; ++i)
    {
        if (details::minIsNull(null_map, i))
            continue;
        auto min = minmaxes_data[i * 2];
        auto max = minmaxes_data[i * 2 + 1];
        auto value_result = Op::template check<T>(value, type, min, max);
        results[i - start_pack] = addNullIfHasNull(value_result, i);
    }
    return results;
}

template <typename Op>
RSResults MinMaxIndex::checkNullableCmp(
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type)
{
    const auto & column_nullable = static_cast<const ColumnNullable &>(*minmaxes);
    const auto & null_map = column_nullable.getNullMapColumn();

    RSResults results(pack_count, RSResult::SomeNull);
    const auto * raw_type = type.get();

#define DISPATCH(TYPE)                                 \
    if (typeid_cast<const DataType##TYPE *>(raw_type)) \
        return checkNullableCmpImpl<Op, TYPE>(column_nullable, null_map, start_pack, pack_count, value, type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        return checkNullableCmpImpl<Op, DataTypeMyTimeBase::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            value,
            type);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        const auto * string_column = checkAndGetColumn<ColumnString>(column_nullable.getNestedColumnPtr().get());
        const auto & chars = string_column->getChars();
        const auto & offsets = string_column->getOffsets();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (details::minIsNull(null_map, i))
                continue;
            size_t pos = i * 2;
            size_t prev_offset = pos == 0 ? 0 : offsets[pos - 1];
            // todo use StringRef instead of String
            auto min = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
            pos = i * 2 + 1;
            prev_offset = offsets[pos - 1];
            auto max = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
            auto value_result = Op::template check<String>(value, type, min, max);
            results[i - start_pack] = addNullIfHasNull(value_result, i);
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        return checkNullableCmpImpl<Op, DataTypeDate::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            value,
            type);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        return checkNullableCmpImpl<Op, DataTypeDateTime::FieldType>(
            column_nullable,
            null_map,
            start_pack,
            pack_count,
            value,
            type);
    }
    return results;
}

// If a pack only contains null marks and delete marks, checkIsNull will return RSResult::All.
// This is safe because MVCC will read the tag column and the deleted rows will be filtered out.
RSResults MinMaxIndex::checkIsNull(size_t start_pack, size_t pack_count)
{
    RSResults results(pack_count, RSResult::None);
    for (size_t i = start_pack; i < start_pack + pack_count; ++i)
    {
        if (has_null_marks[i])
        {
            results[i - start_pack] = has_value_marks[i] ? RSResult::Some : RSResult::All;
        }
    }
    return results;
}

RSResult MinMaxIndex::addNullIfHasNull(RSResult value_result, size_t i) const
{
    return has_null_marks[i] ? addNull(value_result) : value_result;
}
} // namespace DB::DM
