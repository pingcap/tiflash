#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

namespace DB
{
namespace DM
{

void MinMaxIndex::addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark)
{
    const IColumn * column_ptr = &column;
    auto            size       = column.size();
    bool            has_null   = false;
    if (column.isColumnNullable())
    {
        const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

        auto & nullable_column = static_cast<const ColumnNullable &>(column);
        auto & null_mark_data  = nullable_column.getNullMapColumn().getData();
        column_ptr             = &nullable_column.getNestedColumn();

        for (size_t i = 0; i < size; ++i)
        {
            if ((!del_mark_data || !(*del_mark_data)[i]) && null_mark_data[i])
            {
                has_null = true;
                break;
            }
        }
    }

    const IColumn & updated_column = *column_ptr;
    auto [min_index, max_index]    = details::minmax(updated_column, del_mark, 0, updated_column.size());
    if (min_index != NONE_EXIST)
    {
        has_null_marks->push_back(has_null);
        has_value_marks->push_back(1);
        minmaxes->insertFrom(updated_column, min_index);
        minmaxes->insertFrom(updated_column, max_index);
    }
    else
    {
        has_null_marks->push_back(has_null);
        has_value_marks->push_back(0);
        minmaxes->insertDefault();
        minmaxes->insertDefault();
    }
}

void MinMaxIndex::write(const IDataType & type, WriteBuffer & buf)
{
    UInt64 size = has_null_marks->size();
    DB::writeIntBinary(size, buf);
    buf.write((char *)has_null_marks->data(), sizeof(UInt8) * size);
    buf.write((char *)has_value_marks->data(), sizeof(UInt8) * size);
    type.serializeBinaryBulkWithMultipleStreams(*minmaxes, //
                                                [&](const IDataType::SubstreamPath &) { return &buf; },
                                                0,
                                                size * 2,
                                                true,
                                                {});
}

MinMaxIndexPtr MinMaxIndex::read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit)
{
    UInt64 size    = 0;
    size_t buf_pos = buf.count();
    if (bytes_limit != 0)
    {
        DB::readIntBinary(size, buf);
    }
    auto has_null_marks  = std::make_shared<PaddedPODArray<UInt8>>(size);
    auto has_value_marks = std::make_shared<PaddedPODArray<UInt8>>(size);
    auto minmaxes        = type.createColumn();
    buf.read((char *)has_null_marks->data(), sizeof(UInt8) * size);
    buf.read((char *)has_value_marks->data(), sizeof(UInt8) * size);
    type.deserializeBinaryBulkWithMultipleStreams(*minmaxes, //
                                                  [&](const IDataType::SubstreamPath &) { return &buf; },
                                                  size * 2,
                                                  0,
                                                  true,
                                                  {});
    size_t bytes_read = buf.count() - buf_pos;
    if (unlikely(bytes_read != bytes_limit))
    {
        throw DB::TiFlashException("Bad file format: expected read index content size: " + std::to_string(bytes_limit)
                                       + " vs. actual: " + std::to_string(bytes_read),
                                   Errors::DeltaTree::Internal);
    }
    return MinMaxIndexPtr(new MinMaxIndex(has_null_marks, has_value_marks, std::move(minmaxes)));
}

std::pair<Int64, Int64> MinMaxIndex::getIntMinMax(size_t pack_index)
{
    return {minmaxes->getInt(pack_index * 2), minmaxes->getInt(pack_index * 2 + 1)};
}

std::pair<StringRef, StringRef> MinMaxIndex::getStringMinMax(size_t pack_index)
{
    return {minmaxes->getDataAt(pack_index * 2), minmaxes->getDataAt(pack_index * 2 + 1)};
}

std::pair<UInt64, UInt64> MinMaxIndex::getUInt64MinMax(size_t pack_index)
{
    return {minmaxes->get64(pack_index * 2), minmaxes->get64(pack_index * 2 + 1)};
}

RSResult MinMaxIndex::checkEqual(size_t pack_id, const Field & value, const DataTypePtr & type)
{
    if ((*has_null_marks)[pack_id] || value.isNull())
        return RSResult::Some;
    if (!(*has_value_marks)[pack_id])
        return RSResult::None;

    auto raw_type = type.get();
#define DISPATCH(TYPE)                                              \
    if (typeid_cast<const DataType##TYPE *>(raw_type))              \
    {                                                               \
        auto & minmaxes_data = toColumnVectorData<TYPE>(minmaxes);  \
        auto   min           = minmaxes_data[pack_id * 2];          \
        auto   max           = minmaxes_data[pack_id * 2 + 1];      \
        return RoughCheck::checkEqual<TYPE>(value, type, min, max); \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDate::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkEqual<DataTypeDate::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDateTime::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkEqual<DataTypeDateTime::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        auto & minmaxes_data = toColumnVectorData<DataTypeMyTimeBase::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkEqual<DataTypeMyTimeBase::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        auto * string_column = checkAndGetColumn<ColumnString>(minmaxes.get());
        auto & chars         = string_column->getChars();
        auto & offsets       = string_column->getOffsets();
        size_t pos           = pack_id * 2;
        size_t prev_offset   = pos == 0 ? 0 : offsets[pos - 1];
        // todo use StringRef instead of String
        auto min    = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
        pos         = pack_id * 2 + 1;
        prev_offset = offsets[pos - 1];
        auto max    = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
        return RoughCheck::checkEqual<String>(value, type, min, max);
    }
    return RSResult::Some;
}
RSResult MinMaxIndex::checkGreater(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if ((*has_null_marks)[pack_id] || value.isNull())
        return RSResult::Some;
    if (!(*has_value_marks)[pack_id])
        return RSResult::None;

    auto raw_type = type.get();
#define DISPATCH(TYPE)                                                \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                \
    {                                                                 \
        auto & minmaxes_data = toColumnVectorData<TYPE>(minmaxes);    \
        auto   min           = minmaxes_data[pack_id * 2];            \
        auto   max           = minmaxes_data[pack_id * 2 + 1];        \
        return RoughCheck::checkGreater<TYPE>(value, type, min, max); \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDate::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreater<DataTypeDate::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDateTime::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreater<DataTypeDateTime::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        auto & minmaxes_data = toColumnVectorData<DataTypeMyTimeBase::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreater<DataTypeMyTimeBase::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        auto * string_column = checkAndGetColumn<ColumnString>(minmaxes.get());
        auto & chars         = string_column->getChars();
        auto & offsets       = string_column->getOffsets();
        size_t pos           = pack_id * 2;
        size_t prev_offset   = pos == 0 ? 0 : offsets[pos - 1];
        // todo use StringRef instead of String
        auto min    = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
        pos         = pack_id * 2 + 1;
        prev_offset = offsets[pos - 1];
        auto max    = String(chars[prev_offset], offsets[pos] - prev_offset - 1);
        return RoughCheck::checkGreater<String>(value, type, min, max);
    }
    return RSResult::Some;
}
RSResult MinMaxIndex::checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if ((*has_null_marks)[pack_id] || value.isNull())
        return RSResult::Some;
    if (!(*has_value_marks)[pack_id])
        return RSResult::None;

    auto raw_type = type.get();
#define DISPATCH(TYPE)                                                     \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                     \
    {                                                                      \
        auto & minmaxes_data = toColumnVectorData<TYPE>(minmaxes);         \
        auto   min           = minmaxes_data[pack_id * 2];                 \
        auto   max           = minmaxes_data[pack_id * 2 + 1];             \
        return RoughCheck::checkGreaterEqual<TYPE>(value, type, min, max); \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDate::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreaterEqual<DataTypeDate::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        auto & minmaxes_data = toColumnVectorData<DataTypeDateTime::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreaterEqual<DataTypeDateTime::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        auto & minmaxes_data = toColumnVectorData<DataTypeMyTimeBase::FieldType>(minmaxes);
        auto   min           = minmaxes_data[pack_id * 2];
        auto   max           = minmaxes_data[pack_id * 2 + 1];
        return RoughCheck::checkGreaterEqual<DataTypeMyTimeBase::FieldType>(value, type, min, max);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        auto * string_column = checkAndGetColumn<ColumnString>(minmaxes.get());
        auto & chars         = string_column->getChars();
        auto & offsets       = string_column->getOffsets();
        size_t pos           = pack_id * 2;
        size_t prev_offset   = pos == 0 ? 0 : offsets[pos - 1];
        // todo use StringRef instead of String
        auto min    = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
        pos         = pack_id * 2 + 1;
        prev_offset = offsets[pos - 1];
        auto max    = String(reinterpret_cast<const char *>(&chars[prev_offset]), offsets[pos] - prev_offset - 1);
        return RoughCheck::checkGreaterEqual<String>(value, type, min, max);
    }
    return RSResult::Some;
}

String MinMaxIndex::toString() const
{
    return "";
}

} // namespace DM
} // namespace DB
