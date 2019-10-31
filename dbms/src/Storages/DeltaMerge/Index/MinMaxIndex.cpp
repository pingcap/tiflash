#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace DM
{

void MinMaxIndex::addChunk(const IColumn & column, const ColumnVector<UInt8> * del_mark)
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

    auto [min_index, max_index] = minmax(*column_ptr, del_mark, 0, column_ptr->size());
    if (min_index != NONE_EXIST)
    {
        has_null_marks->push_back(has_null);
        has_value_marks->push_back(1);
        minmaxes->insertFrom(*column_ptr, min_index);
        minmaxes->insertFrom(*column_ptr, max_index);
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

MinMaxIndexPtr MinMaxIndex::read(const IDataType & type, ReadBuffer & buf)
{
    UInt64 size;
    DB::readIntBinary(size, buf);
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
    return MinMaxIndexPtr(new MinMaxIndex(has_null_marks, has_value_marks, std::move(minmaxes)));
}

RSResult MinMaxIndex::checkEqual(size_t chunk_id, const Field & value, const DataTypePtr & type)
{
    if ((*has_null_marks)[chunk_id] || value.isNull())
        return Some;
    if (!(*has_value_marks)[chunk_id])
        return RSResult ::None;
    if (type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
    {
        // Currently only support handle.
        auto & minmaxes_data = toColumnVectorData<Handle>(minmaxes);
        auto   min           = minmaxes_data[chunk_id * 2];
        auto   max           = minmaxes_data[chunk_id * 2 + 1];
        Handle v             = value.get<Handle>();

        if (min == max && v == min)
            return RSResult::All;
        else if (v >= min && v <= max)
            return RSResult::Some;
        else
            return RSResult::None;
    }
    return RSResult::Some;
}
RSResult MinMaxIndex::checkGreater(size_t chunk_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if ((*has_null_marks)[chunk_id] || value.isNull())
        return Some;
    if (!(*has_value_marks)[chunk_id])
        return RSResult ::None;
    if (type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
    {
        // Currently only support handle.
        auto & minmaxes_data = toColumnVectorData<Handle>(minmaxes);
        auto   min           = minmaxes_data[chunk_id * 2];
        auto   max           = minmaxes_data[chunk_id * 2 + 1];
        Handle v             = value.get<Handle>();

        if (v >= max)
            return RSResult::None;
        else if (v < min)
            return RSResult::All;
        return RSResult::Some;
    }
    return RSResult::Some;
}
RSResult MinMaxIndex::checkGreaterEqual(size_t chunk_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if ((*has_null_marks)[chunk_id] || value.isNull())
        return Some;
    if (!(*has_value_marks)[chunk_id])
        return RSResult ::None;
    if (type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
    {
        // Currently only support handle.
        auto & minmaxes_data = toColumnVectorData<Handle>(minmaxes);
        auto   min           = minmaxes_data[chunk_id * 2];
        auto   max           = minmaxes_data[chunk_id * 2 + 1];
        Handle v             = value.get<Handle>();

        if (v > max)
            return RSResult::None;
        else if (v <= min)
            return RSResult::All;
        return RSResult::Some;
    }
    return RSResult::Some;
}

String MinMaxIndex::toString() const
{
    return "";
}

} // namespace DM
} // namespace DB