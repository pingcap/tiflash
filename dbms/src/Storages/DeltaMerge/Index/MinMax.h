#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/FieldVisitors.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>

namespace DB
{

namespace DM
{

struct MIN
{
    static constexpr auto is_min = true;
    static constexpr auto name   = "min";
};

struct MAX
{
    static constexpr auto is_min = false;
    static constexpr auto name   = "max";
};

static constexpr size_t NONE_EXIST = std::numeric_limits<size_t>::max();

namespace details
{
inline std::pair<size_t, size_t> minmax(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
{
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    size_t batch_min_idx = NONE_EXIST;
    size_t batch_max_idx = NONE_EXIST;

    for (size_t i = offset; i < offset + limit; ++i)
    {
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

template <typename T>
inline std::pair<size_t, size_t> minmaxVec(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
{
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    size_t batch_min_idx = NONE_EXIST;
    size_t batch_max_idx = NONE_EXIST;

    auto & col_data = static_cast<const ColumnVector<T> &>(column).getData();
    for (size_t i = offset; i < offset + limit; ++i)
    {
        if (!del_mark_data || !(*del_mark_data)[i])
        {
            if (batch_min_idx == NONE_EXIST || col_data[i] < col_data[batch_min_idx])
                batch_min_idx = i;
            if (batch_max_idx == NONE_EXIST || col_data[batch_max_idx] < col_data[i])
                batch_max_idx = i;
        }
    }

    return {batch_min_idx, batch_max_idx};
}
} // namespace details

struct MinMaxValue;
using MinMaxValuePtr = std::shared_ptr<MinMaxValue>;

struct MinMaxValue : private boost::noncopyable
{
    bool has_value = false;

    explicit MinMaxValue() = default;
    explicit MinMaxValue(bool has_value_) : has_value(has_value_) {}

    virtual ~MinMaxValue() = default;

    virtual void           merge(const MinMaxValue & other)                 = 0;
    virtual MinMaxValuePtr clone()                                          = 0;
    virtual void           write(const IDataType & type, WriteBuffer & buf) = 0;


    virtual RSResult checkEqual(const Field & value, const DataTypePtr & type)        = 0;
    virtual RSResult checkGreater(const Field & value, const DataTypePtr & type)      = 0;
    virtual RSResult checkGreaterEqual(const Field & value, const DataTypePtr & type) = 0;

    virtual String toString() const = 0;
};

/// Number types.
template <typename T>
struct MinMaxValueFixed : public MinMaxValue
{
    T min;
    T max;

    MinMaxValueFixed() = default;
    MinMaxValueFixed(bool has_value_, T min_, T max_) : MinMaxValue(has_value_), min(min_), max(max_) {}
    MinMaxValueFixed(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        set(column, del_mark, offset, limit);
    }

    void set(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        auto [min_idx, max_idx] = details::minmaxVec<T>(column, del_mark, offset, limit);
        if (min_idx != NONE_EXIST)
        {
            auto & col_data = static_cast<const ColumnVector<T> &>(column).getData();

            has_value = true;
            min       = col_data[min_idx];
            max       = col_data[max_idx];
        }
    }

    void merge(const MinMaxValue & other) override
    {
        auto & o = static_cast<const MinMaxValueFixed<T> &>(other);
        if (!o.has_value)
            return;
        else if (!has_value)
        {
            min = o.min;
            max = o.max;
        }
        else
        {
            min = std::min(min, o.min);
            max = std::max(max, o.max);
        }
    }

    MinMaxValuePtr clone() override { return std::make_shared<MinMaxValueFixed>(has_value, min, max); }

    void write(const IDataType & type, WriteBuffer & buf) override
    {
        writePODBinary(has_value, buf);
        auto   col      = type.createColumn();
        auto & col_data = typeid_cast<ColumnVector<T> *>(col.get())->getData();
        col_data.push_back(min);
        col_data.push_back(max);
        type.serializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 0, 2, true, {});
    }

    static MinMaxValuePtr read(const IDataType & type, ReadBuffer & buf)
    {
        auto v = std::make_shared<MinMaxValueFixed<T>>();
        readPODBinary(v->has_value, buf);
        auto   col      = type.createColumn();
        auto & col_data = typeid_cast<ColumnVector<T> *>(col.get())->getData();
        type.deserializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 2, 0, true, {});
        v->min = col_data[0];
        v->max = col_data[1];
        return v;
    }

    // clang-format off
    RSResult checkEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkEqual(value, type, min, max); }
    RSResult checkGreater(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreater(value, type, min, max); }
    RSResult checkGreaterEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreaterEqual(value, type, min, max); }
    // clang-format on

    String toString() const override
    {
        std::stringstream ss;
        ss << "{\"type\":\"fixed\",\"min\":\"" << DB::toString(min) << "\",\"max\":\"" << DB::toString(max) << "\"}";
        return ss.str();
    }
};

/// String type only.
struct MinMaxValueString : public MinMaxValue
{
    std::string min;
    std::string max;

    MinMaxValueString() = default;
    MinMaxValueString(bool has_value_, const std::string & min_, const std::string & max_) : MinMaxValue(has_value_), min(min_), max(max_)
    {
    }
    MinMaxValueString(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        set(column, del_mark, offset, limit);
    }

    void set(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        auto [min_idx, max_idx] = details::minmax(column, del_mark, offset, limit);
        if (min_idx != NONE_EXIST)
        {
            auto & cast_column = static_cast<const ColumnString &>(column);

            has_value = true;
            min       = cast_column.getDataAt(min_idx).toString();
            max       = cast_column.getDataAt(max_idx).toString();
        }
    }

    void merge(const MinMaxValue & other) override
    {
        auto & o = static_cast<const MinMaxValueString &>(other);
        if (!o.has_value)
            return;
        else if (!has_value)
        {
            min = o.min;
            max = o.max;
        }
        else
        {
            min = std::min(min, o.min);
            max = std::max(max, o.max);
        }
    }

    MinMaxValuePtr clone() override { return std::make_shared<MinMaxValueString>(has_value, min, max); }

    void write(const IDataType & type, WriteBuffer & buf) override
    {
        writePODBinary(has_value, buf);
        auto col     = type.createColumn();
        auto str_col = typeid_cast<ColumnString *>(col.get());
        str_col->insertData(min.data(), min.size());
        str_col->insertData(max.data(), max.size());
        type.serializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 0, 2, true, {});
    }

    static MinMaxValuePtr read(const IDataType & type, ReadBuffer & buf)
    {
        auto v = std::make_shared<MinMaxValueString>();
        readPODBinary(v->has_value, buf);
        auto col     = type.createColumn();
        auto str_col = typeid_cast<ColumnString *>(col.get());
        type.deserializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 2, 0, true, {});
        v->min = str_col->getDataAt(0).toString();
        v->max = str_col->getDataAt(1).toString();
        return v;
    }

    // clang-format off
    RSResult checkEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkEqual(value, type, min, max); }
    RSResult checkGreater(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreater(value, type, min, max); }
    RSResult checkGreaterEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreaterEqual(value, type, min, max); }
    // clang-format on

    String toString() const override
    {
        std::stringstream ss;
        ss << "{\"type\":\"string\",\"min\":\"" << min << "\",\"max\":\"" << max << "\"}";
        return ss.str();
    }
};

/// Other types.
struct MinMaxValueDataGeneric : public MinMaxValue
{
    Field min;
    Field max;

    MinMaxValueDataGeneric() = default;
    MinMaxValueDataGeneric(bool has_value_, const Field & min_, const Field & max_) : MinMaxValue(has_value_), min(min_), max(max_) {}
    MinMaxValueDataGeneric(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        set(column, del_mark, offset, limit);
    }

    void set(const IColumn & column, const ColumnVector<UInt8> * del_mark, size_t offset, size_t limit)
    {
        auto [min_idx, max_idx] = details::minmax(column, del_mark, offset, limit);
        if (min_idx != NONE_EXIST)
        {
            has_value = true;
            column.get(min_idx, min);
            column.get(max_idx, max);
        }
    }

    void merge(const MinMaxValue & other) override
    {
        auto & o = static_cast<const MinMaxValueDataGeneric &>(other);
        if (!o.has_value)
            return;
        else if (!has_value)
        {
            min = o.min;
            max = o.max;
        }
        else
        {
            min = std::min(min, o.min);
            max = std::max(max, o.max);
        }
    }

    MinMaxValuePtr clone() override { return std::make_shared<MinMaxValueDataGeneric>(has_value, min, max); }

    void write(const IDataType & type, WriteBuffer & buf) override
    {
        writePODBinary(has_value, buf);
        auto col = type.createColumn();
        col->insert(min);
        col->insert(max);
        type.serializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 0, 2, true, {});
    }

    static MinMaxValuePtr read(const IDataType & type, ReadBuffer & buf)
    {
        auto v = std::make_shared<MinMaxValueDataGeneric>();
        readPODBinary(v->has_value, buf);
        auto col = type.createColumn();
        type.deserializeBinaryBulkWithMultipleStreams(*col, [&](const IDataType::SubstreamPath &) { return &buf; }, 2, 0, true, {});
        col->get(0, v->min);
        col->get(1, v->max);
        return v;
    }

    // clang-format off
    RSResult checkEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkEqual(value, type, min, max); }
    RSResult checkGreater(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreater(value, type, min, max); }
    RSResult checkGreaterEqual(const Field & value, const DataTypePtr & type) override { return RoughCheck::checkGreaterEqual(value, type, min, max); }
    // clang-format on

    String toString() const override
    {
        std::stringstream ss;
        ss << "{\"type\":\"generic\",\"min\":\"" << applyVisitor(FieldVisitorToString(), min) << "\",\"max\":\""
           << applyVisitor(FieldVisitorToString(), max) << "\"}";
        return ss.str();
    }
};


} // namespace DM

} // namespace DB
