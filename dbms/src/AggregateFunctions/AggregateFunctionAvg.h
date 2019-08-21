#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{


template <typename T>
struct AggregateFunctionAvgData
{
    T sum;
    UInt64 count;

    AggregateFunctionAvgData() : sum(0), count(0) {}
};


/// Calculates arithmetic mean of numbers.
template <typename T, typename TResult = Float64>
class AggregateFunctionAvg final
    : public IAggregateFunctionDataHelper<
          AggregateFunctionAvgData<std::conditional_t<IsDecimal<T>, TResult, typename NearestFieldType<T>::Type>>,
          AggregateFunctionAvg<T, TResult>>
{
    PrecType prec;
    ScaleType scale;
    PrecType result_prec;
    ScaleType result_scale;

public:
    AggregateFunctionAvg() {}
    AggregateFunctionAvg(PrecType prec_, ScaleType scale_, PrecType result_prec_, ScaleType result_scale_)
        : prec(prec_), scale(scale_), result_prec(result_prec_), result_scale(result_scale_)
    {}
    String getName() const override { return "avg"; }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimal<T>)
            return std::make_shared<DataTypeDecimal<TResult>>(result_prec, result_scale);
        else
            return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (IsDecimal<T>)
            this->data(place).sum += static_cast<const ColumnDecimal<T> &>(*columns[0]).getData()[row_num];
        else
        {
            this->data(place).sum += static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        }
        ++this->data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum, buf);
        writeVarUInt(this->data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum, buf);
        readVarUInt(this->data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        if constexpr (IsDecimal<TResult>)
        {
            ScaleType left_scale = result_scale - scale;
            TResult result = this->data(place).sum.value * getScaleMultiplier<TResult>(left_scale)
                / static_cast<typename TResult::NativeType>(this->data(place).count);
            static_cast<ColumnDecimal<TResult> &>(to).getData().push_back(result);
        }
        else
        {
            static_cast<ColumnFloat64 &>(to).getData().push_back(static_cast<Float64>(this->data(place).sum) / this->data(place).count);
        }
    }

    void create(AggregateDataPtr place) const override
    {
        using Data = AggregateFunctionAvgData<std::conditional_t<IsDecimal<T>, TResult, typename NearestFieldType<T>::Type>>;
        new (place) Data;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
