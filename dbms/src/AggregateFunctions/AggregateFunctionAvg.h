#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{


template <typename T>
struct AggregateFunctionAvgData
{
    T sum = 0;
    UInt64 count = 0;

    AggregateFunctionAvgData(){}

    AggregateFunctionAvgData(PrecType prec, ScaleType scale){
        sum = Decimal(0, prec, scale);
    }
};


/// Calculates arithmetic mean of numbers.
template <typename T>
class AggregateFunctionAvg final : public IAggregateFunctionDataHelper<AggregateFunctionAvgData<typename NearestFieldType<T>::Type>, AggregateFunctionAvg<T>>
{
    PrecType prec;
    ScaleType scale;
    PrecType result_prec;
    ScaleType result_scale;
public:
    AggregateFunctionAvg() {}
    AggregateFunctionAvg(PrecType prec_, ScaleType scale_) : prec(prec_), scale(scale_)
    {
        AvgDecimalInferer::infer(prec, scale, result_prec, result_scale);
    }
    String getName() const override { return "avg"; }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimal<T>)
            return std::make_shared<DataTypeDecimal>(result_prec, result_scale);
        else 
            return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).sum += static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
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
        if constexpr (IsDecimal<T>)
            static_cast<ColumnDecimal &>(to).getData().push_back(
                this->data(place).sum.getAvg(this->data(place).count, result_prec, result_scale));
        else
            static_cast<ColumnFloat64 &>(to).getData().push_back(
                static_cast<Float64>(this->data(place).sum) / this->data(place).count);
    }

    void create(AggregateDataPtr place) const override {
        using Data = AggregateFunctionAvgData<typename NearestFieldType<T>::Type>;
        if constexpr (IsDecimal<T>)
            new (place) Data(result_prec, result_scale);
        else
            new (place) Data;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


}
