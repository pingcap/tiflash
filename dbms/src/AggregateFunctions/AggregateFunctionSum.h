#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

template <typename T>
struct AggregateFunctionSumData
{
    T sum{};

    AggregateFunctionSumData(){}

    void add(T value)
    {
        sum += value;
    }

    template <typename U>
    void add(Decimal<U> v [[maybe_unused]]) {
        if constexpr(IsDecimal<T>)
            sum.value += static_cast<typename T::NativeType>(v.value);
    }

    void merge(const AggregateFunctionSumData & rhs)
    {
        sum += rhs.sum;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
    }

    T get() const
    {
        return sum;
    }
};

template <typename T>
struct AggregateFunctionSumKahanData
{
    static_assert(std::is_floating_point_v<T>,
        "It doesn't make sense to use Kahan Summation algorithm for non floating point types");

    T sum{};
    T compensation{};

    void add(T value)
    {
        auto compensated_value = value - compensation;
        auto new_sum = sum + compensated_value;
        compensation = (new_sum - sum) - compensated_value;
        sum = new_sum;
    }

    void merge(const AggregateFunctionSumKahanData & rhs)
    {
        auto raw_sum = sum + rhs.sum;
        auto rhs_compensated = raw_sum - sum;
        auto compensations = ((rhs.sum - rhs_compensated) + (sum - (raw_sum - rhs_compensated))) + compensation + rhs.compensation;
        sum = raw_sum + compensations;
        compensation = compensations - (sum - raw_sum);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
        writeBinary(compensation, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
        readBinary(compensation, buf);
    }

    T get() const
    {
        return sum;
    }
};


struct NameSum                { static constexpr auto name = "sum"; };
struct NameCountSecondStage                { static constexpr auto name = "countSecondStage"; };
extern const String CountSecondStage;

/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data, typename Name = NameSum>
class AggregateFunctionSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Name>>
{
public:
    String getName() const override { return Name::name; }

    ScaleType result_scale;
    PrecType result_prec;

    AggregateFunctionSum(){}

    AggregateFunctionSum(PrecType prec, ScaleType scale) {
        SumDecimalInferer::infer(prec, scale, result_prec, result_scale);
    };

    DataTypePtr getReturnType() const override {
        if constexpr (IsDecimal<TResult>) {
            return std::make_shared<DataTypeDecimal<TResult>>(result_prec, result_scale);
        } else {
            return std::make_shared<DataTypeNumber<TResult>>();
        }
    }

    void create(AggregateDataPtr place) const override {
        new (place) Data;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (IsDecimal<T>)
            this->data(place).template add<T>(static_cast<const ColumnDecimal<T> &>(*columns[0]).getData()[row_num]);
        else
            this->data(place).add(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        if constexpr (IsDecimal<TResult>) {
            static_cast<ColumnDecimal<TResult> &>(to).getData().push_back(this->data(place).get(), result_scale);
        } else
            static_cast<ColumnVector<TResult> &>(to).getData().push_back(this->data(place).get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
