#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsDuration.h>
#include <Functions/IFunction.h>
#include <fmt/format.h>

namespace DB
{
class FunctionConvertDurationFromInt64 : public IFunction
{
public:
    static constexpr auto name = "ConvertDurationFromInt64";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertDurationFromInt64>(); };
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 2",
                            getName(),
                            toString(arguments.size())),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (!arguments[0].type->isInteger())
        {
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", arguments[0].type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!arguments[1].type->isInteger() || !arguments[1].column->isColumnConst())
        {
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1].type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        auto fsp = arguments[1].column.get()->getInt(0);
        return std::make_shared<DataTypeMyDuration>(fsp);
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        block.getByPosition(result).column = std::move(block.getByPosition(arguments[0]).column);
    }
};

template <typename Impl>
class FunctionDurationSplit : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDurationSplit>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1",
                            getName(),
                            toString(arguments.size())),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (!arguments[0].type->isMyTime())
        {
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", arguments[0].type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * dur_type = checkAndGetDataType<DataTypeMyDuration>(block.getByPosition(arguments[0]).type.get());
        if (dur_type == nullptr)
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                                + " of first argument of function " + name,
                            ErrorCodes::ILLEGAL_COLUMN);
        }
        const auto * duration_col = checkAndGetColumn<ColumnVector<DataTypeMyDuration::FieldType>>(block.getByPosition(arguments[0]).column.get());
        if (duration_col != nullptr)
        {
            const typename ColumnVector<DataTypeMyDuration::FieldType>::Container & vec_duration = duration_col->getData();
            auto col_result = ColumnVector<Int64>::create();
            typename ColumnVector<Int64>::Container & vec_result = col_result->getData();
            size_t size = duration_col->size();
            vec_result.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                MyDuration dur(vec_duration[i], dur_type->getFsp());
                vec_result[i] = Impl::getResult(dur);
            }
            block.getByPosition(result).column = std::move(col_result);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                                + " of first argument of function " + name,
                            ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct DurationSplitHourImpl
{
    static constexpr auto name = "hour";
    static Int64 getResult(MyDuration & dur)
    {
        return dur.hours();
    }
};

struct DurationSplitMinuteImpl
{
    static constexpr auto name = "minute";
    static Int64 getResult(MyDuration & dur)
    {
        return dur.minutes();
    }
};
struct DurationSplitSecondImpl
{
    static constexpr auto name = "second";
    static Int64 getResult(MyDuration & dur)
    {
        return dur.seconds();
    }
};
struct DurationSplitMicroSecondImpl
{
    static constexpr auto name = "microSecond";
    static Int64 getResult(MyDuration & dur)
    {
        return dur.microsecond();
    }
};

using FunctionDurationHour = FunctionDurationSplit<DurationSplitHourImpl>;
using FunctionDurationMinute = FunctionDurationSplit<DurationSplitMinuteImpl>;
using FunctionDurationSecond = FunctionDurationSplit<DurationSplitSecondImpl>;
using FunctionDurationMicroSecond = FunctionDurationSplit<DurationSplitMicroSecondImpl>;

void registerFunctionsDuration(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConvertDurationFromInt64>();

    factory.registerFunction<FunctionDurationHour>();
    factory.registerFunction<FunctionDurationMinute>();
    factory.registerFunction<FunctionDurationSecond>();
    factory.registerFunction<FunctionDurationMicroSecond>();
}
} // namespace DB
