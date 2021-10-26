#pragma once

#include <Common/MyDuration.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <common/DateLUT.h>

#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionConvertDurationFromNanos : public IFunction
{
public:
    static constexpr auto name = "FunctionConvertDurationFromNanos";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertDurationFromNanos>(); };
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
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
        return dur.microSecond();
    }
};

} // namespace DB