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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>

#include <magic_enum.hpp>

namespace DB
{
static std::string extractTimeZoneNameFromColumn(const IColumn & column)
{
    const ColumnConst * time_zone_column = checkAndGetColumnConst<ColumnString>(&column);

    if (!time_zone_column)
        throw Exception(
            "Illegal column " + column.getName() + " of time zone argument of function, must be constant string",
            ErrorCodes::ILLEGAL_COLUMN);

    return time_zone_column->getValue<String>();
}


std::string extractTimeZoneNameFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments,
    size_t time_zone_arg_num,
    size_t datetime_arg_num)
{
    /// Explicit time zone may be passed in last argument.
    if (arguments.size() == time_zone_arg_num + 1 && arguments[time_zone_arg_num].column)
    {
        return extractTimeZoneNameFromColumn(*arguments[time_zone_arg_num].column);
    }
    else
    {
        if (arguments.empty())
            return {};

        /// If time zone is attached to an argument of type DateTime.
        if (const auto * type = checkAndGetDataType<DataTypeDateTime>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone().getTimeZone();

        return {};
    }
}

const DateLUTImpl & extractTimeZoneFromFunctionArguments(
    Block & block,
    const ColumnNumbers & arguments,
    size_t time_zone_arg_num,
    size_t datetime_arg_num)
{
    if (arguments.size() == time_zone_arg_num + 1)
        return DateLUT::instance(
            extractTimeZoneNameFromColumn(*block.getByPosition(arguments[time_zone_arg_num]).column));
    else
    {
        if (arguments.empty())
            return DateLUT::instance();

        /// If time zone is attached to an argument of type DateTime.
        if (const auto * type
            = checkAndGetDataType<DataTypeDateTime>(block.getByPosition(arguments[datetime_arg_num]).type.get()))
            return type->getTimeZone();

        return DateLUT::instance();
    }
}

class FunctionTiDBFromDays : public IFunction
{
public:
    static constexpr auto name = "tidbFromDays";

    explicit FunctionTiDBFromDays(const Context &) {}

    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTiDBFromDays>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = removeNullable(arguments[0]).get();
        if (!arg->isInteger())
            throw Exception(
                fmt::format("Illegal argument type {} of function {}, should be integer", arg->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMyDate>());
    }

    template <typename IntType>
    void dispatch(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        size_t rows = block.rows();

        auto col_to = ColumnVector<DataTypeMyDate::FieldType>::create(rows);
        auto & vec_to = col_to->getData();
        auto result_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & vec_result_null_map = result_null_map->getData();

        auto col_from = block.getByPosition(arguments[0]).column;
        if (block.getByPosition(arguments[0]).type->isNullable())
        {
            Block temporary_block = createBlockWithNestedColumns(block, arguments, result);
            col_from = temporary_block.getByPosition(arguments[0]).column;
        }
        const auto & vec_from = checkAndGetColumn<ColumnVector<IntType>>(col_from.get())->getData();

        for (size_t i = 0; i < rows; ++i)
        {
            try
            {
                IntType val = vec_from[i];
                MyDateTime date(0);
                if (val >= 0)
                {
                    fromDayNum(date, val);
                }
                vec_to[i] = date.toPackedUInt();
            }
            catch (const Exception &)
            {
                vec_result_null_map[i] = 1;
            }
        }

        if (block.getByPosition(arguments[0]).type->isNullable())
        {
            auto column = block.getByPosition(arguments[0]).column;
            for (size_t i = 0; i < rows; i++)
            {
                vec_result_null_map[i] |= column->isNullAt(i);
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(result_null_map));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (block.getByPosition(arguments[0]).type->onlyNull())
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        auto type_index = removeNullable(block.getByPosition(arguments[0]).type)->getTypeId();
        switch (type_index)
        {
        case TypeIndex::UInt8:
            dispatch<UInt8>(block, arguments, result);
            break;
        case TypeIndex::UInt16:
            dispatch<UInt16>(block, arguments, result);
            break;
        case TypeIndex::UInt32:
            dispatch<UInt32>(block, arguments, result);
            break;
        case TypeIndex::UInt64:
            dispatch<UInt64>(block, arguments, result);
            break;
        case TypeIndex::Int8:
            dispatch<Int8>(block, arguments, result);
            break;
        case TypeIndex::Int16:
            dispatch<Int16>(block, arguments, result);
            break;
        case TypeIndex::Int32:
            dispatch<Int32>(block, arguments, result);
            break;
        case TypeIndex::Int64:
            dispatch<Int64>(block, arguments, result);
            break;
        default:
            throw Exception(
                fmt::format(
                    "argument type of {} is invalid, expect integer, got {}",
                    getName(),
                    magic_enum::enum_name(type_index)),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        };
    }
};

void registerFunctionsDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMyTimeZoneConverter<true>>();
    factory.registerFunction<FunctionMyTimeZoneConverter<false>>();
    factory.registerFunction<FunctionMyTimeZoneConvertByOffset<true>>();
    factory.registerFunction<FunctionMyTimeZoneConvertByOffset<false>>();
    factory.registerFunction<FunctionToYear>();
    factory.registerFunction<FunctionToQuarter>();
    factory.registerFunction<FunctionToMonth>();
    factory.registerFunction<FunctionToDayOfMonth>();
    factory.registerFunction<FunctionToDayOfWeek>();
    factory.registerFunction<FunctionToDayOfYear>();
    factory.registerFunction<FunctionToHour>();
    factory.registerFunction<FunctionToMinute>();
    factory.registerFunction<FunctionToSecond>();
    factory.registerFunction<FunctionToStartOfDay>();
    factory.registerFunction<FunctionToMonday>();
    factory.registerFunction<FunctionToStartOfMonth>();
    factory.registerFunction<FunctionToStartOfQuarter>();
    factory.registerFunction<FunctionToStartOfYear>();
    factory.registerFunction<FunctionToStartOfMinute>();
    factory.registerFunction<FunctionToStartOfFiveMinute>();
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
    factory.registerFunction<FunctionToStartOfHour>();
    factory.registerFunction<FunctionToRelativeYearNum>();
    factory.registerFunction<FunctionToRelativeQuarterNum>();
    factory.registerFunction<FunctionToRelativeMonthNum>();
    factory.registerFunction<FunctionToRelativeWeekNum>();
    factory.registerFunction<FunctionToRelativeDayNum>();
    factory.registerFunction<FunctionToRelativeHourNum>();
    factory.registerFunction<FunctionToRelativeMinuteNum>();
    factory.registerFunction<FunctionToRelativeSecondNum>();
    factory.registerFunction<FunctionToTime>();
    factory.registerFunction<FunctionNow>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionToday>();
    factory.registerFunction<FunctionYesterday>();
    factory.registerFunction<FunctionTimeSlot>();
    factory.registerFunction<FunctionTimeSlots>();
    factory.registerFunction<FunctionToYYYYMM>();
    factory.registerFunction<FunctionToYYYYMMDD>();
    factory.registerFunction<FunctionToYYYYMMDDhhmmss>();
    factory.registerFunction<FunctionToDayName>();
    factory.registerFunction<FunctionToMonthName>();

    factory.registerFunction<FunctionAddSeconds>();
    factory.registerFunction<FunctionAddMinutes>();
    factory.registerFunction<FunctionAddHours>();
    factory.registerFunction<FunctionAddDays>();
    factory.registerFunction<FunctionAddWeeks>();
    factory.registerFunction<FunctionAddMonths>();
    factory.registerFunction<FunctionAddYears>();

    factory.registerFunction<FunctionSubtractSeconds>();
    factory.registerFunction<FunctionSubtractMinutes>();
    factory.registerFunction<FunctionSubtractHours>();
    factory.registerFunction<FunctionSubtractDays>();
    factory.registerFunction<FunctionSubtractWeeks>();
    factory.registerFunction<FunctionSubtractMonths>();
    factory.registerFunction<FunctionSubtractYears>();

    factory.registerFunction<FunctionSysDateWithFsp>();
    factory.registerFunction<FunctionSysDateWithoutFsp>();

    factory.registerFunction<FunctionDateDiff>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTiDBTimestampDiff>();
    factory.registerFunction<FunctionExtractMyDateTime>();
    factory.registerFunction<FunctionExtractMyDateTimeFromString>();
    factory.registerFunction<FunctionTiDBDateDiff>();
    factory.registerFunction<FunctionToTiDBDayOfWeek>();
    factory.registerFunction<FunctionToTiDBDayOfYear>();
    factory.registerFunction<FunctionToTiDBWeekOfYear>();
    factory.registerFunction<FunctionToTiDBToSeconds>();
    factory.registerFunction<FunctionToTiDBToDays>();
    factory.registerFunction<FunctionTiDBFromDays>();
    factory.registerFunction<FunctionToTimeZone>();
    factory.registerFunction<FunctionToLastDay>();
}

} // namespace DB
