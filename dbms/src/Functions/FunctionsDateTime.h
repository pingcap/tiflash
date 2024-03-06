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

#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/MyDuration.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>
#include <common/DateLUT.h>
#include <fmt/core.h>

#include <type_traits>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
static constexpr Int64 E6 = 1000000;
} // namespace

/** Functions for working with date and time.
  *
  * toYear, toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond,
  * toMonday, toStartOfMonth, toStartOfYear, toStartOfMinute, toStartOfFiveMinute, toStartOfFifteenMinutes
  * toStartOfHour, toTime,
  * now, today, yesterday
  * TODO: makeDate, makeDateTime
  *
  * (toDate - located in FunctionConversion.h file)
  *
  * Return types:
  *  toYear -> UInt16
  *  toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond -> UInt8
  *  toMonday, toStartOfMonth, toStartOfYear -> Date
  *  toStartOfMinute, toStartOfHour, toTime, now -> DateTime
  *
  * And also:
  *
  * timeSlot(EventTime)
  * - rounds the time to half an hour.
  *
  * timeSlots(StartTime, Duration)
  * - for the time interval beginning at `StartTime` and continuing `Duration` seconds,
  *   returns an array of time points, consisting of rounding down to half an hour of points from this interval.
  *  For example, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  This is necessary to search for hits that are part of the corresponding visit.
  */


/// Determine working timezone either from optional argument with time zone name or from time zone in DateTime type of argument.
std::string extractTimeZoneNameFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments,
    size_t time_zone_arg_num,
    size_t datetime_arg_num);
const DateLUTImpl & extractTimeZoneFromFunctionArguments(
    Block & block,
    const ColumnNumbers & arguments,
    size_t time_zone_arg_num,
    size_t datetime_arg_num);


#define TIME_SLOT_SIZE 1800

/** Transformations.
  * Represents two functions - from datetime (UInt32) and from date (UInt16).
  *
  * Also, the "factor transformation" F is defined for the T transformation.
  * This is a transformation of F such that its value identifies the region of monotonicity for T
  *  (for a fixed value of F, the transformation T is monotonic).
  *
  * Or, figuratively, if T is similar to taking the remainder of division, then F is similar to division.
  *
  * Example: for transformation T "get the day number in the month" (2015-02-03 -> 3),
  *  factor-transformation F is "round to the nearest month" (2015-02-03 -> 2015-02-01).
  */

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(UInt32, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt64, const DateLUTImpl &) { return 0; }
};

struct ToDateImpl
{
    static constexpr auto name = "toDate";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return UInt16(time_zone.toDayNum(t)); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &) { return d; }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfDay",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
    static constexpr auto name = "toStartOfDay";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toDate(t); }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type Date of argument for function toStartOfDay",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfDay",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToMondayImpl
{
    static constexpr auto name = "toMonday";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toMonday", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMonthImpl
{
    static constexpr auto name = "toStartOfMonth";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfMonday",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfQuarterImpl
{
    static constexpr auto name = "toStartOfQuarter";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfQuarter",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfYearImpl
{
    static constexpr auto name = "toStartOfYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfYear",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
    static constexpr auto name = "toTime";

    /// When transforming to time, the date will be equated to 1970-01-02.
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toTime(t) + 86400; }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toStartOfMinute(t); }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type Date of argument for function toStartOfMinute",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfMinute",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinuteImpl
{
    static constexpr auto name = "toStartOfFiveMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toStartOfFiveMinute(t); }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type Date of argument for function toStartOfFiveMinute",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfFiveMinute",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFifteenMinutesImpl
{
    static constexpr auto name = "toStartOfFifteenMinutes";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type Date of argument for function toStartOfFifteenMinutes",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfFifteenMinutes",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toStartOfHour(t); }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type Date of argument for function toStartOfHour",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toStartOfHour",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
    static constexpr auto name = "toYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toYear(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toYear(DayNum(d)); }
    static inline UInt16 execute(UInt64 packed, const DateLUTImpl &) { return UInt16((packed >> 46) / 13); }

    using FactorTransform = ZeroTransform;
};

struct ToQuarterImpl
{
    static constexpr auto name = "toQuarter";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toQuarter(t); }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toQuarter(DayNum(d)); }
    static inline UInt8 execute(UInt64 packed, const DateLUTImpl &)
    {
        return ((/* Month */ (packed >> 46) % 13) + 2) / 3;
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToMonthImpl
{
    static constexpr auto name = "toMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toMonth(t); }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toMonth(DayNum(d)); }
    // tidb date related type, ignore time_zone info
    static inline UInt8 execute(UInt64 t, const DateLUTImpl &) { return static_cast<UInt8>((t >> 46u) % 13); }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
    static constexpr auto name = "toDayOfMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toDayOfMonth(t); }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toDayOfMonth(DayNum(d)); }
    static inline UInt8 execute(UInt64 t, const DateLUTImpl &) { return static_cast<UInt8>((t >> 41) & 31); }

    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toDayOfWeek(t); }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toDayOfWeek(DayNum(d)); }
    static inline UInt8 execute(UInt64 d, const DateLUTImpl &)
    {
        MyDateTime my_time(d);
        return UInt8(my_time.weekDay() + 1);
    }

    using FactorTransform = ToMondayImpl;
};

struct ToDayOfYearImpl
{
    static constexpr auto name = "toDayOfYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toDayOfYear(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toDayOfYear(DayNum(d)); }
    static inline UInt16 execute(UInt64 d, const DateLUTImpl &)
    {
        MyDateTime my_time(d);
        return UInt16(my_time.yearDay());
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToHourImpl
{
    static constexpr auto name = "toHour";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toHour(t); }

    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToMinuteImpl
{
    static constexpr auto name = "toMinute";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toMinute(t); }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
    static constexpr auto name = "toSecond";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toSecond(t); }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfMinuteImpl;
};

struct ToRelativeYearNumImpl
{
    static constexpr auto name = "toRelativeYearNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toYear(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.toYear(DayNum(d)); }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeYearNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toRelativeQuarterNum(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeQuarterNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toRelativeMonthNum(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeMonthNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toRelativeWeekNum(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeWeekNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toDayNum(t); }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &) { return static_cast<DayNum>(d); }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeDayNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};


struct ToRelativeHourNumImpl
{
    static constexpr auto name = "toRelativeHourNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toRelativeHourNum(t); }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeHourNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toRelativeMinuteNum(t); }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(DayNum(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeMinuteNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeSecondNumImpl
{
    static constexpr auto name = "toRelativeSecondNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl &) { return t; }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone) { return time_zone.fromDayNum(DayNum(d)); }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toRelativeSecondNum",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMImpl
{
    static constexpr auto name = "toYYYYMM";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toNumYYYYMM(t); }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(static_cast<DayNum>(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception("Illegal type MyTime of argument for function toYYYYMM", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDImpl
{
    static constexpr auto name = "toYYYYMMDD";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toNumYYYYMMDD(t); }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(static_cast<DayNum>(d));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toYYYYMMDD",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDhhmmssImpl
{
    static constexpr auto name = "toYYYYMMDDhhmmss";

    static inline UInt64 execute(UInt32 t, const DateLUTImpl & time_zone) { return time_zone.toNumYYYYMMDDhhmmss(t); }
    static inline UInt64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(static_cast<DayNum>(d)));
    }
    static inline UInt8 execute(UInt64, const DateLUTImpl &)
    {
        throw Exception(
            "Illegal type MyTime of argument for function toYYYYMMDDhhmmss",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

template <typename FromType, typename ToType, typename Transform>
struct Transformer
{
    static void vector(
        const PaddedPODArray<FromType> & vec_from,
        PaddedPODArray<ToType> & vec_to,
        const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], time_zone);
    }
};

template <typename FromType, typename ToType, typename Transform>
struct DateTimeTransformImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        using Op = Transformer<FromType, ToType, Transform>;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 1, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::vector(sources->getData(), col_to->getData(), time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    Transform::name),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); };

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 1)
        {
            if (!arguments[0].type->isDateOrDateTime())
                throw Exception(
                    fmt::format(
                        "Illegal type {} of argument of function {}. Should be a date or a date with time",
                        arguments[0].type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!checkDataType<DataTypeDateTime>(arguments[0].type.get())
                || !checkDataType<DataTypeString>(arguments[1].type.get()))
                throw Exception(
                    fmt::format(
                        "Function {} supports 1 or 2 arguments. The 1st argument "
                        "must be of type Date or DateTime. The 2nd argument (optional) must be "
                        "a constant string with timezone name. The timezone argument is allowed "
                        "only when the 1st argument has the type DateTime",
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// For DateTime, if time zone is specified, attach it to type.
        if (std::is_same_v<ToDataType, DataTypeDateTime>)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
        else
            return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkDataType<DataTypeDate>(from_type))
            DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform>::execute(
                block,
                arguments,
                result);
        else if (checkDataType<DataTypeDateTime>(from_type))
            DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform>::execute(
                block,
                arguments,
                result);
        else if (from_type->isMyDateOrMyDateTime())
            DateTimeTransformImpl<DataTypeMyTimeBase::FieldType, typename ToDataType::FieldType, Transform>::execute(
                block,
                arguments,
                result);
        else
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    block.getByPosition(arguments[0]).type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        IFunction::Monotonicity is_monotonic{true};
        IFunction::Monotonicity is_not_monotonic;

        if (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
        {
            is_monotonic.is_always_monotonic = true;
            return is_monotonic;
        }

        /// This method is called only if the function has one argument. Therefore, we do not care about the non-local time zone.
        const DateLUTImpl & date_lut = DateLUT::instance();

        if (left.isNull() || right.isNull())
            return is_not_monotonic;

        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

        if (checkAndGetDataType<DataTypeDate>(&type))
        {
            return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), date_lut)
                    == Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), date_lut)
                ? is_monotonic
                : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), date_lut)
                    == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), date_lut)
                ? is_monotonic
                : is_not_monotonic;
        }
    }
};

struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return t + delta; }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta;
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl &) { return addSeconds(t, delta); }

    // TODO: need do these in vector mode in the future
    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        Field packed_uint_value = parseMyDateTime(str, 6, checkTimeValid);
        UInt64 packed_uint = packed_uint_value.template safeGet<UInt64>();
        UInt64 result = AddSecondsImpl::execute(packed_uint, delta, time_zone);
        MyDateTime myDateTime(result);
        return myDateTime.toString(myDateTime.micro_second == 0 ? 0 : 6);
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return t + delta * 60; }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 60;
    }
    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(t, delta * 60, time_zone);
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(str, delta * 60, time_zone);
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return t + delta * 3600; }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 3600;
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(t, delta * 3600, time_zone);
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(str, delta * 3600, time_zone);
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addDays(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &) { return d + delta; }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl &)
    {
        if (t == 0)
        {
            return t;
        }
        MyDateTime my_time(t);
        addDays(my_time, delta);
        return my_time.toPackedUInt();
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        auto value_and_is_date = parseMyDateTimeAndJudgeIsDate(str, 6, checkTimeValid);
        Field packed_uint_value = value_and_is_date.first;
        bool is_date = value_and_is_date.second;
        UInt64 packed_uint = packed_uint_value.template safeGet<UInt64>();
        UInt64 result = AddDaysImpl::execute(packed_uint, delta, time_zone);
        if (is_date)
            return MyDate(result).toString();
        else
        {
            MyDateTime myDateTime(result);
            return myDateTime.toString(myDateTime.micro_second == 0 ? 0 : 6);
        }
    }
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addWeeks(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &) { return d + delta * 7; }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddDaysImpl::execute(t, delta * 7, time_zone);
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddDaysImpl::execute(str, delta * 7, time_zone);
        ;
    }
};

struct AddMonthsImpl
{
    static constexpr auto name = "addMonths";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(DayNum(d), delta);
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl &)
    {
        if (t == 0)
        {
            return t;
        }
        MyDateTime my_time(t);
        addMonths(my_time, delta);
        return my_time.toPackedUInt();
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        auto value_and_is_date = parseMyDateTimeAndJudgeIsDate(str, 6, checkTimeValid);
        Field packed_uint_value = value_and_is_date.first;
        bool is_date = value_and_is_date.second;
        UInt64 packed_uint = packed_uint_value.template safeGet<UInt64>();
        UInt64 result = AddMonthsImpl::execute(packed_uint, delta, time_zone);
        if (is_date)
            return MyDate(result).toString();
        else
        {
            MyDateTime myDateTime(result);
            return myDateTime.toString(myDateTime.micro_second == 0 ? 0 : 6);
        }
    }
};

struct AddYearsImpl
{
    static constexpr auto name = "addYears";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(DayNum(d), delta);
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddMonthsImpl::execute(t, delta * 12, time_zone);
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddMonthsImpl::execute(str, delta * 12, time_zone);
    }
};


template <typename Transform>
struct SubtractIntervalImpl
{
    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(t, -delta, time_zone);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(d, -delta, time_zone);
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(t, -delta, time_zone);
    }

    static inline String execute(String str, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(str, -delta, time_zone);
    }
};

struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl>
{
    static constexpr auto name = "subtractSeconds";
};
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl>
{
    static constexpr auto name = "subtractMinutes";
};
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl>
{
    static constexpr auto name = "subtractHours";
};
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl>
{
    static constexpr auto name = "subtractDays";
};
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl>
{
    static constexpr auto name = "subtractWeeks";
};
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl>
{
    static constexpr auto name = "subtractMonths";
};
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl>
{
    static constexpr auto name = "subtractYears";
};


template <typename FromType, typename ToType, typename Transform>
struct Adder
{
    static void vectorVector(
        const PaddedPODArray<FromType> & vec_from,
        PaddedPODArray<ToType> & vec_to,
        const IColumn & delta,
        const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta.getInt(i), time_zone);
    }

    static void vectorConstant(
        const PaddedPODArray<FromType> & vec_from,
        PaddedPODArray<ToType> & vec_to,
        Int64 delta,
        const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta, time_zone);
    }

    static void constantVector(
        const FromType & from,
        PaddedPODArray<ToType> & vec_to,
        const IColumn & delta,
        const DateLUTImpl & time_zone)
    {
        size_t size = delta.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(from, delta.getInt(i), time_zone);
    }
};


template <typename FromType, typename Transform, bool use_utc_timezone>
struct DateTimeAddIntervalImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        using ToType = decltype(Transform::execute(FromType(), 0, std::declval<DateLUTImpl>()));
        using Op = Adder<FromType, ToType, Transform>;

        const DateLUTImpl & time_zone = use_utc_timezone ? DateLUT::instance("UTC")
                                                         : extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                Op::vectorConstant(sources->getData(), col_to->getData(), delta_const_column->getInt(0), time_zone);
            else
                Op::vectorVector(sources->getData(), col_to->getData(), delta_column, time_zone);

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (const auto * sources = checkAndGetColumnConst<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::constantVector(
                sources->template getValue<FromType>(),
                col_to->getData(),
                *block.getByPosition(arguments[1]).column,
                time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    Transform::name),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

template <typename Transform, bool use_utc_timezone>
struct DateTimeAddIntervalImpl<DataTypeString::FieldType, Transform, use_utc_timezone>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;

        const DateLUTImpl & time_zone = use_utc_timezone ? DateLUT::instance("UTC")
                                                         : extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        auto col_to = ColumnString::create();
        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

        if (const auto * col_from_string = checkAndGetColumn<ColumnString>(source_col.get()))
        {
            const ColumnString::Chars_t * data_from = &col_from_string->getChars();
            const IColumn::Offsets * offsets_from = &col_from_string->getOffsets();
            size_t size = source_col->size();
            col_null_map_to = ColumnUInt8::create(col_from_string->size(), 0);
            vec_null_map_to = &col_null_map_to->getData();
            offsets_to.resize(size);
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            size_t current_offset = 0;

            const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column);
            std::function<int(size_t)> get_value_func;
            if (delta_const_column)
            {
                // string date add const int interval
                get_value_func = [&](size_t) -> int {
                    return delta_const_column->getInt(0);
                };
            }
            else
            {
                // string date add vector int interval
                get_value_func = [&](size_t index) -> int {
                    return delta_column.getInt(index);
                };
            }

            for (size_t i = 0; i < size; ++i)
            {
                size_t next_offset = (*offsets_from)[i];
                size_t org_length = next_offset - current_offset - 1;
                size_t byte_length = org_length;
                String date_str(reinterpret_cast<const char *>(&(*data_from)[current_offset]), byte_length);
                try
                {
                    String result_str = Transform::execute(date_str, get_value_func(i), time_zone);
                    write_buffer.write(reinterpret_cast<const char *>(&(result_str)[0]), result_str.size());
                }
                catch (const Exception &)
                {
                    (*vec_null_map_to)[i] = 1;
                    write_buffer.write(reinterpret_cast<const char *>(&("2020-01-01")[0]), 10);
                }
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
                current_offset = next_offset;
            }
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        }
        // const string date add vector int interval
        else if (const auto * col_from_string = checkAndGetColumnConst<ColumnString>(source_col.get()))
        {
            size_t size = delta_column.size();
            col_null_map_to = ColumnUInt8::create(col_from_string->size(), 0);
            vec_null_map_to = &col_null_map_to->getData();
            String date_str = col_from_string->template getValue<String>();
            offsets_to.resize(size);
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            for (size_t i = 0; i < size; ++i)
            {
                try
                {
                    String result_str = Transform::execute(date_str, delta_column.getInt(i), time_zone);
                    write_buffer.write(reinterpret_cast<const char *>(&(result_str)[0]), result_str.size());
                }
                catch (const Exception &)
                {
                    (*vec_null_map_to)[i] = 1;
                    write_buffer.write(reinterpret_cast<const char *>(&("2020-01-01")[0]), 10);
                }

                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    Transform::name),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


template <typename Transform>
class FunctionDateOrDateTimeAddInterval : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateOrDateTimeAddInterval>(); };

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        //todo support string as tidb support string
        if (!arguments[1].type->isNumber())
            throw Exception(
                fmt::format("Second argument for function {} (delta) must be number", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!arguments[0].type->isDateOrDateTime() && !arguments[0].type->isString())
                throw Exception(
                    fmt::format(
                        "Illegal type {} of argument of function {}. Should be a date or a date with time or string",
                        arguments[0].type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
        {
            if (!checkDataType<DataTypeDateTime>(arguments[0].type.get())
                || !checkDataType<DataTypeString>(arguments[2].type.get()))
                throw Exception(
                    fmt::format(
                        "Function {} supports 2 or 3 arguments. The 1st argument "
                        "must be of type Date or DateTime. The 2nd argument must be number. "
                        "The 3rd argument (optional) must be "
                        "a constant string with timezone name. The timezone argument is allowed "
                        "only when the 1st argument has the type DateTime",
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (checkDataType<DataTypeDate>(arguments[0].type.get()))
        {
            if (std::is_same_v<
                    decltype(Transform::execute(DataTypeDate::FieldType(), 0, std::declval<DateLUTImpl>())),
                    UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else if (checkDataType<DataTypeDateTime>(arguments[0].type.get()))
        {
            if (std::is_same_v<
                    decltype(Transform::execute(DataTypeDateTime::FieldType(), 0, std::declval<DateLUTImpl>())),
                    UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else if (checkDataType<DataTypeString>(arguments[0].type.get()))
        {
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        }
        else
        {
            // for MyDate and MyDateTime, according to TiDB implementation the return type is always return MyDateTime
            // todo consider the fsp in delta part
            int fsp = 0;
            const auto * my_datetime_type = checkAndGetDataType<DataTypeMyDateTime>(arguments[0].type.get());
            if (my_datetime_type != nullptr)
                fsp = my_datetime_type->getFraction();
            return std::make_shared<DataTypeMyDateTime>(fsp);
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkDataType<DataTypeDate>(from_type))
            DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform, false>::execute(block, arguments, result);
        else if (checkDataType<DataTypeDateTime>(from_type))
            DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform, false>::execute(block, arguments, result);
        else if (from_type->isMyDateOrMyDateTime())
            DateTimeAddIntervalImpl<DataTypeMyTimeBase::FieldType, Transform, true>::execute(block, arguments, result);
        else if (checkDataType<DataTypeString>(from_type))
            DateTimeAddIntervalImpl<DataTypeString::FieldType, Transform, true>::execute(block, arguments, result);
        else
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    block.getByPosition(arguments[0]).type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

class FunctionTiDBTimestampDiff : public IFunction
{
public:
    static constexpr auto name = "tidbTimestampDiff";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBTimestampDiff>(); };

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!removeNullable(arguments[0])->isString())
            throw Exception(
                fmt::format("First argument for function {} (unit) must be String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[1])->isMyDateOrMyDateTime() && !arguments[1]->onlyNull())
            throw Exception(
                fmt::format("Second argument for function {} must be MyDate or MyDateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[2])->isMyDateOrMyDateTime() && !arguments[2]->onlyNull())
            throw Exception(
                fmt::format("Third argument for function {} must be MyDate or MyDateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // to align with tidb, timestampdiff with zeroDate input should return null, so always return nullable type
        return makeNullable(std::make_shared<DataTypeInt64>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception(
                fmt::format("First argument for function {} must be constant String", getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        String unit = Poco::toLower(unit_column->getValue<String>());

        bool has_nullable = false;
        bool has_null_constant = false;
        for (const auto & arg : arguments)
        {
            const auto & elem = block.getByPosition(arg);
            has_nullable |= elem.type->isNullable();
            has_null_constant |= elem.type->onlyNull();
        }

        if (has_null_constant)
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        ColumnPtr x_p = block.getByPosition(arguments[1]).column;
        ColumnPtr y_p = block.getByPosition(arguments[2]).column;
        if (has_nullable)
        {
            Block temporary_block = createBlockWithNestedColumns(block, arguments, result);
            x_p = temporary_block.getByPosition(arguments[1]).column;
            y_p = temporary_block.getByPosition(arguments[2]).column;
        }

        const IColumn & x = *x_p;
        const IColumn & y = *y_p;

        size_t rows = block.rows();
        auto res = ColumnInt64::create(rows, 0);
        auto result_null_map = ColumnUInt8::create(rows, 0);
        if (unit == "year")
            dispatchForColumns<MonthDiffCalculatorImpl, YearDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "quarter")
            dispatchForColumns<MonthDiffCalculatorImpl, QuarterDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "month")
            dispatchForColumns<MonthDiffCalculatorImpl, MonthDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "week")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, WeekDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "day")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, DayDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "hour")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, HourDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "minute")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, MinuteDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "second")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, SecondDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else if (unit == "microsecond")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, MicroSecondDiffResultCalculator>(
                x,
                y,
                res->getData(),
                result_null_map->getData());
        else
            throw Exception(
                fmt::format("Function {} does not support '{}' unit", getName(), unit),
                ErrorCodes::BAD_ARGUMENTS);
        // warp null

        if (block.getByPosition(arguments[1]).type->isNullable()
            || block.getByPosition(arguments[2]).type->isNullable())
        {
            ColumnUInt8::Container & vec_result_null_map = result_null_map->getData();
            ColumnPtr _x_p = block.getByPosition(arguments[1]).column;
            ColumnPtr _y_p = block.getByPosition(arguments[2]).column;
            for (size_t i = 0; i < rows; i++)
            {
                vec_result_null_map[i] |= (_x_p->isNullAt(i) || _y_p->isNullAt(i));
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }

private:
    template <typename MonthDiffCalculator, typename ResultCalculator>
    void dispatchForColumns(
        const IColumn & x,
        const IColumn & y,
        ColumnInt64::Container & res,
        ColumnUInt8::Container & res_null_map) const
    {
        const auto * x_const = checkAndGetColumnConst<ColumnUInt64>(&x);
        const auto * y_const = checkAndGetColumnConst<ColumnUInt64>(&y);
        if (x_const)
        {
            const auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            constantVector<MonthDiffCalculator, ResultCalculator>(
                x_const->getValue<UInt64>(),
                *y_vec,
                res,
                res_null_map);
        }
        else if (y_const)
        {
            const auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            vectorConstant<MonthDiffCalculator, ResultCalculator>(
                *x_vec,
                y_const->getValue<UInt64>(),
                res,
                res_null_map);
        }
        else
        {
            const auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            const auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            vectorVector<MonthDiffCalculator, ResultCalculator>(*x_vec, *y_vec, res, res_null_map);
        }
    }

    template <typename MonthDiffCalculator, typename ResultCalculator>
    void vectorVector(
        const ColumnVector<UInt64> & x,
        const ColumnVector<UInt64> & y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map) const
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
        {
            result_null_map[i] = (x_data[i] == 0 || y_data[i] == 0);
            if (!result_null_map[i])
                result[i] = calculate<MonthDiffCalculator, ResultCalculator>(x_data[i], y_data[i]);
        }
    }

    template <typename MonthDiffCalculator, typename ResultCalculator>
    void vectorConstant(
        const ColumnVector<UInt64> & x,
        UInt64 y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map) const
    {
        const auto & x_data = x.getData();
        if (y == 0)
        {
            for (size_t i = 0, size = x.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = x.size(); i < size; ++i)
            {
                result_null_map[i] = (x_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate<MonthDiffCalculator, ResultCalculator>(x_data[i], y);
            }
        }
    }

    template <typename MonthDiffCalculator, typename ResultCalculator>
    void constantVector(
        UInt64 x,
        const ColumnVector<UInt64> & y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map) const
    {
        const auto & y_data = y.getData();
        if (x == 0)
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
            {
                result_null_map[i] = (y_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate<MonthDiffCalculator, ResultCalculator>(x, y_data[i]);
            }
        }
    }

    static void calculateTimeDiff(
        const MyDateTime & x,
        const MyDateTime & y,
        Int64 & seconds,
        int & micro_seconds,
        bool & neg)
    {
        Int64 days_x = calcDayNum(x.year, x.month, x.day);
        Int64 days_y = calcDayNum(y.year, y.month, y.day);
        Int64 days = days_y - days_x;

        Int64 tmp = (days * MyTimeBase::SECOND_IN_ONE_DAY + y.hour * 3600LL + y.minute * 60LL + y.second
                     - (x.hour * 3600LL + x.minute * 60LL + x.second))
                * E6
            + y.micro_second - x.micro_second;
        if (tmp < 0)
        {
            tmp = -tmp;
            neg = true;
        }
        seconds = tmp / E6;
        micro_seconds = int(tmp % E6);
    }

    struct MonthDiffCalculatorImpl
    {
        static inline UInt32 execute(const MyDateTime & x_time, const MyDateTime & y_time, const bool & neg)
        {
            UInt32 months = 0;
            UInt32 year_begin, year_end, month_begin, month_end, day_begin, day_end;
            UInt32 second_begin, second_end, micro_second_begin, micro_second_end;
            if (neg)
            {
                year_begin = y_time.year;
                year_end = x_time.year;
                month_begin = y_time.month;
                month_end = x_time.month;
                day_begin = y_time.day;
                day_end = y_time.day;
                second_begin = y_time.hour * 3600 + y_time.minute * 60 + y_time.second;
                second_end = x_time.hour * 3600 + x_time.minute * 60 + x_time.second;
                micro_second_begin = y_time.micro_second;
                micro_second_end = x_time.micro_second;
            }
            else
            {
                year_begin = x_time.year;
                year_end = y_time.year;
                month_begin = x_time.month;
                month_end = y_time.month;
                day_begin = x_time.day;
                day_end = y_time.day;
                second_begin = x_time.hour * 3600 + x_time.minute * 60 + x_time.second;
                second_end = y_time.hour * 3600 + y_time.minute * 60 + y_time.second;
                micro_second_begin = x_time.micro_second;
                micro_second_end = y_time.micro_second;
            }

            // calc years
            UInt32 years = year_end - year_begin;
            if (month_end < month_begin || (month_end == month_begin && day_end < day_begin))
            {
                years--;
            }

            // calc months
            months = 12 * years;
            if (month_end < month_begin || (month_end == month_begin && day_end < day_begin))
            {
                months += 12 - (month_begin - month_end);
            }
            else
            {
                months += month_end - month_begin;
            }

            if (day_end < day_begin)
            {
                months--;
            }
            else if (
                (day_end == day_begin)
                && ((second_end < second_begin)
                    || (second_end == second_begin && micro_second_end < micro_second_begin)))
            {
                months--;
            }
            return months;
        }
    };

    struct DummyMonthDiffCalculatorImpl
    {
        static inline UInt32 execute(const MyDateTime &, const MyDateTime &, const bool) { return 0; }
    };

    struct YearDiffResultCalculator
    {
        static inline Int64 execute(const Int64, const Int64, const Int64 months, const int neg_value)
        {
            return months / 12 * neg_value;
        }
    };

    struct QuarterDiffResultCalculator
    {
        static inline Int64 execute(const Int64, const Int64, const Int64 months, const int neg_value)
        {
            return months / 3 * neg_value;
        }
    };

    struct MonthDiffResultCalculator
    {
        static inline Int64 execute(const Int64, const Int64, const Int64 months, const int neg_value)
        {
            return months * neg_value;
        }
    };

    struct WeekDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64, const Int64, const int neg_value)
        {
            return seconds / MyTimeBase::SECOND_IN_ONE_DAY / 7 * neg_value;
        }
    };

    struct DayDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64, const Int64, const int neg_value)
        {
            return seconds / MyTimeBase::SECOND_IN_ONE_DAY * neg_value;
        }
    };

    struct HourDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64, const Int64, const int neg_value)
        {
            return seconds / 3600 * neg_value;
        }
    };

    struct MinuteDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64, const Int64, const int neg_value)
        {
            return seconds / 60 * neg_value;
        }
    };

    struct SecondDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64, const Int64, const int neg_value)
        {
            return seconds * neg_value;
        }
    };

    struct MicroSecondDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 micro_seconds, const Int64, const int neg_value)
        {
            return (seconds * E6 + micro_seconds) * neg_value;
        }
    };

    template <typename MonthDiffCalculator, typename ResultCalculator>
    Int64 calculate(UInt64 x, UInt64 y) const
    {
        MyDateTime x_time(x);
        MyDateTime y_time(y);
        Int64 seconds = 0;
        int micro_seconds = 0;
        bool neg = false;
        calculateTimeDiff(x_time, y_time, seconds, micro_seconds, neg);
        UInt32 months = MonthDiffCalculator::execute(x_time, y_time, neg);
        return ResultCalculator::execute(seconds, micro_seconds, months, neg ? -1 : 1);
    }
};

/** TiDBDateDiff(t1, t2)
 *  Supports for tidb's dateDiff,
 *  returns t1 − t2 expressed as a value in days from one date to the other.
 *  Only the date parts of the values are used in the calculation.
 */
class FunctionTiDBDateDiff : public IFunction
{
public:
    static constexpr auto name = "tidbDateDiff";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBDateDiff>(); };

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!removeNullable(arguments[0])->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format("First argument for function {} must be MyDate or MyDateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[1])->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format("Second argument for function {} must be MyDate or MyDateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // to align with tidb, dateDiff with zeroDate input should return null, so always return nullable type
        return makeNullable(std::make_shared<DataTypeInt64>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        bool has_nullable = false;
        bool has_null_constant = false;
        for (const auto & arg : arguments)
        {
            const auto & elem = block.getByPosition(arg);
            has_nullable |= elem.type->isNullable();
            has_null_constant |= elem.type->onlyNull();
        }

        if (has_null_constant)
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        ColumnPtr x_p = block.getByPosition(arguments[0]).column;
        ColumnPtr y_p = block.getByPosition(arguments[1]).column;
        if (has_nullable)
        {
            Block temporary_block = createBlockWithNestedColumns(block, arguments, result);
            x_p = temporary_block.getByPosition(arguments[0]).column;
            y_p = temporary_block.getByPosition(arguments[1]).column;
        }

        const IColumn & x = *x_p;
        const IColumn & y = *y_p;

        size_t rows = block.rows();
        auto res = ColumnInt64::create(rows, 0);
        auto result_null_map = ColumnUInt8::create(rows, 0);

        dispatch(x, y, res->getData(), result_null_map->getData());

        if (block.getByPosition(arguments[0]).type->isNullable()
            || block.getByPosition(arguments[1]).type->isNullable())
        {
            ColumnUInt8::Container & vec_result_null_map = result_null_map->getData();
            ColumnPtr _x_p = block.getByPosition(arguments[0]).column;
            ColumnPtr _y_p = block.getByPosition(arguments[1]).column;
            for (size_t i = 0; i < rows; i++)
            {
                vec_result_null_map[i] |= (_x_p->isNullAt(i) || _y_p->isNullAt(i));
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }

private:
    static void dispatch(
        const IColumn & x,
        const IColumn & y,
        ColumnInt64::Container & res,
        ColumnUInt8::Container & res_null_map)
    {
        const auto * x_const = checkAndGetColumnConst<ColumnUInt64>(&x);
        const auto * y_const = checkAndGetColumnConst<ColumnUInt64>(&y);
        if (x_const)
        {
            const auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            constantVector(x_const->getValue<UInt64>(), *y_vec, res, res_null_map);
        }
        else if (y_const)
        {
            const auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            vectorConstant(*x_vec, y_const->getValue<UInt64>(), res, res_null_map);
        }
        else
        {
            const auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            const auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            vectorVector(*x_vec, *y_vec, res, res_null_map);
        }
    }

    static void vectorVector(
        const ColumnVector<UInt64> & x,
        const ColumnVector<UInt64> & y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map)
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
        {
            result_null_map[i] = (x_data[i] == 0 || y_data[i] == 0);
            if (!result_null_map[i])
                result[i] = calculate(x_data[i], y_data[i]);
        }
    }

    static void vectorConstant(
        const ColumnVector<UInt64> & x,
        UInt64 y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map)
    {
        const auto & x_data = x.getData();
        if (y == 0)
        {
            for (size_t i = 0, size = x.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = x.size(); i < size; ++i)
            {
                result_null_map[i] = (x_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate(x_data[i], y);
            }
        }
    }

    static void constantVector(
        UInt64 x,
        const ColumnVector<UInt64> & y,
        ColumnInt64::Container & result,
        ColumnUInt8::Container & result_null_map)
    {
        const auto & y_data = y.getData();
        if (x == 0)
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
            {
                result_null_map[i] = (y_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate(x, y_data[i]);
            }
        }
    }

    static Int64 calculate(UInt64 x_packed, UInt64 y_packed)
    {
        MyDateTime x(x_packed);
        MyDateTime y(y_packed);

        Int64 days_x = calcDayNum(x.year, x.month, x.day);
        Int64 days_y = calcDayNum(y.year, y.month, y.day);

        return days_x - days_y;
    }
};

/** dateDiff('unit', t1, t2, [timezone])
  * t1 and t2 can be Date or DateTime
  *
  * If timezone is specified, it applied to both arguments.
  * If not, timezones from datatypes t1 and t2 are used.
  * If that timezones are not the same, the result is unspecified.
  *
  * Timezone matters because days can have different length.
  */
class FunctionDateDiff : public IFunction
{
public:
    static constexpr auto name = "dateDiff";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateDiff>(); };

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("First argument for function {} (unit) must be String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isDateOrDateTime())
            throw Exception(
                fmt::format("Second argument for function {} must be Date or DateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isDateOrDateTime())
            throw Exception(
                fmt::format("Third argument for function {} must be Date or DateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 4 && !arguments[3]->isString())
            throw Exception(
                fmt::format("Fourth argument for function {} (timezone) must be String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 3}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception(
                fmt::format("First argument for function {} must be constant String", getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        String unit = Poco::toLower(unit_column->getValue<String>());

        const IColumn & x = *block.getByPosition(arguments[1]).column;
        const IColumn & y = *block.getByPosition(arguments[2]).column;

        size_t rows = block.rows();
        auto res = ColumnInt64::create(rows);

        const DateLUTImpl & timezone_x = extractTimeZoneFromFunctionArguments(block, arguments, 3, 1);
        const DateLUTImpl & timezone_y = extractTimeZoneFromFunctionArguments(block, arguments, 3, 2);

        if (unit == "year" || unit == "yy" || unit == "yyyy")
            dispatchForColumns<ToRelativeYearNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "quarter" || unit == "qq" || unit == "q")
            dispatchForColumns<ToRelativeQuarterNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "month" || unit == "mm" || unit == "m")
            dispatchForColumns<ToRelativeMonthNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "week" || unit == "wk" || unit == "ww")
            dispatchForColumns<ToRelativeWeekNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "day" || unit == "dd" || unit == "d")
            dispatchForColumns<ToRelativeDayNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "hour" || unit == "hh")
            dispatchForColumns<ToRelativeHourNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "minute" || unit == "mi" || unit == "n")
            dispatchForColumns<ToRelativeMinuteNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "second" || unit == "ss" || unit == "s")
            dispatchForColumns<ToRelativeSecondNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else
            throw Exception(
                fmt::format("Function {} does not support '{}' unit", getName(), unit),
                ErrorCodes::BAD_ARGUMENTS);

        block.getByPosition(result).column = std::move(res);
    }

private:
    template <typename Transform>
    void dispatchForColumns(
        const IColumn & x,
        const IColumn & y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * x_vec = checkAndGetColumn<ColumnUInt16>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (const auto * x_vec = checkAndGetColumn<ColumnUInt32>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (const auto * x_const = checkAndGetColumnConst<ColumnUInt16>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt16>(), y, timezone_x, timezone_y, result);
        else if (const auto * x_const = checkAndGetColumnConst<ColumnUInt32>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt32>(), y, timezone_x, timezone_y, result);
        else
            throw Exception(
                fmt::format("Illegal column for first argument of function {}, must be Date or DateTime", getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchForSecondColumn(
        const ColumnVector<T1> & x,
        const IColumn & y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            vectorVector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (const auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            vectorVector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (const auto * y_const = checkAndGetColumnConst<ColumnUInt16>(&y))
            vectorConstant<Transform>(x, y_const->getValue<UInt16>(), timezone_x, timezone_y, result);
        else if (const auto * y_const = checkAndGetColumnConst<ColumnUInt32>(&y))
            vectorConstant<Transform>(x, y_const->getValue<UInt32>(), timezone_x, timezone_y, result);
        else
            throw Exception(
                fmt::format("Illegal column for second argument of function {}, must be Date or DateTime", getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchConstForSecondColumn(
        T1 x,
        const IColumn & y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            constantVector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (const auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            constantVector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else
            throw Exception(
                fmt::format("Illegal column for second argument of function {}, must be Date or DateTime", getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1, typename T2>
    void vectorVector(
        const ColumnVector<T1> & x,
        const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void vectorConstant(
        const ColumnVector<T1> & x,
        T2 y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y, timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void constantVector(
        T1 x,
        const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x,
        const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & y_data = y.getData();
        for (size_t i = 0, size = y.size(); i < size; ++i)
            result[i] = calculate<Transform>(x, y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    Int64 calculate(T1 x, T2 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y) const
    {
        return Int64(Transform::execute(y, timezone_y)) - Int64(Transform::execute(x, timezone_x));
    }
};


/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = "now";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) const override
    {
        block.getByPosition(result).column
            = DataTypeUInt32().createColumnConst(block.rows(), static_cast<UInt64>(time(nullptr)));
    }
};


class FunctionToday : public IFunction
{
public:
    static constexpr auto name = "today";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToday>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) const override
    {
        block.getByPosition(result).column
            = DataTypeUInt16().createColumnConst(block.rows(), UInt64(DateLUT::instance().toDayNum(time(nullptr))));
    }
};


class FunctionYesterday : public IFunction
{
public:
    static constexpr auto name = "yesterday";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionYesterday>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) const override
    {
        block.getByPosition(result).column
            = DataTypeUInt16().createColumnConst(block.rows(), UInt64(DateLUT::instance().toDayNum(time(nullptr)) - 1));
    }
};

template <bool convert_from_utc>
class FunctionMyTimeZoneConvertByOffset : public IFunction
{
    using FromFieldType = typename DataTypeMyDateTime::FieldType;
    using ToFieldType = typename DataTypeMyDateTime::FieldType;

public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMyTimeZoneConvertByOffset>(); };
    static constexpr auto name = convert_from_utc ? "ConvertTimeZoneByOffsetFromUTC" : "ConvertTimeZoneByOffsetToUTC";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }


    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 2",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeMyDateTime>(arguments[0].type.get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of first argument of function {}. Should be MyDateTime",
                    arguments[0].type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[1].type->isInteger())
            throw Exception(
                fmt::format(
                    "Illegal type {} of second argument of function {}. Should be Integer type",
                    arguments[1].type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0].type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (const ColumnVector<FromFieldType> * col_from
            = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnVector<ToFieldType>::create();
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            const auto * offset_col = block.getByPosition(arguments.back()).column.get();
            if (!offset_col->isColumnConst())
                throw Exception(
                    fmt::format("Second argument of function {} must be an integral constant", getName()),
                    ErrorCodes::ILLEGAL_COLUMN);

            const auto offset = offset_col->getInt(0);
            for (size_t i = 0; i < size; ++i)
            {
                UInt64 result_time = vec_from[i] + offset;
                // todo maybe affected by daytime saving, need double check
                if constexpr (convert_from_utc)
                    convertTimeZoneByOffset(vec_from[i], result_time, true, offset);
                else
                    convertTimeZoneByOffset(vec_from[i], result_time, false, offset);
                vec_to[i] = result_time;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    name),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
template <bool convert_from_utc>
class FunctionMyTimeZoneConverter : public IFunction
{
    using FromFieldType = typename DataTypeMyDateTime::FieldType;
    using ToFieldType = typename DataTypeMyDateTime::FieldType;

public:
    static constexpr auto name = convert_from_utc ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneToUTC";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMyTimeZoneConverter>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 2",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeMyDateTime>(arguments[0].type.get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}. Should be MyDateTime",
                    arguments[0].type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0].type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (const ColumnVector<FromFieldType> * col_from
            = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnVector<ToFieldType>::create();
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            static const auto & time_zone_utc = DateLUT::instance("UTC");
            const auto & time_zone_other = extractTimeZoneFromFunctionArguments(block, arguments, 1, 0);
            for (size_t i = 0; i < size; ++i)
            {
                UInt64 result_time = 0;
                if constexpr (convert_from_utc)
                    convertTimeZone(vec_from[i], result_time, time_zone_utc, time_zone_other);
                else
                    convertTimeZone(vec_from[i], result_time, time_zone_other, time_zone_utc);
                vec_to[i] = result_time;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    name),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/// Just changes time zone information for data type. The calculation is free.
class FunctionToTimeZone : public IFunction
{
public:
    static constexpr auto name = "toTimeZone";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToTimeZone>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 2",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeDateTime>(arguments[0].type.get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}. Should be DateTime",
                    arguments[0].type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        return std::make_shared<DataTypeDateTime>(time_zone_name);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};


class FunctionTimeSlot : public IFunction
{
public:
    static constexpr auto name = "timeSlot";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlot>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeDateTime>(arguments[0].get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of first argument of function {}. Must be DateTime.",
                    arguments[0]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeDateTime>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (const ColumnUInt32 * times
            = typeid_cast<const ColumnUInt32 *>(block.getByPosition(arguments[0]).column.get()))
        {
            auto res = ColumnUInt32::create();
            ColumnUInt32::Container & res_vec = res->getData();
            const ColumnUInt32::Container & vec = times->getData();

            size_t size = vec.size();
            res_vec.resize(size);

            for (size_t i = 0; i < size; ++i)
                res_vec[i] = vec[i] / TIME_SLOT_SIZE * TIME_SLOT_SIZE;

            block.getByPosition(result).column = std::move(res);
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename DurationType>
struct TimeSlotsImpl
{
    static void vectorVector(
        const PaddedPODArray<UInt32> & starts,
        const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values,
        ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE;
                 ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vectorConstant(
        const PaddedPODArray<UInt32> & starts,
        DurationType duration,
        PaddedPODArray<UInt32> & result_values,
        ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + duration) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constantVector(
        UInt32 start,
        const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values,
        ColumnArray::Offsets & result_offsets)
    {
        size_t size = durations.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constantConstant(UInt32 start, DurationType duration, Array & result)
    {
        for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + duration) / TIME_SLOT_SIZE; ++value)
            result.push_back(static_cast<UInt64>(value * TIME_SLOT_SIZE));
    }
};


class FunctionTimeSlots : public IFunction
{
public:
    static constexpr auto name = "timeSlots";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlots>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeDateTime>(arguments[0].get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of first argument of function {}. Must be DateTime",
                    arguments[0]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt32>(arguments[1].get()))
            throw Exception(
                fmt::format(
                    "Illegal type {} of second argument of function {}. Must be UInt32",
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * starts = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());
        const auto * const_starts
            = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());

        const auto * durations = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());
        const auto * const_durations
            = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vectorVector(starts->getData(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vectorConstant(
                starts->getData(),
                const_durations->getValue<UInt32>(),
                res_values,
                res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constantVector(
                const_starts->getValue<UInt32>(),
                durations->getData(),
                res_values,
                res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && const_durations)
        {
            Array const_res;
            TimeSlotsImpl<UInt32>::constantConstant(
                const_starts->getValue<UInt32>(),
                const_durations->getValue<UInt32>(),
                const_res);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), const_res);
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal columns {}, {} of arguments of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    block.getByPosition(arguments[1]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct ExtractMyDateTimeImpl
{
    static Int64 extractYear(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToYearImpl::execute(packed, lut);
    }

    static Int64 extractQuater(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToQuarterImpl::execute(packed, lut);
    }

    static Int64 extractMonth(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToMonthImpl::execute(packed, lut);
    }

    static Int64 extractWeek(UInt64 packed)
    {
        MyDateTime datetime(packed);
        return datetime.week(0);
    }

    static Int64 extractDay(UInt64 packed) { return (packed >> 41) & ((1 << 5) - 1); }

    static Int64 extractDayMicrosecond(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        Int64 s = datetime.second;
        return (day * 1000000 + h * 10000 + m * 100 + s) * 1000000 + datetime.micro_second;
    }

    static Int64 extractDaySecond(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        Int64 s = datetime.second;
        return day * 1000000 + h * 10000 + m * 100 + s;
    }

    static Int64 extractDayMinute(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        return day * 10000 + h * 100 + m;
    }

    static Int64 extractDayHour(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        return day * 100 + h;
    }

    static Int64 extractYearMonth(UInt64 packed)
    {
        Int64 y = extractYear(packed);
        Int64 m = extractMonth(packed);
        return y * 100 + m;
    }
};

class FunctionExtractMyDateTime : public IFunction
{
public:
    static constexpr auto name = "extractMyDateTime";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractMyDateTime>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("First argument for function {} (unit) must be String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format(
                    "Illegal type {} of second argument of function {}. Must be DateOrDateTime.",
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception(
                fmt::format("First argument for function {} must be constant String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String unit = Poco::toLower(unit_column->getValue<String>());

        auto col_from = block.getByPosition(arguments[1]).column;

        size_t rows = block.rows();
        auto col_to = ColumnInt64::create(rows);
        auto & vec_to = col_to->getData();

        if (unit == "year")
            dispatch<ExtractMyDateTimeImpl::extractYear>(col_from, vec_to);
        else if (unit == "quarter")
            dispatch<ExtractMyDateTimeImpl::extractQuater>(col_from, vec_to);
        else if (unit == "month")
            dispatch<ExtractMyDateTimeImpl::extractMonth>(col_from, vec_to);
        else if (unit == "week")
            dispatch<ExtractMyDateTimeImpl::extractWeek>(col_from, vec_to);
        else if (unit == "day")
            dispatch<ExtractMyDateTimeImpl::extractDay>(col_from, vec_to);
        else if (unit == "day_microsecond")
            dispatch<ExtractMyDateTimeImpl::extractDayMicrosecond>(col_from, vec_to);
        else if (unit == "day_second")
            dispatch<ExtractMyDateTimeImpl::extractDaySecond>(col_from, vec_to);
        else if (unit == "day_minute")
            dispatch<ExtractMyDateTimeImpl::extractDayMinute>(col_from, vec_to);
        else if (unit == "day_hour")
            dispatch<ExtractMyDateTimeImpl::extractDayHour>(col_from, vec_to);
        else if (unit == "year_month")
            dispatch<ExtractMyDateTimeImpl::extractYearMonth>(col_from, vec_to);
        else
            throw Exception(
                fmt::format("Function {} does not support '{}' unit", getName(), unit),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    using Func = Int64 (*)(UInt64);

    template <Func F>
    static void dispatch(const ColumnPtr col_from, PaddedPODArray<Int64> & vec_to)
    {
        if (const auto * from = checkAndGetColumn<ColumnUInt64>(col_from.get()); from)
        {
            const auto & data = from->getData();
            vectorDatetime<F>(data, vec_to);
        }
    }

    template <Func F>
    static void vectorDatetime(const ColumnUInt64::Container & vec_from, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(vec_from.size());
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            vec_to[i] = F(vec_from[i]);
        }
    }
};

struct ExtractMyDurationImpl
{
    static Int64 signMultiplier(const MyDuration & duration) { return duration.isNeg() ? -1 : 1; }

    static Int64 extractHour(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * duration.hours();
    }

    static Int64 extractMinute(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * duration.minutes();
    }

    static Int64 extractSecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * duration.seconds();
    }

    static Int64 extractMicrosecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * duration.microSecond();
    }

    static Int64 extractSecondMicrosecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * (duration.seconds() * 1000000LL + duration.microSecond());
    }

    static Int64 extractMinuteMicrosecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration)
            * ((duration.minutes() * 100LL + duration.seconds()) * 1000000LL + duration.microSecond());
    }

    static Int64 extractMinuteSecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * (duration.minutes() * 100LL + duration.seconds());
    }

    static Int64 extractHourMicrosecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration)
            * ((duration.hours() * 10000LL + duration.minutes() * 100LL + duration.seconds()) * 1000000LL
               + duration.microSecond());
    }

    static Int64 extractHourSecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration)
            * (duration.hours() * 10000LL + duration.minutes() * 100LL + duration.seconds());
    }

    static Int64 extractHourMinute(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * (duration.hours() * 100LL + duration.minutes());
    }

    static Int64 extractDayMicrosecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration)
            * ((duration.hours() * 10000LL + duration.minutes() * 100LL + duration.seconds()) * 1000000LL
               + duration.microSecond());
    }

    static Int64 extractDaySecond(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration)
            * (duration.hours() * 10000LL + duration.minutes() * 100LL + duration.seconds());
    }

    static Int64 extractDayMinute(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * (duration.hours() * 100LL + duration.minutes());
    }

    static Int64 extractDayHour(Int64 nano)
    {
        MyDuration duration(nano);
        return signMultiplier(duration) * duration.hours();
    }
};

struct ExtractMyDateTimeFromStringImpl
{
    static Int64 extractDayMicrosecond(String dtStr)
    {
        Field duration_field = parseMyDuration(dtStr);
        Int64 result = 0;
        if (duration_field.isNull())
        {
            // TODO: should return null
            return 0;
        }
        MyDuration duration(duration_field.template safeGet<Int64>());
        result = ExtractMyDurationImpl::extractDayMicrosecond(duration.nanoSecond());

        Field datetime_field = parseMyDateTime(dtStr);
        if (!datetime_field.isNull())
        {
            MyDateTime datetime(datetime_field.template safeGet<UInt64>());
            if (datetime.hour == duration.hours() && datetime.minute == duration.minutes()
                && datetime.second == duration.seconds() && datetime.year > 0)
            {
                return ExtractMyDateTimeImpl::extractDayMicrosecond(datetime.toPackedUInt());
            }
        }
        return result;
    }

    static Int64 extractDaySecond(String dtStr)
    {
        Field duration_field = parseMyDuration(dtStr);
        Int64 result = 0;
        if (duration_field.isNull())
        {
            // TODO: should return null
            return 0;
        }
        MyDuration duration(duration_field.template safeGet<Int64>());
        result = ExtractMyDurationImpl::extractDaySecond(duration.nanoSecond());

        Field datetime_field = parseMyDateTime(dtStr);
        if (!datetime_field.isNull())
        {
            MyDateTime datetime(datetime_field.template safeGet<UInt64>());
            if (datetime.hour == duration.hours() && datetime.minute == duration.minutes()
                && datetime.second == duration.seconds() && datetime.year > 0)
            {
                return ExtractMyDateTimeImpl::extractDaySecond(datetime.toPackedUInt());
            }
        }
        return result;
    }

    static Int64 extractDayMinute(String dtStr)
    {
        Field duration_field = parseMyDuration(dtStr);
        Int64 result = 0;
        if (duration_field.isNull())
        {
            // TODO: should return null
            return 0;
        }
        MyDuration duration(duration_field.template safeGet<Int64>());
        result = ExtractMyDurationImpl::extractDayMinute(duration.nanoSecond());

        Field datetime_field = parseMyDateTime(dtStr);
        if (!datetime_field.isNull())
        {
            MyDateTime datetime(datetime_field.template safeGet<UInt64>());
            if (datetime.hour == duration.hours() && datetime.minute == duration.minutes()
                && datetime.second == duration.seconds() && datetime.year > 0)
            {
                return ExtractMyDateTimeImpl::extractDayMinute(datetime.toPackedUInt());
            }
        }
        return result;
    }

    static Int64 extractDayHour(String dtStr)
    {
        Field duration_field = parseMyDuration(dtStr);
        Int64 result = 0;
        if (duration_field.isNull())
        {
            // TODO: should return null
            return 0;
        }
        MyDuration duration(duration_field.template safeGet<Int64>());
        result = ExtractMyDurationImpl::extractDayHour(duration.nanoSecond());

        Field datetime_field = parseMyDateTime(dtStr);
        if (!datetime_field.isNull())
        {
            MyDateTime datetime(datetime_field.template safeGet<UInt64>());
            if (datetime.hour == duration.hours() && datetime.minute == duration.minutes()
                && datetime.second == duration.seconds() && datetime.year > 0)
            {
                return ExtractMyDateTimeImpl::extractDayHour(datetime.toPackedUInt());
            }
        }
        return result;
    }
};

class FunctionExtractMyDateTimeFromString : public IFunction
{
public:
    static constexpr auto name = "extractMyDateTimeFromString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractMyDateTimeFromString>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("First argument for function {} (unit) must be String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                fmt::format(
                    "Illegal type {} of second argument of function {}. Must be String.",
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception(
                fmt::format("First argument for function {} must be constant String", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        String unit = Poco::toLower(unit_column->getValue<String>());

        auto col_from = block.getByPosition(arguments[1]).column;

        size_t rows = block.rows();
        auto col_to = ColumnInt64::create(rows);
        auto & vec_to = col_to->getData();

        if (unit == "day_microsecond")
            dispatch<ExtractMyDateTimeFromStringImpl::extractDayMicrosecond>(col_from, vec_to);
        else if (unit == "day_second")
            dispatch<ExtractMyDateTimeFromStringImpl::extractDaySecond>(col_from, vec_to);
        else if (unit == "day_minute")
            dispatch<ExtractMyDateTimeFromStringImpl::extractDayMinute>(col_from, vec_to);
        else if (unit == "day_hour")
            dispatch<ExtractMyDateTimeFromStringImpl::extractDayHour>(col_from, vec_to);
        else
            throw Exception(
                fmt::format("Function {} does not support '{}' unit", getName(), unit),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    using Func = Int64 (*)(String);

    template <Func F>
    static void dispatch(const ColumnPtr col_from, PaddedPODArray<Int64> & vec_to)
    {
        if (const auto * from = checkAndGetColumn<ColumnString>(col_from.get()); from)
        {
            const auto & data = from->getChars();
            const auto & offsets = from->getOffsets();
            vectorString<F>(data, offsets, vec_to);
        }
    }

    template <Func F>
    static void vectorString(
        const ColumnString::Chars_t & vec_from,
        const ColumnString::Offsets & offsets_from,
        PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(offsets_from.size());
        size_t current_offset = 0;
        for (size_t i = 0, sz = offsets_from.size(); i < sz; i++)
        {
            size_t next_offset = offsets_from[i];
            size_t string_size = next_offset - current_offset - 1;
            StringRef string_value(&vec_from[current_offset], string_size);
            vec_to[i] = F(string_value.toString());
            current_offset = next_offset;
        }
    }
};

struct SysDateWithFsp
{
public:
    static constexpr auto name = "sysDateWithFsp";
    static constexpr size_t arguments_number = 1;
    static constexpr bool use_default_implementation_for_constants = false;
    static ColumnNumbers getColumnNumbers() { return ColumnNumbers{0}; }

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments)
    {
        int fsp = 0;
        const auto fsp_type = arguments[0].type;
        const auto * fsp_column = arguments[0].column.get();
        if (fsp_type && fsp_type->isInteger() && fsp_column && fsp_column->isColumnConst())
        {
            fsp = fsp_column->getInt(0);
        }
        else
        {
            throw TiFlashException(
                fmt::format("First argument for function {} must be constant number", name),
                Errors::Coprocessor::BadRequest);
        }
        return std::make_shared<DataTypeMyDateTime>(fsp);
    }

    static UInt8 getFsp(Block & block, const ColumnNumbers & arguments)
    {
        const auto fsp_type = block.getByPosition(arguments[0]).type;
        const auto * fsp_column = block.getByPosition(arguments[0]).column.get();
        if (fsp_type && fsp_type->isInteger() && fsp_column && fsp_column->isColumnConst())
        {
            return fsp_column->getInt(0);
        }
        else
        {
            throw TiFlashException(
                fmt::format("First argument for function {} must be constant number", name),
                Errors::Coprocessor::BadRequest);
        }
    }
};

struct SysDateWithoutFsp
{
public:
    static constexpr auto name = "sysDateWithoutFsp";
    static constexpr size_t arguments_number = 0;
    static constexpr bool use_default_implementation_for_constants = true;
    static ColumnNumbers getColumnNumbers() { return ColumnNumbers{}; }

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeMyDateTime>(0); }

    static UInt8 getFsp(Block &, const ColumnNumbers &) { return 0; }
};

template <typename Transform>
class FunctionSysDate : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionSysDate>(context_); };
    explicit FunctionSysDate(const Context & context_)
        : context(context_){};

    String getName() const override { return Transform::name; }

    size_t getNumberOfArguments() const override { return Transform::arguments_number; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Transform::getReturnType(arguments);
    }

    bool useDefaultImplementationForConstants() const override
    {
        return Transform::use_default_implementation_for_constants;
    }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return Transform::getColumnNumbers(); }

    static void array(const UInt64 sysdate_packet, const Int32 row_count, UInt64 * dst)
    {
        UInt64 * dst_end = dst + row_count;
#if __SSE2__
        const auto uint64_sse = sizeof(__m128i) / sizeof(UInt64);
        auto * uint64_end_sse = dst + (dst_end - dst) / uint64_sse * uint64_sse;
        while (dst < uint64_end_sse)
        {
            _mm_store_si128(reinterpret_cast<__m128i *>(dst), _mm_set_epi64x(sysdate_packet, sysdate_packet));
            dst += uint64_sse;
        }
#endif
        while (dst < dst_end)
        {
            *dst = sysdate_packet;
            ++dst;
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const int row_count = block.rows();
        UInt8 fsp = Transform::getFsp(block, arguments);
        const UInt64 sysdate_packed
            = MyDateTime::getSystemDateTimeByTimezone(context.getTimezoneInfo(), fsp).toPackedUInt();
        auto col_to = ColumnVector<DataTypeMyDateTime::FieldType>::create(row_count);
        auto & vec_to = col_to->getData();
        vec_to.resize(row_count);
        if (row_count > 0)
            array(sysdate_packed, row_count, &vec_to[0]);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    const Context & context;
};

/// Behavior differences from TiDB:
/// for date in ['0000-01-01', '0000-03-01'), ToDayNameImpl is the same with MySQL, while TiDB is offset by one day
/// TiDB_DayName('0000-01-01') = 'Saturday', MySQL/TiFlash_DayName('0000-01-01') = 'Sunday'
struct ToDayNameImpl
{
    static constexpr auto name = "toDayName";
    static inline size_t getMaxStringLen()
    {
        return 10; // Wednesday + TerminatingZero
    }
    static inline const String & execute(UInt64 t) { return MyDateTime(t).weekDayName(); }
};

struct ToMonthNameImpl
{
    static constexpr auto name = "toMonthName";
    static inline size_t getMaxStringLen()
    {
        return 10; // September + TerminatingZero
    }
    static inline const String & execute(UInt64 t) { return MyDateTime(t).monthName(); }
};

template <typename Transform>
class FunctionDateTimeToString : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context & context_)
    {
        return std::make_shared<FunctionDateTimeToString>(context_);
    };
    explicit FunctionDateTimeToString(const Context & context_)
        : context(context_){};

    String getName() const override { return Transform::name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format("First argument for function {} (unit) must be MyDate or MyDateTime", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
        if (const auto * sources = checkAndGetColumn<ColumnVector<DataTypeMyTimeBase::FieldType>>(source_col.get()))
        {
            auto col_to = ColumnString::create();
            const auto & vec_from = sources->getData();
            size_t size = vec_from.size();
            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
            auto & vec_null_map_to = col_null_map_to->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            data_to.resize(size * Transform::getMaxStringLen());
            offsets_to.resize(size);
            size_t total_str_len = 0;
            for (size_t i = 0; i < size; ++i)
            {
                const String & res = Transform::execute(vec_from[i]);
                if (res.empty())
                    vec_null_map_to[i] = 1;
                size_t length = res.length();
                // Following offset operations are learned from ColumnString's insertData
                memcpy(&data_to[total_str_len], res.c_str(), length);
                data_to[total_str_len + length] = 0; // for terminating zero
                total_str_len += (length + 1);
                offsets_to[i] = total_str_len;
            }
            data_to.resize(total_str_len);
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    block.getByPosition(arguments[0]).type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    const Context & context;
};

template <typename ToFieldType>
struct TiDBLastDayTransformerImpl
{
    static_assert(std::is_same_v<ToFieldType, DataTypeMyDate::FieldType>);
    static constexpr auto name = "tidbLastDay";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            bool is_null = false;
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB also considers NO_ZERO_DATE sql_mode. But sql_mode is not handled by TiFlash for now.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        UInt8 last_day = getLastDay(val.year, val.month);
        return MyDate(val.year, val.month, last_day).toPackedUInt();
    }
};

template <typename ToFieldType>
struct TiDBDayOfWeekTransformerImpl
{
    static constexpr auto name = "tidbDayOfWeek";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            bool is_null = false;
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB also considers NO_ZERO_DATE sql_mode. But sql_mode is not handled by TiFlash for now.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        /// Behavior differences from TiDB:
        /// for date in ['0000-01-01', '0000-03-01'), dayOfWeek is the same with MySQL, while TiDB is offset by one day
        /// In TiDB dayOfWeek('0000-01-01') = 7, in MySQL/TiFlash dayOfWeek('0000-01-01') = 1
        return static_cast<ToFieldType>(val.weekDay() + 1);
    }
};

template <typename ToFieldType>
struct TiDBDayOfYearTransformerImpl
{
    static constexpr auto name = "tidbDayOfYear";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            bool is_null = false;
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB also considers NO_ZERO_DATE sql_mode. But sql_mode is not handled by TiFlash for now.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        return static_cast<ToFieldType>(val.yearDay());
    }
};

template <typename ToFieldType>
struct TiDBWeekOfYearTransformerImpl
{
    static constexpr auto name = "tidbWeekOfYear";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        bool is_null = false;
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
            is_null = false;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB also considers NO_ZERO_DATE sql_mode. But sql_mode is not handled by TiFlash for now.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        /// Behavior differences from TiDB:
        /// for '0000-01-02', weekofyear is the same with MySQL, while TiDB is offset by one day
        /// TiDB_weekofyear('0000-01-02') = 52, MySQL/TiFlash_weekofyear('0000-01-02') = 1
        return static_cast<ToFieldType>(val.week(3));
    }
};

template <typename ToFieldType>
struct TiDBToSecondsTransformerImpl
{
    static constexpr auto name = "tidbToSeconds";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        bool is_null = false;
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
            is_null = false;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB returns normal value if one of month/day is zero for to_seconds function, while MySQL return null if either of them is zero.
        // TiFlash aligns with MySQL to align the behavior with other functions like last_day.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        return static_cast<ToFieldType>(calcSeconds(val.year, val.month, val.day, val.hour, val.minute, val.second));
    }
};

template <typename ToFieldType>
struct TiDBToDaysTransformerImpl
{
    static constexpr auto name = "tidbToDays";

    static void execute(
        const Context & context,
        const ColumnVector<DataTypeMyTimeBase::FieldType>::Container & vec_from,
        typename ColumnVector<ToFieldType>::Container & vec_to,
        typename ColumnVector<UInt8>::Container & vec_null_map)
    {
        bool is_null = false;
        for (size_t i = 0, sz = vec_from.size(); i < sz; ++i)
        {
            MyTimeBase val(vec_from[i]);
            vec_to[i] = execute(context, val, is_null);
            vec_null_map[i] = is_null;
            is_null = false;
        }
    }

    static ToFieldType execute(const Context & context, const MyTimeBase & val, bool & is_null)
    {
        // TiDB returns normal value if one of month/day is zero for to_seconds function, while MySQL return null if either of them is zero.
        // TiFlash aligns with MySQL to align the behavior with other functions like last_day.
        if (val.month == 0 || val.day == 0)
        {
            context.getDAGContext()->handleInvalidTime(
                fmt::format("Invalid time value: month({}) or day({}) is zero", val.month, val.day),
                Errors::Types::WrongValue);
            is_null = true;
            return 0;
        }
        return static_cast<ToFieldType>(calcDayNum(val.year, val.month, val.day));
    }
};

// Similar to FunctionDateOrDateTimeToSomething, but also handle nullable result and mysql sql mode.
template <typename ToDataType, template <typename> class Transformer, bool return_nullable>
class FunctionMyDateOrMyDateTimeToSomething : public IFunction
{
private:
    const Context & context;

public:
    using ToFieldType = typename ToDataType::FieldType;
    static constexpr auto name = Transformer<ToFieldType>::name;

    explicit FunctionMyDateOrMyDateTimeToSomething(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionMyDateOrMyDateTimeToSomething>(context);
    };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!arguments[0].type->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}. Should be a date or a date with time",
                    arguments[0].type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr return_type = std::make_shared<ToDataType>();
        if constexpr (return_nullable)
            return_type = makeNullable(return_type);
        return return_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const DataTypePtr & from_type = block.getByPosition(arguments[0]).type;

        if (from_type->isMyDateOrMyDateTime())
        {
            using FromFieldType = typename DataTypeMyTimeBase::FieldType;

            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();

            const size_t size = vec_from.size();
            auto col_to = ColumnVector<ToFieldType>::create(size);
            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();

            if constexpr (return_nullable)
            {
                ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(size, 0);
                ColumnUInt8::Container & vec_null_map = col_null_map->getData();
                Transformer<ToFieldType>::execute(context, vec_from, vec_to, vec_null_map);
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
            }
            else
            {
                Transformer<ToFieldType>::execute(context, vec_from, vec_to);
                block.getByPosition(result).column = std::move(col_to);
            }
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    block.getByPosition(arguments[0]).type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

static constexpr bool return_nullable = true;
static constexpr bool return_not_null = false;

using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;
using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;
using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;
using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;
using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfWeekImpl>;
using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;
using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToHourImpl>;
using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMinuteImpl>;
using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToSecondImpl>;
using FunctionToStartOfDay = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfDayImpl>;
using FunctionToMonday = FunctionDateOrDateTimeToSomething<DataTypeDate, ToMondayImpl>;
using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfMonthImpl>;
using FunctionToStartOfQuarter = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfQuarterImpl>;
using FunctionToStartOfYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfYearImpl>;
using FunctionToStartOfMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfMinuteImpl>;
using FunctionToStartOfFiveMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFiveMinuteImpl>;
using FunctionToStartOfFifteenMinutes
    = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFifteenMinutesImpl>;
using FunctionToStartOfHour = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfHourImpl>;
using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;
using FunctionToLastDay
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeMyDate, TiDBLastDayTransformerImpl, return_nullable>;
using FunctionToTiDBDayOfWeek
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeUInt16, TiDBDayOfWeekTransformerImpl, return_nullable>;
using FunctionToTiDBDayOfYear
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeUInt16, TiDBDayOfYearTransformerImpl, return_nullable>;
using FunctionToTiDBWeekOfYear
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeUInt16, TiDBWeekOfYearTransformerImpl, return_nullable>;
using FunctionToTiDBToSeconds
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeUInt64, TiDBToSecondsTransformerImpl, return_nullable>;
using FunctionToTiDBToDays
    = FunctionMyDateOrMyDateTimeToSomething<DataTypeUInt32, TiDBToDaysTransformerImpl, return_nullable>;
using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl>;
using FunctionToRelativeQuarterNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeQuarterNumImpl>;
using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMonthNumImpl>;
using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl>;
using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl>;
using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl>;
using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl>;
using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl>;

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;
using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;
using FunctionToYYYYMMDDhhmmss = FunctionDateOrDateTimeToSomething<DataTypeUInt64, ToYYYYMMDDhhmmssImpl>;
using FunctionToDayName = FunctionDateTimeToString<ToDayNameImpl>;
using FunctionToMonthName = FunctionDateTimeToString<ToMonthNameImpl>;

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;
using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;
using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;
using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;
using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;
using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;
using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

using FunctionSubtractSeconds = FunctionDateOrDateTimeAddInterval<SubtractSecondsImpl>;
using FunctionSubtractMinutes = FunctionDateOrDateTimeAddInterval<SubtractMinutesImpl>;
using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;
using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;
using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;
using FunctionSubtractMonths = FunctionDateOrDateTimeAddInterval<SubtractMonthsImpl>;
using FunctionSubtractYears = FunctionDateOrDateTimeAddInterval<SubtractYearsImpl>;

using FunctionSysDateWithFsp = FunctionSysDate<SysDateWithFsp>;
using FunctionSysDateWithoutFsp = FunctionSysDate<SysDateWithoutFsp>;

} // namespace DB
