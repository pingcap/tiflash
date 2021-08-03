#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <Common/typeid_cast.h>

#include <IO/WriteHelpers.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <common/DateLUT.h>

#include <Poco/String.h>

#include <type_traits>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static const Int64 SECOND_IN_ONE_DAY = 86400;
static const Int64 E6 = 1000000;

// day number per 400 years, from the year that year % 400 = 1
static const int DAY_NUM_PER_400_YEARS = 365 * 400 + 97;
// day number per 100 years in every 400 years, from the year that year % 100 = 1
// note the day number of the last 100 years should be DAY_NUM_PER_100_YEARS + 1
static const int DAY_NUM_PER_100_YEARS = 365 * 100 + 24;
// day number per 4 years in every 100 years, from the year that year % 4 = 1
// note the day number of the last 4 years should be DAY_NUM_PER_4_YEARS - 1
static const int DAY_NUM_PER_4_YEARS = 365 * 4 + 1;
// day number per years in every 4 years
// note the day number of the last 1 years maybe DAY_NUM_PER_YEARS + 1
static const int DAY_NUM_PER_YEARS = 365;

inline void fillMonthAndDay(int day_num, int & month, int & day, const int * accumulated_days_per_month)
{
    month = day_num / 31;
    if (accumulated_days_per_month[month] < day_num)
        month++;
    day = day_num - (month == 0 ? 0 : accumulated_days_per_month[month-1] + 1);
}

inline void fromDayNum(MyDateTime & t, int day_num)
{
    // day_num is the days from 0000-01-01
    if (day_num < 0)
        throw Exception("MyDate/MyDateTime only support date after 0000-01-01");
    int year = 0, month = 0, day = 0;
    if (likely(day_num >= 366))
    {
        // year 0000 is leap year
        day_num -= 366;

        int num_of_400_years = day_num / DAY_NUM_PER_400_YEARS;
        day_num = day_num % DAY_NUM_PER_400_YEARS;

        int num_of_100_years = day_num / DAY_NUM_PER_100_YEARS;
        // the day number of the last 100 years should be DAY_NUM_PER_100_YEARS + 1
        // so can not use day_num % DAY_NUM_PER_100_YEARS
        day_num = day_num - (num_of_100_years * DAY_NUM_PER_100_YEARS);

        int num_of_4_years = day_num / DAY_NUM_PER_4_YEARS;
        // can not use day_num % DAY_NUM_PER_4_YEARS
        day_num = day_num - (num_of_4_years * DAY_NUM_PER_4_YEARS);

        int num_of_years = day_num / DAY_NUM_PER_YEARS;
        // can not use day_num % DAY_NUM_PER_YEARS
        day_num = day_num - (num_of_years * DAY_NUM_PER_YEARS);

        year = 1 + num_of_400_years * 400 + num_of_100_years * 100 + num_of_4_years * 4 + num_of_years;
    }
    static const int ACCUMULATED_DAYS_PER_MONTH[] = {30,58,89,119,150,180,211,242,272,303,333,364};
    static const int ACCUMULATED_DAYS_PER_MONTH_LEAP_YEAR[] = {30,59,90,120,151,181,212,243,273,304,334,365};
    bool is_leap_year = year % 400 == 0 || (year % 4 == 0 && year % 100 != 0);
    fillMonthAndDay(day_num, month, day, is_leap_year ? ACCUMULATED_DAYS_PER_MONTH_LEAP_YEAR : ACCUMULATED_DAYS_PER_MONTH);
    t.year = year;
    t.month = month + 1;
    t.day = day + 1;
}

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
std::string extractTimeZoneNameFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);
const DateLUTImpl & extractTimeZoneFromFunctionArguments(Block & block, const ColumnNumbers & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);



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

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfDay", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
    static constexpr auto name = "toStartOfDay";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDate(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfDay", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfDay", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        return time_zone.toFirstDayNumOfWeek(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
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
        return time_zone.toFirstDayNumOfMonth(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfMonday", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        return time_zone.toFirstDayNumOfQuarter(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfQuarter", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        return time_zone.toFirstDayNumOfYear(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfYear", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
    static constexpr auto name = "toTime";

    /// When transforming to time, the date will be equated to 1970-01-02.
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toTime(t) + 86400;
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinuteImpl
{
    static constexpr auto name = "toStartOfFiveMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfFiveMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfFiveMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        throw Exception("Illegal type Date of argument for function toStartOfFifteenMinutes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfFifteenMinutes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t);
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
    static constexpr auto name = "toYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum_t(d));
    }
    static inline UInt16 execute(UInt64 packed, const DateLUTImpl &) { return UInt16((packed >> 46) / 13); }

    using FactorTransform = ZeroTransform;
};

struct ToQuarterImpl
{
    static constexpr auto name = "toQuarter";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 packed, const DateLUTImpl &) { return ((/* Month */ (packed >> 46) % 13) + 2) / 3; }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToMonthImpl
{
    static constexpr auto name = "toMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(DayNum_t(d));
    }
    // tidb date related type, ignore time_zone info
    static inline UInt8 execute(UInt64 t, const DateLUTImpl &) { return (UInt8)((t >> 46u) % 13); }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
    static constexpr auto name = "toDayOfMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 t, const DateLUTImpl & ) {
        return (UInt8)((t >> 41) & 31);
    }

    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toDayOfWeek", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToMondayImpl;
};

struct ToHourImpl
{
    static constexpr auto name = "toHour";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }

    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToMinuteImpl
{
    static constexpr auto name = "toMinute";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
    static constexpr auto name = "toSecond";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfMinuteImpl;
};

struct ToRelativeYearNumImpl
{
    static constexpr auto name = "toRelativeYearNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeYearNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeQuarterNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeMonthNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeWeekNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return static_cast<DayNum_t>(d);
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeDayNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};


struct ToRelativeHourNumImpl
{
    static constexpr auto name = "toRelativeHourNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeHourNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeMinuteNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeSecondNumImpl
{
    static constexpr auto name = "toRelativeSecondNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t;
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toRelativeSecondNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMImpl
{
    static constexpr auto name = "toYYYYMM";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(static_cast<DayNum_t>(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toYYYYMM", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDImpl
{
    static constexpr auto name = "toYYYYMMDD";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(static_cast<DayNum_t>(d));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toYYYYMMDD", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDhhmmssImpl
{
    static constexpr auto name = "toYYYYMMDDhhmmss";

    static inline UInt64 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(t);
    }
    static inline UInt64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(static_cast<DayNum_t>(d)));
    }
    static inline UInt8 execute(UInt64 , const DateLUTImpl & ) {
        throw Exception("Illegal type MyTime of argument for function toYYYYMMDDhhmmss", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};


template <typename FromType, typename ToType, typename Transform>
struct Transformer
{
    static void vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const DateLUTImpl & time_zone)
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
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
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

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 1)
        {
            if (!arguments[0].type->isDateOrDateTime())
                throw Exception{
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else if (arguments.size() == 2)
        {
            if (!checkDataType<DataTypeDateTime>(arguments[0].type.get())
                || !checkDataType<DataTypeString>(arguments[1].type.get()))
                throw Exception{
                    "Function " + getName() + " supports 1 or 2 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// For DateTime, if time zone is specified, attach it to type.
        if (std::is_same_v<ToDataType, DataTypeDateTime>)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
        else
            return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkDataType<DataTypeDate>(from_type))
            DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result);
        else if (checkDataType<DataTypeDateTime>(from_type))
            DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result);
        else if (checkDataType<DataTypeMyDateTime>(from_type) || checkDataType<DataTypeMyDate>(from_type))
            DateTimeTransformImpl<DataTypeMyTimeBase::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        IFunction::Monotonicity is_monotonic { true };
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
                ? is_monotonic : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), date_lut)
                == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), date_lut)
                ? is_monotonic : is_not_monotonic;
        }
    }
};

static inline void addDays(MyDateTime & t, Int64 days)
{
    Int32 current_days = calcDayNum(t.year, t.month, t.day);
    current_days += days;
    fromDayNum(t, current_days);
}

static inline void addMonths(MyDateTime & t, Int64 months)
{
    // month in my_time start from 1
    Int64 current_month = t.month -1;
    current_month += months;
    if (current_month >= 0)
    {
        Int64 year = current_month / 12;
        current_month = current_month % 12;
        t.year += year;
    }
    else
    {
        Int64 year = (-current_month) / 12;
        if((-current_month) % 12 != 0)
            year++;
        current_month += year * 12;
        t.year -= year;
    }
    static const int day_num_in_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    static const int day_num_in_month_leap_year[] = {31,29,31,30,31,30,31,31,30,31,30,31};
    int max_day = 0;
    if (t.year % 400 == 0 || (t.year % 100 != 0 && t.year % 4 == 0))
        max_day = day_num_in_month_leap_year[current_month];
    else
        max_day = day_num_in_month[current_month];
    t.month = current_month + 1;
    t.day = t.day > max_day ? max_day : t.day;
}

struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d)) + delta;
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl &)
    {
        // todo support zero date
        if (t == 0)
        {
            return t;
        }
        MyDateTime my_time(t);
        Int64 current_second = my_time.hour * 3600 + my_time.minute * 60 + my_time.second;
        current_second += delta;
        if (current_second >= 0)
        {
            Int64 days = current_second / SECOND_IN_ONE_DAY;
            current_second = current_second % SECOND_IN_ONE_DAY;
            if (days != 0)
                addDays(my_time, days);
        }
        else
        {
            Int64 days = (-current_second) / SECOND_IN_ONE_DAY;
            if ((-current_second) % SECOND_IN_ONE_DAY != 0)
            {
                days++;
            }
            current_second += days * SECOND_IN_ONE_DAY;
            addDays(my_time, -days);
        }
        my_time.hour = current_second / 3600;
        my_time.minute = (current_second % 3600) / 60;
        my_time.second = current_second % 60;
        return my_time.toPackedUInt();
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 60;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d)) + delta * 60;
    }
    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(t, delta * 60, time_zone);
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 3600;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d)) + delta * 3600;
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddSecondsImpl::execute(t, delta * 3600, time_zone);
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addDays(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta;
    }

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
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addWeeks(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta * 7;
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddDaysImpl::execute(t, delta * 7, time_zone);
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
        return time_zone.addMonths(DayNum_t(d), delta);
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
        return time_zone.addYears(DayNum_t(d), delta);
    }

    static inline UInt64 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return AddMonthsImpl::execute(t, delta * 12, time_zone);
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
};

struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl> { static constexpr auto name = "subtractSeconds"; };
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl> { static constexpr auto name = "subtractMinutes"; };
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl> { static constexpr auto name = "subtractHours"; };
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl> { static constexpr auto name = "subtractDays"; };
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl> { static constexpr auto name = "subtractWeeks"; };
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl> { static constexpr auto name = "subtractMonths"; };
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl> { static constexpr auto name = "subtractYears"; };


template <typename FromType, typename ToType, typename Transform>
struct Adder
{
    static void vector_vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta.getInt(i), time_zone);
    }

    static void vector_constant(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, Int64 delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta, time_zone);
    }

    static void constant_vector(const FromType & from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
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

        const DateLUTImpl & time_zone = use_utc_timezone ? DateLUT::instance("UTC") : extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                Op::vector_constant(sources->getData(), col_to->getData(), delta_const_column->getField().get<Int64>(), time_zone);
            else
                Op::vector_vector(sources->getData(), col_to->getData(), delta_column, time_zone);

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (const auto * sources = checkAndGetColumnConst<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::constant_vector(sources->template getValue<FromType>(), col_to->getData(), *block.getByPosition(arguments[1]).column, time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
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

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        //todo support string as tidb support string
        if (!arguments[1].type->isNumber())
            throw Exception("Second argument for function " + getName() + " (delta) must be number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!arguments[0].type->isDateOrDateTime())
                throw Exception{
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
        {
            if (!checkDataType<DataTypeDateTime>(arguments[0].type.get())
                || !checkDataType<DataTypeString>(arguments[2].type.get()))
                throw Exception{
                    "Function " + getName() + " supports 2 or 3 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument must be number. "
                    "The 3rd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (checkDataType<DataTypeDate>(arguments[0].type.get()))
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDate::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else if (checkDataType<DataTypeDateTime>(arguments[0].type.get()))
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDateTime::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else
        {
            // for MyDate and MyDateTime, according to TiDB implementation the return type is always return MyDateTime
            // todo consider the fsp in delta part
            int fsp = 0;
            const auto * my_datetime_type = checkAndGetDataType<DataTypeMyDateTime>(arguments[0].type.get());
            if(my_datetime_type != nullptr)
                fsp = my_datetime_type->getFraction();
            return std::make_shared<DataTypeMyDateTime>(fsp);
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkDataType<DataTypeDate>(from_type))
            DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform, false>::execute(block, arguments, result);
        else if (checkDataType<DataTypeDateTime>(from_type))
            DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform, false>::execute(block, arguments, result);
        else if (checkDataType<DataTypeMyDate>(from_type) || checkDataType<DataTypeMyDateTime>(from_type))
            DateTimeAddIntervalImpl<DataTypeMyTimeBase::FieldType, Transform, true>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

class FunctionTiDBTimestampDiff : public IFunction
{
public:
    static constexpr auto name = "tidbTimestampDiff";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBTimestampDiff>(); };

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!removeNullable(arguments[0])->isString())
            throw Exception("First argument for function " + getName() + " (unit) must be String",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if(!checkDataType<DataTypeMyDateTime>(removeNullable(arguments[1]).get()) &&
           !checkDataType<DataTypeMyDate>(removeNullable(arguments[1]).get()) &&
           !arguments[1]->onlyNull())
            throw Exception("Second argument for function " + getName() + " must be MyDate or MyDateTime",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if(!checkDataType<DataTypeMyDateTime>(removeNullable(arguments[2]).get()) &&
           !checkDataType<DataTypeMyDate>(removeNullable(arguments[2]).get()) &&
           !arguments[2]->onlyNull())
            throw Exception("Third argument for function " + getName() + " must be MyDate or MyDateTime",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // to align with tidb, timestampdiff with zeroDate input should return null, so always return nullable type
        return makeNullable(std::make_shared<DataTypeInt64>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception("First argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        String unit = Poco::toLower(unit_column->getValue<String>());

        bool has_nullable = false;
        bool has_null_constant = false;
        for(const auto & arg : arguments)
        {
            const auto & elem = block.getByPosition(arg);
            has_nullable |= elem.type->isNullable();
            has_null_constant |= elem.type->onlyNull();
        }

        if (has_null_constant)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
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
            dispatchForColumns<MonthDiffCalculatorImpl, YearDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "quarter")
            dispatchForColumns<MonthDiffCalculatorImpl, QuarterDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "month")
            dispatchForColumns<MonthDiffCalculatorImpl, MonthDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "week")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, WeekDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "day")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, DayDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "hour")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, HourDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "minute")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, MinuteDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "second")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, SecondDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else if (unit == "microsecond")
            dispatchForColumns<DummyMonthDiffCalculatorImpl, MicroSecondDiffResultCalculator>(x, y, res->getData(), result_null_map->getData());
        else
            throw Exception("Function " + getName() + " does not support '" + unit + "' unit", ErrorCodes::BAD_ARGUMENTS);
        // warp null

        if (block.getByPosition(arguments[1]).type->isNullable()
        || block.getByPosition(arguments[2]).type->isNullable())
        {
            ColumnUInt8::Container &vec_result_null_map = result_null_map->getData();
            ColumnPtr x_p = block.getByPosition(arguments[1]).column;
            ColumnPtr y_p = block.getByPosition(arguments[2]).column;
            for (size_t i = 0; i < rows; i++) {
                vec_result_null_map[i] |= (x_p->isNullAt(i) || y_p->isNullAt(i));
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }

private:
    template <typename MonthDiffCalculator, typename ResultCalculator>
    void dispatchForColumns(const IColumn & x, const IColumn & y, ColumnInt64::Container & res, ColumnUInt8::Container & res_null_map)
    {
        auto * x_const = checkAndGetColumnConst<ColumnUInt64>(&x);
        auto * y_const = checkAndGetColumnConst<ColumnUInt64>(&y);
        if(x_const)
        {
            auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            constant_vector<MonthDiffCalculator, ResultCalculator>(x_const->getValue<UInt64>(), *y_vec, res, res_null_map);
        }
        else if (y_const)
        {
            auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            vector_constant<MonthDiffCalculator, ResultCalculator>(*x_vec, y_const->getValue<UInt64>(), res, res_null_map);
        }
        else
        {
            auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            vector_vector<MonthDiffCalculator, ResultCalculator>(*x_vec, *y_vec, res, res_null_map);
        }
    }

    template <typename MonthDiffCalculator, typename ResultCalculator>
    void vector_vector(const ColumnVector<UInt64> & x, const ColumnVector<UInt64> & y,
            ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
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
    void vector_constant(const ColumnVector<UInt64> & x, UInt64 y,
            ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
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
    void constant_vector(UInt64 x, const ColumnVector<UInt64> & y,
            ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
    {
        const auto & y_data = y.getData();
        if (x == 0)
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = y.size(); i < size; ++i) {
                result_null_map[i] = (y_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate<MonthDiffCalculator, ResultCalculator>(x, y_data[i]);
            }
        }
    }

    void calculateTimeDiff(const MyDateTime &x, const MyDateTime &y, Int64 & seconds, int & micro_seconds, bool & neg) {
        Int64 days_x = calcDayNum(x.year, x.month, x.day);
        Int64 days_y = calcDayNum(y.year, y.month, y.day);
        Int64 days = days_y - days_x;

        Int64 tmp = (days * SECOND_IN_ONE_DAY + y.hour * 3600LL +
                y.minute * 60LL + y.second - (x.hour * 3600LL + x.minute * 60LL + x.second)) * E6 +
                y.micro_second - x.micro_second;
        if(tmp < 0)
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
            if(neg)
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
            if (month_end < month_begin ||
                (month_end == month_begin && day_end < day_begin))
            {
                years--;
            }

            // calc months
            months = 12 * years;
            if (month_end < month_begin ||
                (month_end == month_begin && day_end < day_begin))
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
            else if ((day_end == day_begin) &&
                       ((second_end < second_begin) ||
                        (second_end == second_begin && micro_second_end < micro_second_begin)))
            {
                months--;
            }
            return months;
        }
    };

    struct DummyMonthDiffCalculatorImpl
    {
        static inline UInt32 execute(const MyDateTime & , const MyDateTime & , const bool )
        {
            return 0;
        }
    };

    struct YearDiffResultCalculator
    {
        static inline Int64 execute(const Int64 , const Int64 , const Int64 months, const int neg_value)
        {
            return months / 12 * neg_value;
        }
    };

    struct QuarterDiffResultCalculator
    {
        static inline Int64 execute(const Int64 , const Int64 , const Int64 months, const int neg_value)
        {
            return months / 3 * neg_value;
        }
    };

    struct MonthDiffResultCalculator
    {
        static inline Int64 execute(const Int64 , const Int64 , const Int64 months, const int neg_value)
        {
            return months * neg_value;
        }
    };

    struct WeekDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 , const Int64 , const int neg_value)
        {
            return seconds / SECOND_IN_ONE_DAY / 7 * neg_value;
        }
    };

    struct DayDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 , const Int64 , const int neg_value)
        {
            return seconds / SECOND_IN_ONE_DAY * neg_value;
        }
    };

    struct HourDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 , const Int64 , const int neg_value)
        {
            return seconds / 3600 * neg_value;
        }
    };

    struct MinuteDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 , const Int64 , const int neg_value)
        {
            return seconds / 60 * neg_value;
        }
    };

    struct SecondDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 , const Int64 , const int neg_value)
        {
            return seconds * neg_value;
        }
    };

    struct MicroSecondDiffResultCalculator
    {
        static inline Int64 execute(const Int64 seconds, const Int64 micro_seconds, const Int64 , const int neg_value)
        {
            return (seconds * E6 + micro_seconds) * neg_value;
        }
    };

    template <typename MonthDiffCalculator, typename ResultCalculator>
    Int64 calculate(UInt64 x, UInt64 y)
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

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if(!removeNullable(arguments[0]).get()->isDateOrDateTime())
            throw Exception("First argument for function " + getName() + " must be MyDate or MyDateTime",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if(!removeNullable(arguments[1]).get()->isDateOrDateTime())
            throw Exception("Second argument for function " + getName() + " must be MyDate or MyDateTime",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // to align with tidb, dateDiff with zeroDate input should return null, so always return nullable type
        return makeNullable(std::make_shared<DataTypeInt64>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        bool has_nullable = false;
        bool has_null_constant = false;
        for(const auto & arg : arguments)
        {
            const auto & elem = block.getByPosition(arg);
            has_nullable |= elem.type->isNullable();
            has_null_constant |= elem.type->onlyNull();
        }

        if (has_null_constant)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
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
            ColumnUInt8::Container &vec_result_null_map = result_null_map->getData();
            ColumnPtr x_p = block.getByPosition(arguments[0]).column;
            ColumnPtr y_p = block.getByPosition(arguments[1]).column;
            for (size_t i = 0; i < rows; i++) {
                vec_result_null_map[i] |= (x_p->isNullAt(i) || y_p->isNullAt(i));
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }
private:
    void dispatch(const IColumn & x, const IColumn & y, ColumnInt64::Container & res, ColumnUInt8::Container & res_null_map)
    {
        auto * x_const = checkAndGetColumnConst<ColumnUInt64>(&x);
        auto * y_const = checkAndGetColumnConst<ColumnUInt64>(&y);
        if(x_const)
        {
            auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            constant_vector(x_const->getValue<UInt64>(), *y_vec, res, res_null_map);
        }
        else if (y_const)
        {
            auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            vector_constant(*x_vec, y_const->getValue<UInt64>(), res, res_null_map);
        }
        else
        {
            auto * x_vec = checkAndGetColumn<ColumnUInt64>(&x);
            auto * y_vec = checkAndGetColumn<ColumnUInt64>(&y);
            vector_vector(*x_vec, *y_vec, res, res_null_map);
        }
    }

    void vector_vector(const ColumnVector<UInt64> & x, const ColumnVector<UInt64> & y,
                       ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
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

    void vector_constant(const ColumnVector<UInt64> & x, UInt64 y,
                         ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
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

    void constant_vector(UInt64 x, const ColumnVector<UInt64> & y,
                         ColumnInt64::Container & result, ColumnUInt8::Container & result_null_map)
    {
        const auto & y_data = y.getData();
        if (x == 0)
        {
            for (size_t i = 0, size = y.size(); i < size; ++i)
                result_null_map[i] = 1;
        }
        else
        {
            for (size_t i = 0, size = y.size(); i < size; ++i) {
                result_null_map[i] = (y_data[i] == 0);
                if (!result_null_map[i])
                    result[i] = calculate(x, y_data[i]);
            }
        }
    }

    Int64 calculate(UInt64 x_packed, UInt64 y_packed)
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

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isString())
            throw Exception("First argument for function " + getName() + " (unit) must be String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isDateOrDateTime())
            throw Exception("Second argument for function " + getName() + " must be Date or DateTime",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isDateOrDateTime())
            throw Exception("Third argument for function " + getName() + " must be Date or DateTime",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 4 && !arguments[3]->isString())
            throw Exception("Fourth argument for function " + getName() + " (timezone) must be String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 3}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception("First argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

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
            throw Exception("Function " + getName() + " does not support '" + unit + "' unit", ErrorCodes::BAD_ARGUMENTS);

        block.getByPosition(result).column = std::move(res);
    }

private:
    template <typename Transform>
    void dispatchForColumns(
        const IColumn & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * x_vec = checkAndGetColumn<ColumnUInt16>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (auto * x_vec = checkAndGetColumn<ColumnUInt32>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (auto * x_const = checkAndGetColumnConst<ColumnUInt16>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt16>(), y, timezone_x, timezone_y, result);
        else if (auto * x_const = checkAndGetColumnConst<ColumnUInt32>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt32>(), y, timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for first argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchForSecondColumn(
        const ColumnVector<T1> & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            vector_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            vector_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_const = checkAndGetColumnConst<ColumnUInt16>(&y))
            vector_constant<Transform>(x, y_const->getValue<UInt16>(), timezone_x, timezone_y, result);
        else if (auto * y_const = checkAndGetColumnConst<ColumnUInt32>(&y))
            vector_constant<Transform>(x, y_const->getValue<UInt32>(), timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for second argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchConstForSecondColumn(
        T1 x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            constant_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            constant_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for second argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1, typename T2>
    void vector_vector(
        const ColumnVector<T1> & x, const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void vector_constant(
        const ColumnVector<T1> & x, T2 y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & x_data = x.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y, timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void constant_vector(
        T1 x, const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & y_data = y.getData();
        for (size_t i = 0, size = y.size(); i < size; ++i)
            result[i] = calculate<Transform>(x, y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    Int64 calculate(T1 x, T2 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y)
    {
        return Int64(Transform::execute(y, timezone_y))
             - Int64(Transform::execute(x, timezone_x));
    }
};


/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = "now";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt32().createColumnConst(
            block.rows(),
            static_cast<UInt64>(time(nullptr)));
    }
};


class FunctionToday : public IFunction
{
public:
    static constexpr auto name = "today";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToday>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt16().createColumnConst(
            block.rows(),
            UInt64(DateLUT::instance().toDayNum(time(nullptr))));
    }
};


class FunctionYesterday : public IFunction
{
public:
    static constexpr auto name = "yesterday";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionYesterday>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt16().createColumnConst(
            block.rows(),
            UInt64(DateLUT::instance().toDayNum(time(nullptr)) - 1));
    }
};

class FunctionMyTimeZoneConvertByOffset : public IFunction
{
    using FromFieldType = typename DataTypeMyDateTime::FieldType;
    using ToFieldType = typename DataTypeMyDateTime::FieldType;
public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMyTimeZoneConvertByOffset>(); };
    static constexpr auto name = "ConvertTimeZoneByOffset";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override {return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }


    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(arguments.size()) + ", should be 2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeMyDateTime>(arguments[0].type.get()))
            throw Exception{
                    "Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() +
                    ". Should be MyDateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!arguments[1].type->isInteger())
            throw Exception{
                    "Illegal type " + arguments[1].type->getName() + " of second argument of function " + getName() +
                    ". Should be Integer type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return arguments[0].type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override {
        static const DateLUTImpl & UTC = DateLUT::instance("UTC");
        if (const ColumnVector<FromFieldType> *col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get())) {
            auto col_to = ColumnVector<ToFieldType>::create();
            const typename ColumnVector<FromFieldType>::Container &vec_from = col_from->getData();
            typename ColumnVector<ToFieldType>::Container &vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            const auto offset_col = block.getByPosition(arguments.back()).column.get();
            if (!offset_col->isColumnConst())
                throw Exception{
                        "Second argument of function " + getName() + " must be an integral constant",
                        ErrorCodes::ILLEGAL_COLUMN};

            const auto offset = offset_col->getInt(0);
            for (size_t i = 0; i < size; ++i) {
                UInt64 result_time = vec_from[i] + offset;
                // todo maybe affected by daytime saving, need double check
                convertTimeZoneByOffset(vec_from[i], result_time, offset, UTC);
                vec_to[i] = result_time;
            }

            block.getByPosition(result).column = std::move(col_to);
        } else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + " of first argument of function " + name,
                            ErrorCodes::ILLEGAL_COLUMN);
    }

};
template <bool convert_from_utc>
class FunctionMyTimeZoneConverter : public IFunction
{
    using FromFieldType = typename DataTypeMyDateTime::FieldType;
    using ToFieldType = typename DataTypeMyDateTime::FieldType;
public:
    static constexpr auto name = convert_from_utc ? "ConvertTimeZoneFromUTC": "ConvertTimeZoneToUTC";
    static FunctionPtr create(const Context &) {return std::make_shared<FunctionMyTimeZoneConverter>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeMyDateTime>(arguments[0].type.get()))
            throw Exception{
                "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be MyDateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return arguments[0].type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
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
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of first argument of function " + name,
            ErrorCodes::ILLEGAL_COLUMN);
    }
};

/// Just changes time zone information for data type. The calculation is free.
class FunctionToTimeZone : public IFunction
{
public:
    static constexpr auto name = "toTimeZone";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToTimeZone>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!checkDataType<DataTypeDateTime>(arguments[0].type.get()))
            throw Exception{
                "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be DateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        return std::make_shared<DataTypeDateTime>(time_zone_name);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};


class FunctionTimeSlot : public IFunction
{
public:
    static constexpr auto name = "timeSlot";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlot>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeDateTime>(arguments[0].get()))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeDateTime>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (const ColumnUInt32 * times = typeid_cast<const ColumnUInt32 *>(block.getByPosition(arguments[0]).column.get()))
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
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename DurationType>
struct TimeSlotsImpl
{
    static void vector_vector(
        const PaddedPODArray<UInt32> & starts, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vector_constant(
        const PaddedPODArray<UInt32> & starts, DurationType duration,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
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

    static void constant_vector(
        UInt32 start, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
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

    static void constant_constant(
        UInt32 start, DurationType duration,
        Array & result)
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

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeDateTime>(arguments[0].get()))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt32>(arguments[1].get()))
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto starts = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());
        auto const_starts = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());

        auto durations = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());
        auto const_durations = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getValue<UInt32>(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constant_vector(const_starts->getValue<UInt32>(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && const_durations)
        {
            Array const_res;
            TimeSlotsImpl<UInt32>::constant_constant(const_starts->getValue<UInt32>(), const_durations->getValue<UInt32>(), const_res);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), const_res);
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
                    + ", " + block.getByPosition(arguments[1]).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct ExtractMyDateTimeImpl
{
    static Int64 extract_year(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToYearImpl::execute(packed, lut);
    }

    static Int64 extract_quater(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToQuarterImpl::execute(packed, lut);
    }

    static Int64 extract_month(UInt64 packed)
    {
        static const auto & lut = DateLUT::instance();
        return ToMonthImpl::execute(packed, lut);
    }

    static Int64 extract_week(UInt64 packed)
    {
        MyDateTime datetime(packed);
        return datetime.week(0);
    }

    static Int64 extract_day(UInt64 packed) { return (packed >> 41) & ((1 << 5) - 1); }

    static Int64 extract_day_microsecond(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        Int64 s = datetime.second;
        return (day * 1000000 + h * 10000 + m * 100 + s) * 1000000 + datetime.micro_second;
    }

    static Int64 extract_day_second(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        Int64 s = datetime.second;
        return day * 1000000 + h * 10000 + m * 100 + s;
    }

    static Int64 extract_day_minute(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        Int64 m = datetime.minute;
        return day * 10000 + h * 100 + m;
    }

    static Int64 extract_day_hour(UInt64 packed)
    {
        MyDateTime datetime(packed);
        Int64 day = datetime.day;
        Int64 h = datetime.hour;
        return day * 100 + h;
    }

    static Int64 extract_year_month(UInt64 packed)
    {
        Int64 y = extract_year(packed);
        Int64 m = extract_month(packed);
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
            throw TiFlashException("First argument for function " + getName() + " (unit) must be String", Errors::Coprocessor::BadRequest);

        // TODO: Support Extract from string, see https://github.com/pingcap/tidb/issues/22700
        // if (!(arguments[1]->isString() || arguments[1]->isDateOrDateTime()))
        if (!arguments[1]->isMyDateOrMyDateTime())
            throw TiFlashException(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be DateOrDateTime.",
                Errors::Coprocessor::BadRequest);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw TiFlashException(
                "First argument for function " + getName() + " must be constant String", Errors::Coprocessor::BadRequest);

        String unit = Poco::toLower(unit_column->getValue<String>());

        auto from_column = block.getByPosition(arguments[1]).column;

        size_t rows = block.rows();
        auto col_to = ColumnInt64::create(rows);
        auto & vec_to = col_to->getData();

        if (unit == "year")
            dispatch<ExtractMyDateTimeImpl::extract_year>(from_column, vec_to);
        else if (unit == "quarter")
            dispatch<ExtractMyDateTimeImpl::extract_quater>(from_column, vec_to);
        else if (unit == "month")
            dispatch<ExtractMyDateTimeImpl::extract_month>(from_column, vec_to);
        else if (unit == "week")
            dispatch<ExtractMyDateTimeImpl::extract_week>(from_column, vec_to);
        else if (unit == "day")
            dispatch<ExtractMyDateTimeImpl::extract_day>(from_column, vec_to);
        else if (unit == "day_microsecond")
            dispatch<ExtractMyDateTimeImpl::extract_day_microsecond>(from_column, vec_to);
        else if (unit == "day_second")
            dispatch<ExtractMyDateTimeImpl::extract_day_second>(from_column, vec_to);
        else if (unit == "day_minute")
            dispatch<ExtractMyDateTimeImpl::extract_day_minute>(from_column, vec_to);
        else if (unit == "day_hour")
            dispatch<ExtractMyDateTimeImpl::extract_day_hour>(from_column, vec_to);
        else if (unit == "year_month")
            dispatch<ExtractMyDateTimeImpl::extract_year_month>(from_column, vec_to);
        /// TODO: support ExtractDuration
        // else if (unit == "hour");
        // else if (unit == "minute");
        // else if (unit == "second");
        // else if (unit == "microsecond");
        // else if (unit == "second_microsecond");
        // else if (unit == "minute_microsecond");
        // else if (unit == "minute_second");
        // else if (unit == "hour_microsecond");
        // else if (unit == "hour_second");
        // else if (unit == "hour_minute");
        else
            throw TiFlashException("Function " + getName() + " does not support '" + unit + "' unit", Errors::Coprocessor::BadRequest);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    using Func = Int64 (*)(UInt64);

    template <Func F>
    static void dispatch(const ColumnPtr col_from, PaddedPODArray<Int64> & vec_to)
    {
        if (const auto * from = checkAndGetColumn<ColumnString>(col_from.get()); from)
        {
            const auto & data = from->getChars();
            const auto & offsets = from->getOffsets();
            if (checkColumnConst<ColumnString>(from))
            {
                StringRef string_ref(data.data(), offsets[0] - 1);
                constant_string<F>(string_ref, from->size(), vec_to);
            }
            else
            {
                vector_string<F>(data, offsets, vec_to);
            }
        }
        else if (const auto * from = checkAndGetColumn<ColumnUInt64>(col_from.get()); from)
        {
            const auto & data = from->getData();
            if (checkColumnConst<ColumnUInt64>(from))
            {
                constant_datetime<F>(from->getUInt(0), from->size(), vec_to);
            }
            else
            {
                vector_datetime<F>(data, vec_to);
            }
        }
    }

    template <Func F>
    static void constant_string(const StringRef & from, size_t size, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(size);
        auto from_value = get<UInt64>(parseMyDateTime(from.toString()));
        for (size_t i = 0; i < size; ++i)
        {
            vec_to[i] = F(from_value);
        }
    }

    template <Func F>
    static void vector_string(
        const ColumnString::Chars_t & vec_from, const ColumnString::Offsets & offsets_from, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(offsets_from.size() + 1);
        size_t current_offset = 0;
        for (size_t i = 0; i < offsets_from.size(); i++)
        {
            size_t next_offset = offsets_from[i];
            size_t string_size = next_offset - current_offset - 1;
            StringRef string_value(&vec_from[current_offset], string_size);
            auto packed_value = get<UInt64>(parseMyDateTime(string_value.toString()));
            vec_to[i] = F(packed_value);
            current_offset = next_offset;
        }
    }

    template <Func F>
    static void constant_datetime(const UInt64 & from, size_t size, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            vec_to[i] = F(from);
        }
    }

    template <Func F>
    static void vector_datetime(const ColumnUInt64::Container & vec_from, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(vec_from.size());
        for (size_t i = 0; i < vec_from.size(); i++)
        {
            vec_to[i] = F(vec_from[i]);
        }
    }
};


using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;
using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;
using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;
using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;
using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfWeekImpl>;
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
using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFifteenMinutesImpl>;
using FunctionToStartOfHour = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfHourImpl>;
using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;

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

}
