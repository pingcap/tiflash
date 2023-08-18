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

#include <Common/MyDuration.h>
#include <Core/Field.h>
#include <Interpreters/TimezoneInfo.h>
#include <common/DateLUTImpl.h>

struct StringRef;
namespace DB
{
struct MyTimeBase
{
    static constexpr Int64 SECOND_IN_ONE_DAY = 86400;
    static constexpr Int64 SECOND_IN_ONE_HOUR = 3600;
    static constexpr Int64 SECOND_IN_ONE_MINUTE = 60;


    // copied from https://github.com/pingcap/tidb/blob/master/types/time.go
    // Core time bit fields.
    static const UInt64 YEAR_BIT_FIELD_OFFSET = 50, YEAR_BIT_FIELD_WIDTH = 14;
    static const UInt64 MONTH_BIT_FIELD_OFFSET = 46, MONTH_BIT_FIELD_WIDTH = 4;
    static const UInt64 DAY_BIT_FIELD_OFFSET = 41, DAY_BIT_FIELD_WIDTH = 5;
    static const UInt64 HOUR_BIT_FIELD_OFFSET = 36, HOUR_BIT_FIELD_WIDTH = 5;
    static const UInt64 MINUTE_BIT_FIELD_OFFSET = 30, MINUTE_BIT_FIELD_WIDTH = 6;
    static const UInt64 SECOND_BIT_FIELD_OFFSET = 24, SECOND_BIT_FIELD_WIDTH = 6;
    static const UInt64 MICROSECOND_BIT_FIELD_OFFSET = 4, MICROSECOND_BIT_FIELD_WIDTH = 20;
    // fspTt bit field.
    // `fspTt` format:
    // | fsp: 3 bits | type: 1 bit |
    // When `fsp` is valid (in range [0, 6]):
    // 1. `type` bit 0 represent `DateTime`
    // 2. `type` bit 1 represent `Timestamp`
    //
    // Since s`Date` does not require `fsp`, we could use `fspTt` == 0b1110 to represent it.
    static const UInt64 FSPTT_BIT_FIELD_OFFSET = 0, FSPTT_BIT_FIELD_WIDTH = 4;

    static const UInt64 YEAR_BIT_FIELD_MASK = ((1ull << YEAR_BIT_FIELD_WIDTH) - 1) << YEAR_BIT_FIELD_OFFSET;
    static const UInt64 MONTH_BIT_FIELD_MASK = ((1ull << MONTH_BIT_FIELD_WIDTH) - 1) << MONTH_BIT_FIELD_OFFSET;
    static const UInt64 DAY_BIT_FIELD_MASK = ((1ull << DAY_BIT_FIELD_WIDTH) - 1) << DAY_BIT_FIELD_OFFSET;
    static const UInt64 HOUR_BIT_FIELD_MASK = ((1ull << HOUR_BIT_FIELD_WIDTH) - 1) << HOUR_BIT_FIELD_OFFSET;
    static const UInt64 MINUTE_BIT_FIELD_MASK = ((1ull << MINUTE_BIT_FIELD_WIDTH) - 1) << MINUTE_BIT_FIELD_OFFSET;
    static const UInt64 SECOND_BIT_FIELD_MASK = ((1ull << SECOND_BIT_FIELD_WIDTH) - 1) << SECOND_BIT_FIELD_OFFSET;
    static const UInt64 MICROSECOND_BIT_FIELD_MASK = ((1ull << MICROSECOND_BIT_FIELD_WIDTH) - 1)
        << MICROSECOND_BIT_FIELD_OFFSET;
    static const UInt64 FSPTT_BIT_FIELD_MASK = ((1ull << FSPTT_BIT_FIELD_WIDTH) - 1) << FSPTT_BIT_FIELD_OFFSET;

    static const UInt64 FSPTT_FOR_DATE = 0b1110;
    static const UInt64 FSP_BIT_FIELD_MASK = 0b1110;
    static const UInt64 CORE_TIME_BIT_FIELD_MASK = ~FSPTT_BIT_FIELD_MASK;

    static const UInt64 YMD_MASK = ~((1ull << 41) - 1);

    // weekBehaviourMondayFirst set Monday as first day of week; otherwise Sunday is first day of week
    static const UInt32 WEEK_BEHAVIOR_MONDAY_FIRST = 1;
    // If set, Week is in range 1-53, otherwise Week is in range 0-53.
    // Note that this flag is only relevant if WEEK_JANUARY is not set
    static const UInt32 WEEK_BEHAVIOR_YEAR = 2;
    // If not set, Weeks are numbered according to ISO 8601:1988.
    // If set, the week that contains the first 'first-day-of-week' is week 1.
    static const UInt32 WEEK_BEHAVIOR_FIRST_WEEKDAY = 4;

    enum MyTimeType : UInt8
    {
        TypeDate = 0,
        TypeDateTime,
        TypeTimeStamp,
        TypeDuration
    };

    UInt16 year; // year <= 9999
    UInt8 month; // month <= 12
    UInt8 day; // day <= 31
    // When it's type is Time, HH:MM:SS may be 839:59:59 to -839:59:59, so use int16 to avoid overflow
    Int16 hour;
    UInt8 minute;
    UInt8 second;
    UInt32 micro_second; // ms second <= 999999

    MyTimeBase() = default;
    explicit MyTimeBase(UInt64 packed);
    MyTimeBase(
        UInt16 year_,
        UInt8 month_,
        UInt8 day_,
        UInt16 hour_,
        UInt8 minute_,
        UInt8 second_,
        UInt32 micro_second_);

    UInt64 toPackedUInt() const;
    UInt64 toCoreTime() const;

    // DateFormat returns a textual representation of the time value formatted
    // according to layout
    // See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
    void dateFormat(const String & layout, String & result) const;

    // returns the week day of current date(0 as sunday)
    int weekDay() const;
    // returns the week day name of current date, return empty string if invalid
    const String & weekDayName() const;
    const String & monthName() const;
    // the following methods are port from TiDB
    int yearDay() const;

    int week(UInt32 mode) const;

    std::tuple<int, int> calcWeek(UInt32 mode) const;

    // Check validity of time under specified SQL_MODE.
    // return false if time is invalid
    bool isValid(bool allow_zero_in_date, bool allow_invalid_date) const;
};

struct MyDateTime : public MyTimeBase
{
    explicit MyDateTime(UInt64 packed)
        : MyTimeBase(packed)
    {}

    MyDateTime(UInt16 year_, UInt8 month_, UInt8 day_, UInt16 hour_, UInt8 minute_, UInt8 second_, UInt32 micro_second_)
        : MyTimeBase(year_, month_, day_, hour_, minute_, second_, micro_second_)
    {}

    String toString(int fsp) const;

    MyDuration convertToMyDuration(int fsp);

    static MyDateTime getSystemDateTimeByTimezone(const TimezoneInfo &, UInt8 fsp);
};

struct MyDate : public MyTimeBase
{
    explicit MyDate(UInt64 packed)
        : MyTimeBase(packed)
    {}

    MyDate(UInt16 year_, UInt8 month_, UInt8 day_)
        : MyTimeBase(year_, month_, day_, 0, 0, 0, 0)
    {}

    String toString() const
    {
        String result;
        dateFormat("%Y-%m-%d", result);
        return result;
    }
};

bool numberToDateTime(Int64 number, MyDateTime & result, bool allowZeroDate = true);

struct MyDateTimeFormatter
{
    std::vector<std::function<void(const MyTimeBase & datetime, String & result)>> formatters;
    explicit MyDateTimeFormatter(const String & layout_);
    void format(const MyTimeBase & datetime, String & result)
    {
        for (auto & f : formatters)
        {
            f(datetime, result);
        }
    }
};

struct MyDateTimeParser
{
    explicit MyDateTimeParser(String format_);

    std::optional<UInt64> parseAsPackedUInt(const StringRef & str_view) const;

    struct Context;

private:
    const String format;

    // Parsing method. Parse from ctx.view[ctx.pos].
    // If success, update `datetime`, `ctx` and return true.
    // If fails, return false.
    using ParserCallback = std::function<bool(MyDateTimeParser::Context & ctx, MyTimeBase & datetime)>;
    std::vector<ParserCallback> parsers;
};

bool checkTimeValid(Int32 year, Int32 month, Int32 day, Int32 hour, Int32 minute, Int32 second);
bool checkTimeValidAllowMonthAndDayZero(Int32 year, Int32 month, Int32 day, Int32 hour, Int32 minute, Int32 second);
bool noNeedCheckTime(Int32, Int32, Int32, Int32, Int32, Int32);

using CheckTimeFunc = std::function<bool(Int32, Int32, Int32, Int32, Int32, Int32)>;

static const int8_t DefaultFsp = 6;
static bool DefaultIsFloat = false;
static CheckTimeFunc DefaultCheckTimeFunc = noNeedCheckTime;

Field parseMyDateTime(const String & str, int8_t fsp = DefaultFsp, CheckTimeFunc checkTimeFunc = DefaultCheckTimeFunc);
Field parseMyDateTimeFromFloat(
    const String & str,
    int8_t fsp = DefaultFsp,
    CheckTimeFunc checkTimeFunc = DefaultCheckTimeFunc);
std::pair<Field, bool> parseMyDateTimeAndJudgeIsDate(
    const String & str,
    int8_t fsp = DefaultFsp,
    CheckTimeFunc checkTimeFunc = DefaultCheckTimeFunc,
    bool isFloat = DefaultIsFloat);

Field parseMyDuration(const String & str, int8_t fsp = DefaultFsp);

void convertTimeZone(
    UInt64 from_time,
    UInt64 & to_time,
    const DateLUTImpl & time_zone_from,
    const DateLUTImpl & time_zone_to,
    bool throw_exception = false);

void convertTimeZoneByOffset(
    UInt64 from_time,
    UInt64 & to_time,
    bool from_utc,
    Int64 offset,
    bool throw_exception = false);

MyDateTime convertUTC2TimeZone(time_t utc_ts, UInt32 micro_second, const DateLUTImpl & time_zone_to);

MyDateTime convertUTC2TimeZoneByOffset(time_t utc_ts, UInt32 micro_second, Int64 offset);

std::pair<time_t, UInt32> roundTimeByFsp(time_t second, UInt64 nano_second, UInt8 fsp);

int calcDayNum(int year, int month, int day);
void fromDayNum(MyDateTime & t, int day_num);

// returns seconds since '0000-00-00'
UInt64 calcSeconds(int year, int month, int day, int hour, int minute, int second);

size_t maxFormattedDateTimeStringLength(const String & format);

inline time_t getEpochSecond(const MyDateTime & my_time, const DateLUTImpl & time_zone)
{
    return time_zone
        .makeDateTime(my_time.year, my_time.month, my_time.day, my_time.hour, my_time.minute, my_time.second);
}

bool isValidSeparator(char c, int previous_parts);

// Build CoreTime value with checking overflow of internal bit fields, return true if input is invalid.
// Note that this function will not check if the input is logically a valid datetime value.
bool toCoreTimeChecked(
    const UInt64 & year,
    const UInt64 & month,
    const UInt64 & day,
    const UInt64 & hour,
    const UInt64 & minute,
    const UInt64 & second,
    const UInt64 & microsecond,
    MyDateTime & result);

inline bool isLeapYear(UInt16 year)
{
    return ((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0);
}

// Get last day of a month. Return 0 if month if invalid.
inline UInt8 getLastDay(UInt16 year, UInt8 month)
{
    static constexpr UInt8 days_of_month_table[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    UInt8 last_day = 0;
    if (month > 0 && month <= 12)
        last_day = days_of_month_table[month];
    if (month == 2 && isLeapYear(year))
        last_day = 29;
    return last_day;
}

UInt64 addSeconds(UInt64 t, Int64 delta);
void addDays(MyDateTime & t, Int64 days);
void addMonths(MyDateTime & t, Int64 months);

MyDate createMyDateFromCoreTime(UInt64 coreTime);
MyDateTime createMyDateTimeFromCoreTime(UInt64 coreTime);

} // namespace DB
