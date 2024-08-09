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

#include <Common/MyTime.h>
#include <Common/StringUtils/StringRefUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <cctype>
#include <initializer_list>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

// day number per 400 years, from the year that year % 400 = 1
static constexpr int DAY_NUM_PER_400_YEARS = 365 * 400 + 97;
// day number per 100 years in every 400 years, from the year that year % 100 = 1
// note the day number of the last 100 years should be DAY_NUM_PER_100_YEARS + 1
static constexpr int DAY_NUM_PER_100_YEARS = 365 * 100 + 24;
// day number per 4 years in every 100 years, from the year that year % 4 = 1
// note the day number of the last 4 years should be DAY_NUM_PER_4_YEARS - 1
static constexpr int DAY_NUM_PER_4_YEARS = 365 * 4 + 1;
// day number per years in every 4 years
// note the day number of the last 1 years maybe DAY_NUM_PER_YEARS + 1
static constexpr int DAY_NUM_PER_YEARS = 365;

// adjustYear adjusts year according to y.
// See https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html
int32_t adjustYear(int32_t year)
{
    if (year >= 0 && year <= 69)
        return 2000 + year;
    if (year >= 70 && year <= 99)
        return 1900 + year;
    return year;
}

bool scanTimeArgs(const std::vector<String> & seps, std::initializer_list<int *> && list)
{
    int i = 0;
    try
    {
        for (auto * ptr : list)
        {
            *ptr = std::stoi(seps[i]);
            i++;
        }
    }
    catch (std::exception & e)
    {
        return false;
    }

    return true;
}

// find index of fractional point.
int getFracIndex(const String & format)
{
    int idx = -1;
    for (int i = int(format.size()) - 1; i >= 0; i--)
    {
        if (std::ispunct(format[i]))
        {
            if (format[i] == '.')
            {
                idx = i;
            }
            break;
        }
    }
    return idx;
}

// helper for date part splitting, punctuation characters are valid separators anywhere,
// while space and 'T' are valid separators only between date and time.
bool isValidSeperator(char c, int previous_parts)
{
    if (isPunctuation(c))
        return true;

    // for https://github.com/pingcap/tics/issues/4036
    return previous_parts == 2 && (c == 'T' || isWhitespaceASCII(c));
}

std::vector<String> parseDateFormat(String format)
{
    format = Poco::trimInPlace(format);

    if (format.empty())
        return {};

    if (!std::isdigit(format[0]) || !std::isdigit(format[format.size() - 1]))
    {
        return {};
    }

    std::vector<String> seps;
    seps.reserve(6);
    size_t start = 0;
    for (size_t i = 0; i < format.size(); i++)
    {
        if (isValidSeperator(format[i], seps.size()))
        {
            int previous_parts = seps.size();
            seps.push_back(format.substr(start, i - start));
            start = i + 1;

            for (size_t j = i + 1; j < format.size(); j++)
            {
                if (!isValidSeperator(format[j], previous_parts))
                    break;
                start++;
                i++;
            }
            continue;
        }

        if (!std::isdigit(format[i]))
        {
            return {};
        }
    }
    seps.push_back(format.substr(start));
    return seps;
}

// GetTimezone parses the trailing timezone information of a given time string literal. If idx = -1 is returned, it
// means timezone information not found, otherwise it indicates the index of the starting index of the timezone
// information. If the timezone contains sign, hour part and/or minute part, it will be returned as is, otherwise an
// empty string will be returned.
//
// Supported syntax:
//   MySQL compatible: ((?P<tz_sign>[-+])(?P<tz_hour>[0-9]{2}):(?P<tz_minute>[0-9]{2})){0,1}$, see
//     https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html and https://dev.mysql.com/doc/refman/8.0/en/datetime.html
//     the first link specified that timezone information should be in "[H]H:MM, prefixed with a + or -" while the
//     second link specified that for string literal, "hour values less than than 10, a leading zero is required.".
//   ISO-8601: Z|((((?P<tz_sign>[-+])(?P<tz_hour>[0-9]{2})(:(?P<tz_minute>[0-9]{2}){0,1}){0,1})|((?P<tz_minute>[0-9]{2}){0,1}){0,1}))$
//     see https://www.cl.cam.ac.uk/~mgk25/iso-time.html
std::tuple<int, String, String, String, String> getTimeZone(const String & literal)
{
    static const std::map<int, std::tuple<int, int>> valid_idx_combinations{
        {100, {0, 0}}, // 23:59:59Z
        {30, {2, 0}}, // 23:59:59+08
        {50, {4, 2}}, // 23:59:59+0800
        {63, {5, 2}}, // 23:59:59+08:00
        // postgres supports the following additional syntax that deviates from ISO8601, although we won't support it
        // currently, it will be fairly easy to add in the current parsing framework
        // 23:59:59Z+08
        // 23:59:59Z+08:00
    };

    String tz_sign, tz_hour, tz_sep, tz_minute;

    // idx is for the position of the starting of the timezone information
    // zidx is for the z symbol
    // sidx is for the sign
    // spidx is for the separator
    int idx = -1, zidx = -1, sidx = -1, spidx = -1;

    size_t l = literal.size();

    for (int i = l - 1; i >= 0; i--)
    {
        if (literal[i] == 'Z')
        {
            zidx = i;
            break;
        }
        if (sidx == -1 && (literal[i] == '-' || literal[i] == '+'))
        {
            sidx = i;
        }
        if (spidx == -1 && literal[i] == ':')
        {
            spidx = i;
        }
    }
    // we could enumerate all valid combinations of these values and look it up in a table, see validIdxCombinations
    // zidx can be -1 (23:59:59+08:00), l-1 (23:59:59Z)
    // sidx can be -1, l-3, l-5, l-6
    // spidx can be -1, l-3
    int k = 0;
    if (l - zidx == 1)
    {
        k += 100;
    }
    if (int t = l - sidx; t == 3 || t == 5 || t == 6)
    {
        k += t * 10;
    }
    if (l - spidx == 3)
    {
        k += 3;
    }
    if (auto tmp = valid_idx_combinations.find(k); tmp != valid_idx_combinations.end())
    {
        auto [h, m] = valid_idx_combinations.at(k);
        int hidx = l - h;
        int midx = l - m;
        auto validate = [](const String & v) {
            return '0' <= v[0] && v[0] <= '9' && '0' <= v[1] && v[1] <= '9';
        };
        if (sidx != -1)
        {
            tz_sign = literal.substr(sidx, 1);
            idx = sidx;
        }
        if (zidx != -1)
        {
            idx = zidx;
        }
        if ((l - spidx) == 3)
        {
            tz_sep = literal.substr(spidx, 1);
        }
        if (h != 0)
        {
            tz_hour = literal.substr(hidx, 2);
            if (!validate(tz_hour))
            {
                return std::make_tuple(-1, "", "", "", "");
            }
        }
        if (m != 0)
        {
            tz_minute = literal.substr(midx, 2);
            if (!validate(tz_minute))
            {
                return std::make_tuple(-1, "", "", "", "");
            }
        }
        return std::make_tuple(idx, tz_sign, tz_hour, tz_sep, tz_minute);
    }
    return std::make_tuple(-1, "", "", "", "");
}

// TODO: make unified helper
bool isPunctuation(char c)
{
<<<<<<< HEAD
    return (c >= 0x21 && c <= 0x2F) || (c >= 0x3A && c <= 0x40) || (c >= 0x5B && c <= 0x60) || (c >= 0x7B && c <= 0x7E);
=======
    auto [tz_idx, tz_sign, tz_hour, tz_sep, tz_minute] = getTimeZone(format);
    int end = format.length() - 1;
    if (tz_idx != -1)
    {
        end = tz_idx - 1;
    }
    int idx = -1;
    for (int i = end; i >= 0; i--)
    {
        if (format[i] != '+' && format[i] != '-' && isPunctuation(format[i]))
        {
            if (format[i] == '.')
            {
                idx = i;
            }
            break;
        }
    }
    return idx;
>>>>>>> 7b17f5b39e (Fix the wrong result bug when casting string as datetime with time zone or illegal chars (#9255))
}

std::tuple<std::vector<String>, String, bool, String, String, String, String> splitDatetime(String format)
{
    std::vector<String> seps;
    String frac;
    bool has_tz = false;
    auto [tz_idx, tz_sign, tz_hour, tz_sep, tz_minute] = getTimeZone(format);
    if (tz_idx > 0)
    {
        has_tz = true;
        while (tz_idx > 0 && isPunctuation(format[tz_idx - 1]))
        {
            // in case of multiple separators, e.g. 2020-10--10
            tz_idx--;
        }
        format = format.substr(0, tz_idx);
    }
    int frac_idx = getFracIndex(format);
    if (frac_idx > 0)
    {
        frac = format.substr(frac_idx + 1);
        while (frac_idx > 0 && isPunctuation(format[frac_idx - 1]))
        {
            // in case of multiple separators, e.g. 2020-10-10 11:00:00..123456
            frac_idx--;
        }
        format = format.substr(0, frac_idx);
    }
    seps = parseDateFormat(format);
    return std::make_tuple(std::move(seps), std::move(frac), std::move(has_tz), std::move(tz_sign), std::move(tz_hour), std::move(tz_sep), std::move(tz_minute));
}


MyTimeBase::MyTimeBase(UInt64 packed)
{
    UInt64 ymdhms = packed >> 24;
    UInt64 ymd = ymdhms >> 17;
    day = UInt8(ymd & ((1 << 5) - 1));
    UInt64 ym = ymd >> 5;
    month = UInt8(ym % 13);
    year = UInt16(ym / 13);

    UInt64 hms = ymdhms & ((1 << 17) - 1);
    second = UInt8(hms & ((1 << 6) - 1));
    minute = UInt8((hms >> 6) & ((1 << 6) - 1));
    hour = UInt16(hms >> 12);

    micro_second = packed % (1 << 24);
}

MyTimeBase::MyTimeBase(UInt16 year_, UInt8 month_, UInt8 day_, UInt16 hour_, UInt8 minute_, UInt8 second_, UInt32 micro_second_)
    : year(year_)
    , month(month_)
    , day(day_)
    , hour(hour_)
    , minute(minute_)
    , second(second_)
    , micro_second(micro_second_)
{}

UInt64 MyTimeBase::toPackedUInt() const
{
    UInt64 ymd = ((year * 13 + month) << 5) | day;
    UInt64 hms = (hour << 12) | (minute << 6) | second;
    return (ymd << 17 | hms) << 24 | micro_second;
}

// TODO this function will be revised once we changed MyTime to CoreTime in TiFlash.
UInt64 MyTimeBase::toCoreTime() const
{
    // copied from https://github.com/pingcap/tidb/blob/master/types/time.go
    UInt64 v = 0;
    v |= (UInt64(micro_second) << MICROSECOND_BIT_FIELD_OFFSET) & MICROSECOND_BIT_FIELD_MASK;
    v |= (UInt64(second) << SECOND_BIT_FIELD_OFFSET) & SECOND_BIT_FIELD_MASK;
    v |= (UInt64(minute) << MINUTE_BIT_FIELD_OFFSET) & MINUTE_BIT_FIELD_MASK;
    v |= (UInt64(hour) << HOUR_BIT_FIELD_OFFSET) & HOUR_BIT_FIELD_MASK;
    v |= (UInt64(day) << DAY_BIT_FIELD_OFFSET) & DAY_BIT_FIELD_MASK;
    v |= (UInt64(month) << MONTH_BIT_FIELD_OFFSET) & MONTH_BIT_FIELD_MASK;
    v |= (UInt64(year) << YEAR_BIT_FIELD_OFFSET) & YEAR_BIT_FIELD_MASK;
    return v;
}

// the implementation is the same as TiDB
void MyTimeBase::dateFormat(const String & layout, String & result) const
{
    auto formatter = MyDateTimeFormatter(layout);
    formatter.format(*this, result);
}

// the implementation is the same as TiDB
int MyTimeBase::yearDay() const
{
    if (month == 0 || day == 0)
    {
        return 0;
    }
    return calcDayNum(year, month, day) - calcDayNum(year, 1, 1) + 1;
}

UInt32 adjustWeekMode(UInt32 mode)
{
    mode &= 7u;
    if (!(mode & MyTimeBase::WEEK_BEHAVIOR_MONDAY_FIRST))
        mode ^= MyTimeBase::WEEK_BEHAVIOR_FIRST_WEEKDAY;
    return mode;
}

// the implementation is the same as TiDB
int MyTimeBase::week(UInt32 mode) const
{
    if (month == 0 || day == 0)
    {
        return 0;
    }
    auto [year, week] = calcWeek(adjustWeekMode(mode));
    std::ignore = year;
    return week;
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
// the implementation is the same as TiDB
int calcWeekday(int day_num, bool sunday_first_day_of_week)
{
    day_num += 5;
    if (sunday_first_day_of_week)
        day_num++;
    return day_num % 7;
}

// the implementation is the same as TiDB
int calcDaysInYear(int year)
{
    if ((year & 3u) == 0 && (year % 100 != 0 || (year % 400 == 0 && (year != 0))))
        return 366;
    return 365;
}

// the implementation is the same as TiDB
std::tuple<int, int> MyTimeBase::calcWeek(UInt32 mode) const
{
    int days, ret_year, ret_week;
    int ty = year, tm = month, td = day;
    int day_num = calcDayNum(ty, tm, td);
    int first_day_num = calcDayNum(ty, 1, 1);
    bool monday_first = mode & WEEK_BEHAVIOR_MONDAY_FIRST;
    bool week_year = mode & WEEK_BEHAVIOR_YEAR;
    bool first_week_day = mode & WEEK_BEHAVIOR_FIRST_WEEKDAY;

    int week_day = calcWeekday(first_day_num, !monday_first);

    ret_year = ty;

    if (tm == 1 && td <= 7 - week_day)
    {
        if (!week_year && ((first_week_day && week_day != 0) || (!first_week_day && week_day >= 4)))
        {
            ret_week = 0;
            return std::make_tuple(ret_year, ret_week);
        }
        week_year = true;
        ret_year--;
        days = calcDaysInYear(ret_year);
        first_day_num -= days;
        week_day = (week_day + 53 * 7 - days) % 7;
    }

    if ((first_week_day && week_day != 0) || (!first_week_day && week_day >= 4))
    {
        days = day_num - (first_day_num + 7 - week_day);
    }
    else
    {
        days = day_num - (first_day_num - week_day);
    }

    if (week_year && days >= 52 * 7)
    {
        week_day = (week_day + calcDaysInYear(ret_year)) % 7;
        if ((!first_week_day && week_day < 4) || (first_week_day && week_day == 0))
        {
            ret_year++;
            ret_week = 1;
            return std::make_tuple(ret_year, ret_week);
        }
    }
    ret_week = days / 7 + 1;
    return std::make_tuple(ret_year, ret_week);
}

static const String month_names[] = {
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
};

static const String abbrev_month_names[] = {
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
};

static const String abbrev_weekday_names[] = {
    "Sun",
    "Mon",
    "Tue",
    "Wed",
    "Thu",
    "Fri",
    "Sat",
};

static const String weekday_names[] = {
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
};

static const String abbr_day_of_month[] = {
    "th",
    "st",
    "nd",
    "rd",
    "th",
    "th",
    "th",
    "th",
    "th",
    "th",
};

#define INT_TO_STRING                                                                                                                   \
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", \
        "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52",   \
        "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73",   \
        "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94",   \
        "95", "96", "97", "98", "99",

static const String int_to_2_width_string[] = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", INT_TO_STRING};

static const String int_to_string[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", INT_TO_STRING};

int MyTimeBase::weekDay() const
{
    int current_abs_day_num = calcDayNum(year, month, day);
    // 1986-01-05 is sunday
    int reference_abs_day_num = calcDayNum(1986, 1, 5);
    int diff = current_abs_day_num - reference_abs_day_num;
    if (diff < 0)
        diff += (-diff / 7 + 1) * 7;
    diff = diff % 7;
    return diff;
}

const String & MyTimeBase::weekDayName() const
{
    static const String invalid_weekday;
    if (month == 0 || day == 0)
        return invalid_weekday;
    return weekday_names[weekDay()];
}

const String & MyTimeBase::monthName() const
{
    static const String invalid_month_name;
    if (month <= 0 || month > 12)
        return invalid_month_name;
    return month_names[month - 1];
}

bool checkTimeValid(Int32 year, Int32 month, Int32 day, Int32 hour, Int32 minute, Int32 second)
{
    if (year > 9999 || month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59)
    {
        return false;
    }
    return day <= getLastDay(year, month);
}

<<<<<<< HEAD
std::pair<Field, bool> parseMyDateTimeAndJudgeIsDate(const String & str, int8_t fsp, bool needCheckTimeValid)
=======
bool noNeedCheckTime(Int32, Int32, Int32, Int32, Int32, Int32)
{
    return true;
}

UInt64 addSeconds(UInt64 t, Int64 delta)
{
    // todo support zero date
    if (t == 0)
    {
        return t;
    }
    MyDateTime my_time(t);
    Int64 current_second = my_time.hour * MyTimeBase::SECOND_IN_ONE_HOUR
        + my_time.minute * MyTimeBase::SECOND_IN_ONE_MINUTE + my_time.second;
    current_second += delta;
    if (current_second >= 0)
    {
        Int64 days = current_second / MyTimeBase::SECOND_IN_ONE_DAY;
        current_second = current_second % MyTimeBase::SECOND_IN_ONE_DAY;
        if (days != 0)
            addDays(my_time, days);
    }
    else
    {
        Int64 days = (-current_second) / MyTimeBase::SECOND_IN_ONE_DAY;
        if ((-current_second) % MyTimeBase::SECOND_IN_ONE_DAY != 0)
        {
            days++;
        }
        current_second += days * MyTimeBase::SECOND_IN_ONE_DAY;
        addDays(my_time, -days);
    }
    my_time.hour = current_second / MyTimeBase::SECOND_IN_ONE_HOUR;
    my_time.minute = (current_second % MyTimeBase::SECOND_IN_ONE_HOUR) / MyTimeBase::SECOND_IN_ONE_MINUTE;
    my_time.second = current_second % MyTimeBase::SECOND_IN_ONE_MINUTE;
    return my_time.toPackedUInt();
}

// Return true if the time is invalid.
inline bool getDatetime(const Int64 & num, MyDateTime & result)
{
    UInt64 ymd = num / 1000000;
    UInt64 hms = num - ymd * 1000000;

    UInt64 year = ymd / 10000;
    ymd %= 10000;
    UInt64 month = ymd / 100;
    UInt64 day = ymd % 100;

    UInt64 hour = hms / 10000;
    hms %= 10000;
    UInt64 minute = hms / 100;
    UInt64 second = hms % 100;

    if (toCoreTimeChecked(year, month, day, hour, minute, second, 0, result))
    {
        return true;
    }
    return !result.isValid(true, false);
}

// Convert a integer number to DateTime and return true if the result is NULL.
// If number is invalid(according to SQL_MODE), return NULL and handle the error with DAGContext.
// This function may throw exception.
inline bool numberToDateTime(Int64 number, MyDateTime & result, bool allowZeroDate)
{
    MyDateTime datetime(0);
    if (number == 0)
    {
        if (allowZeroDate)
        {
            result = datetime;
            return false;
        }
        return true;
    }

    // datetime type
    if (number >= 10000101000000)
    {
        return getDatetime(number, result);
    }

    // check MMDD
    if (number < 101)
    {
        return true;
    }

    // check YYMMDD: 2000-2069
    if (number <= 69 * 10000 + 1231)
    {
        number = (number + 20000000) * 1000000;
        return getDatetime(number, result);
    }

    if (number < 70 * 10000 + 101)
    {
        return true;
    }

    // check YYMMDD
    if (number <= 991231)
    {
        number = (number + 19000000) * 1000000;
        return getDatetime(number, result);
    }

    // check hour/min/second
    if (number <= 99991231)
    {
        number *= 1000000;
        return getDatetime(number, result);
    }

    // check MMDDHHMMSS
    if (number < 101000000)
    {
        return true;
    }

    // check YYMMDDhhmmss: 2000-2069
    if (number <= 69 * 10000000000 + 1231235959)
    {
        number += 20000000000000;
        return getDatetime(number, result);
    }

    // check YYYYMMDDhhmmss
    if (number < 70 * 10000000000 + 101000000)
    {
        return true;
    }

    // check YYMMDDHHMMSS
    if (number <= 991231235959)
    {
        number += 19000000000000;
        return getDatetime(number, result);
    }

    return getDatetime(number, result);
}

// returns frac, overflow, matched. eg., "999" fsp=2 will overflow.
std::tuple<UInt32, bool, bool> parseFrac(const std::string_view str, int8_t fsp)
{
    if (str.empty())
    {
        return {0, false, true};
    }
    if (fsp == -1)
    {
        fsp = 6;
    }
    if (fsp < 0 || fsp > 6)
    {
        return {0, false, false};
    }
    try
    {
        int end_pos = static_cast<size_t>(fsp) >= str.size() ? str.size() : (fsp + 1);
        UInt32 tmp = 0;
        int size = 0;
        for (int i = 0; i < end_pos; ++i)
        {
            if (auto c = str[i]; c >= '0' && c <= '9')
            {
                tmp = tmp * 10 + c - '0';
                size++;
            }
            else
            {
                break;
            }
        }

        if (fsp >= size)
        {
            return {tmp * std::pow(10, 6 - size), false, true};
        }

        tmp = (tmp + 5) / 10;
        if (tmp >= std::pow(10, fsp))
        {
            // overflow
            return {0, true, true};
        }
        // Get the final frac, with 6 digit number
        //  1236 round 3 -> 124 -> 124000
        //  0312 round 2 -> 3 -> 30000
        //  999 round 2 -> 100 -> overflow
        return {tmp * std::pow(10, 6 - fsp), false, true};
    }
    catch (std::exception & e)
    {
        return {0, false, false};
    }
}

// isFloat is true means that the input string is float format like "1212.111"
std::pair<Field, bool> parseMyDateTimeAndJudgeIsDate(
    const String & str,
    int8_t fsp,
    CheckTimeFunc checkTimeFunc,
    bool isFloat)
>>>>>>> 7b17f5b39e (Fix the wrong result bug when casting string as datetime with time zone or illegal chars (#9255))
{
    Int32 year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, delta_hour = 0, delta_minute = 0;

    bool is_date = false;

    bool hhmmss = false;

    auto [seps, frac_str, has_tz, tz_sign, tz_hour, tz_sep, tz_minute] = splitDatetime(str);

    bool truncated_or_incorrect = false;

    // no_absorb tests if can absorb FSP or TZ
    auto no_absorb = [](const std::vector<String> & seps) {
        // if we have more than 5 parts (i.e. 6), the tailing part can't be absorbed
        // or if we only have 1 part, but its length is longer than 4, then it is at least YYMMD, in this case, FSP can
        // not be absorbed, and it will be handled later, and the leading sign prevents TZ from being absorbed, because
        // if date part has no separators, we can't use -/+ as separators between date & time.
        return seps.size() > 5 || (seps.size() == 1 && seps[0].size() > 4);
    };

    if (!frac_str.empty())
    {
        if (!no_absorb(seps))
        {
            seps.push_back(frac_str);
            frac_str = "";
        }
    }

    if (has_tz && !tz_sign.empty())
    {
        // if tz_sign is empty, it's sure that the string literal contains timezone (e.g., 2010-10-10T10:10:10Z),
        // therefore we could safely skip this branch.
        if (!no_absorb(seps) && !(!tz_minute.empty() && tz_sep.empty()))
        {
            // we can't absorb timezone if there is no separate between tz_hour and tz_minute
            if (!tz_hour.empty())
            {
                seps.push_back(tz_hour);
            }
            if (!tz_minute.empty())
            {
                seps.push_back(tz_minute);
            }
            has_tz = false;
        }
    }

    switch (seps.size())
    {
    // No delimiter
    case 1:
    {
        size_t l = seps[0].size();
        switch (l)
        {
        case 14: // YYYYMMDDHHMMSS
        {
            int ret = std::sscanf(seps[0].c_str(), "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 6);
            hhmmss = true;
            break;
        }
        case 12: // YYMMDDHHMMSS
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 6);
            year = adjustYear(year);
            hhmmss = true;
            break;
        }
        case 11: // YYMMDDHHMMS
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute, &second); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 6);
            year = adjustYear(year);
            hhmmss = true;
            break;
        }
        case 10: // YYMMDDHHMM
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 5);
            year = adjustYear(year);
            break;
        }
        case 9: // YYMMDDHHM
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 5);
            year = adjustYear(year);
            break;
        }
        case 8: // YYYYMMDD
        {
            int ret = std::sscanf(seps[0].c_str(), "%4d%2d%2d", &year, &month, &day); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 3);
            break;
        }
        case 7: // YYMMDDH
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d%1d", &year, &month, &day, &hour); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 4);
            year = adjustYear(year);
            break;
        }
        case 6: // YYMMDD
        case 5: // YYMMD
        {
            int ret = std::sscanf(seps[0].c_str(), "%2d%2d%2d", &year, &month, &day); //NOLINT(cert-err34-c): check conversion error manually
            truncated_or_incorrect = (ret != 3);
            year = adjustYear(year);
            break;
        }
        default:
        {
            return {Field(), is_date};
        }
        }
        if (l == 5 || l == 6 || l == 8)
        {
            // YYMMDD or YYYYMMDD
            // We must handle float => string => datetime, the difference is that fractional
            // part of float type is discarded directly, while fractional part of string type
            // is parsed to HH:MM:SS.
            int ret = 0;
            switch (frac_str.size())
            {
            case 0:
                is_date = true;
                break;
            case 1:
            case 2:
            {
                ret = std::sscanf(frac_str.c_str(), "%2d ", &hour); //NOLINT(cert-err34-c): check conversion error manually
                truncated_or_incorrect = (ret != 1);
                break;
            }
            case 3:
            case 4:
            {
                ret = std::sscanf(frac_str.c_str(), "%2d%2d ", &hour, &minute); //NOLINT(cert-err34-c): check conversion error manually
                truncated_or_incorrect = (ret != 2);
                break;
            }
            default:
            {
                ret = std::sscanf(frac_str.c_str(), "%2d%2d%2d ", &hour, &minute, &second); //NOLINT(cert-err34-c): check conversion error manually
                truncated_or_incorrect = (ret != 3);
                break;
            }
            }
        }
        if (l == 9 || l == 10)
        {
            if (frac_str.empty())
            {
                second = 0;
            }
            else
            {
                truncated_or_incorrect = (std::sscanf(frac_str.c_str(), "%2d ", &second) == 0); //NOLINT
            }
        }
        if (truncated_or_incorrect)
        {
            return {Field(), is_date};
        }
        break;
    }
    case 3:
    {
        // YYYY-MM-DD
        if (!scanTimeArgs(seps, {&year, &month, &day}))
            return {Field(), is_date};
        is_date = true;
        break;
    }
    case 4:
    {
        // YYYY-MM-DD HH
        if (!scanTimeArgs(seps, {&year, &month, &day, &hour}))
            return {Field(), is_date};
        break;
    }
    case 5:
    {
        // YYYY-MM-DD HH-MM
        if (!scanTimeArgs(seps, {&year, &month, &day, &hour, &minute}))
            return {Field(), is_date};
        break;
    }
    case 6:
    {
        // We don't have fractional seconds part.
        // YYYY-MM-DD HH-MM-SS
        if (!scanTimeArgs(seps, {&year, &month, &day, &hour, &minute, &second}))
            return {Field(), is_date};
        hhmmss = true;
        break;
    }
    default:
    {
        return {Field(), is_date};
    }
    }

    // If str is sepereated by delimiters, the first one is year, and if the year is 2 digit,
    // we should adjust it.
    // TODO: adjust year is very complex, now we only consider the simplest way.
    if (seps[0].size() == 2)
    {
        if (year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && frac_str.empty())
        {
            // Skip a special case "00-00-00".
        }
        else
        {
            year = adjustYear(year);
        }
    }

    UInt32 micro_second = 0;
    if (hhmmss && !frac_str.empty())
    {
        // If input string is "20170118.999", without hhmmss, fsp is meaningless.
        // TODO: this case is not only meaningless, but erroneous, please confirm.
        if (static_cast<size_t>(fsp) >= frac_str.size())
        {
            micro_second = std::stoul(frac_str);
            micro_second = micro_second * std::pow(10, 6 - frac_str.size());
        }
        else
        {
            auto result_frac = frac_str.substr(0, fsp + 1);
            micro_second = std::stoul(result_frac);
            micro_second = (micro_second + 5) / 10;
            // Overflow
            if (micro_second >= std::pow(10, fsp))
            {
                MyDateTime datetime(year, month, day, hour, minute, second, 0);
                UInt64 result = addSeconds(datetime.toPackedUInt(), 1);
                MyDateTime result_datetime(result);
                year = result_datetime.year;
                month = result_datetime.month;
                day = result_datetime.day;
                hour = result_datetime.hour;
                minute = result_datetime.minute;
                second = result_datetime.second;
                micro_second = 0;
            }
            else
            {
                micro_second = micro_second * std::pow(10, 6 - fsp);
            }
        }
    }

    if (needCheckTimeValid && !checkTimeValid(year, month, day, hour, minute, second))
    {
        return {Field(), is_date};
    }

    MyDateTime result(year, month, day, hour, minute, second, micro_second);

    if (has_tz)
    {
        if (!hhmmss)
        {
            return {Field(), is_date};
        }
        if (!tz_hour.empty())
        {
            delta_hour = (tz_hour[0] - '0') * 10 + (tz_hour[1] - '0');
        }
        if (!tz_minute.empty())
        {
            delta_minute = (tz_minute[0] - '0') * 10 + (tz_minute[1] - '0');
        }
        // allowed delta range is [-14:00, 14:00], and we will intentionally reject -00:00
        if (delta_hour > 14 || delta_minute > 59 || (delta_hour == 14 && delta_minute != 0)
            || (tz_sign == "-" && delta_hour == 0 && delta_minute == 0))
        {
            return {Field(), is_date};
        }
        // by default, if the temporal string literal does not contain timezone information, it will be in the timezone
        // specified by the time_zone system variable. However, if the timezone is specified in the string literal, we
        // will use the specified timezone to interpret the string literal and convert it into the system timezone.
        int offset = delta_hour * 60 * 60 + delta_minute * 60;
        if (tz_sign == "-")
        {
            offset = -offset;
        }
        auto tmp = addSeconds(result.toPackedUInt(), -offset);
        result = MyDateTime(tmp);
    }

    return std::pair<Field, bool>{result.toPackedUInt(), is_date};
}

// TODO: support parse time from float string
Field parseMyDateTime(const String & str, int8_t fsp, bool needCheckTimeValid)
{
    return parseMyDateTimeAndJudgeIsDate(str, fsp, needCheckTimeValid).first;
}

String MyDateTime::toString(int fsp) const
{
    const static String format = "%Y-%m-%d %H:%i:%s";
    String result;
    result.reserve(maxFormattedDateTimeStringLength(format));
    dateFormat(format, result);
    auto length = result.length();
    if (fsp > 0)
    {
        result.append(".")
            .append(int_to_2_width_string[micro_second / 10000])
            .append(int_to_2_width_string[micro_second % 10000 / 100])
            .append(int_to_2_width_string[micro_second % 100]);
        result.resize(length + fsp + 1);
    }
    return result;
}

//TODO: we can use modern c++ api instead.
MyDateTime MyDateTime::getSystemDateTimeByTimezone(const TimezoneInfo & timezoneInfo, UInt8 fsp)
{
    struct timespec ts; // NOLINT(cppcoreguidelines-pro-type-member-init)
    clock_gettime(CLOCK_REALTIME, &ts);

    time_t second = ts.tv_sec;
    UInt32 nano_second = ts.tv_nsec;
    auto second_and_micro_second = roundTimeByFsp(second, nano_second, fsp);
    second = second_and_micro_second.first;
    UInt32 micro_second = second_and_micro_second.second;

    if (timezoneInfo.is_name_based)
        return convertUTC2TimeZone(second, micro_second, *timezoneInfo.timezone);
    else
        return convertUTC2TimeZoneByOffset(second, micro_second, timezoneInfo.timezone_offset);
}

inline bool isZeroDate(UInt64 time)
{
    return time == 0;
}

void convertTimeZoneImpl(UInt64 from_time, UInt64 & to_time, const DateLUTImpl & time_zone_from, const DateLUTImpl & time_zone_to, bool from_utc, Int64 offset, bool throw_exception)
{
    if (isZeroDate(from_time))
    {
        to_time = from_time;
        return;
    }
    MyDateTime from_my_time(from_time);
    time_t from_epoch = getEpochSecond(from_my_time, time_zone_from);
    time_t utc_epoch = from_epoch;
    time_t to_epoch = from_epoch;
    if (from_utc)
    {
        to_epoch += offset;
    }
    else
    {
        utc_epoch -= offset;
        to_epoch -= offset;
    }
    if (unlikely(utc_epoch <= 0))
    {
        if (throw_exception)
        {
            throw Exception("Unsupported timestamp value , TiFlash only supports timestamp after 1970-01-01 00:00:00 UTC)");
        }
        else
        {
            /// For time earlier than 1970-01-01 00:00:00 UTC, return 0, aligned with mysql and tidb
            to_time = 0;
            return;
        }
    }
    MyDateTime to_my_time(time_zone_to.toYear(to_epoch), time_zone_to.toMonth(to_epoch), time_zone_to.toDayOfMonth(to_epoch), time_zone_to.toHour(to_epoch), time_zone_to.toMinute(to_epoch), time_zone_to.toSecond(to_epoch), from_my_time.micro_second);
    to_time = to_my_time.toPackedUInt();
}

void convertTimeZone(UInt64 from_time, UInt64 & to_time, const DateLUTImpl & time_zone_from, const DateLUTImpl & time_zone_to, bool throw_exception)
{
    convertTimeZoneImpl(from_time, to_time, time_zone_from, time_zone_to, true, 0, throw_exception);
}

void convertTimeZoneByOffset(UInt64 from_time, UInt64 & to_time, bool from_utc, Int64 offset, bool throw_exception)
{
    static const auto & time_zone_utc = DateLUT::instance("UTC");
    convertTimeZoneImpl(from_time, to_time, time_zone_utc, time_zone_utc, from_utc, offset, throw_exception);
}

MyDateTime convertUTC2TimeZone(time_t utc_ts, UInt32 micro_second, const DateLUTImpl & time_zone_to)
{
    return MyDateTime(time_zone_to.toYear(utc_ts), time_zone_to.toMonth(utc_ts), time_zone_to.toDayOfMonth(utc_ts), time_zone_to.toHour(utc_ts), time_zone_to.toMinute(utc_ts), time_zone_to.toSecond(utc_ts), micro_second);
}

MyDateTime convertUTC2TimeZoneByOffset(time_t utc_ts, UInt32 micro_second, Int64 offset)
{
    static const auto & time_zone_utc = DateLUT::instance("UTC");
    time_t epoch = utc_ts + offset;
    return MyDateTime(time_zone_utc.toYear(epoch), time_zone_utc.toMonth(epoch), time_zone_utc.toDayOfMonth(epoch), time_zone_utc.toHour(epoch), time_zone_utc.toMinute(epoch), time_zone_utc.toSecond(epoch), micro_second);
}

std::pair<time_t, UInt32> roundTimeByFsp(time_t second, UInt64 nano_second, UInt8 fsp)
{
    static const UInt64 max_nano_second = std::pow(10, 9);
    if (unlikely(fsp > 6))
    {
        throw Exception("Invalid precision " + std::to_string(fsp) + ". It should between 0 and 6");
    }
    UInt64 scale = std::pow(10, 9 - fsp);
    nano_second = (nano_second + scale / 2) / scale * scale;
    if (nano_second >= max_nano_second)
    {
        auto extra_second = nano_second / max_nano_second;
        nano_second = nano_second - extra_second * max_nano_second;
        second += extra_second;
    }
    return std::pair<time_t, UInt32>{second, nano_second / 1000};
}

// the implementation is the same as TiDB
int calcDayNum(int year, int month, int day)
{
    if (year == 0 && month == 0)
        return 0;
    int delsum = 365 * year + 31 * (month - 1) + day;
    if (month <= 2)
    {
        year--;
    }
    else
    {
        delsum -= (month * 4 + 23) / 10;
    }
    int temp = ((year / 100 + 1) * 3) / 4;
    return delsum + year / 4 - temp;
}

UInt64 calcSeconds(int year, int month, int day, int hour, int minute, int second)
{
    if (year == 0 && month == 0)
        return 0;
    Int32 current_days = calcDayNum(year, month, day);
    return current_days * MyTimeBase::SECOND_IN_ONE_DAY + hour * MyTimeBase::SECOND_IN_ONE_HOUR
        + minute * MyTimeBase::SECOND_IN_ONE_MINUTE + second;
}

size_t maxFormattedDateTimeStringLength(const String & format)
{
    size_t result = 0;
    bool in_pattern_match = false;
    for (char x : format)
    {
        if (in_pattern_match)
        {
            switch (x)
            {
            case 'b':
            case 'j':
            case 'a':
                result += 3;
                break;
            case 'M':
            case 'W':
                result += 9;
                break;
            case 'm':
            case 'c':
            case 'd':
            case 'e':
            case 'H':
            case 'k':
            case 'h':
            case 'I':
            case 'l':
            case 'i':
            case 'p':
            case 'S':
            case 's':
            case 'U':
            case 'u':
            case 'V':
            case 'v':
            case 'y':
                result += 2;
                break;
            case 'D':
            case 'X':
            case 'x':
            case 'Y':
                result += 4;
                break;
            case 'r':
                result += 11;
                break;
            case 'T':
                result += 8;
                break;
            case 'f':
                result += 6;
                break;
            case 'w':
                result += 1;
                break;
            default:
                result += 1;
                break;
            }
            in_pattern_match = false;
            continue;
        }

        if (x == '%')
            in_pattern_match = true;
        else
            result++;
    }
    return std::max<size_t>(result, 1);
}

bool MyTimeBase::isValid(bool allow_zero_in_date, bool allow_invalid_date) const
{
    if (!(year == 0 && month == 0 && day == 0))
    {
        if (!allow_zero_in_date && (month == 0 || day == 0))
        {
            return false;
        }
    }

    if (year >= 9999 || month > 12)
    {
        return false;
    }

    UInt8 max_day = 31;
    if (!allow_invalid_date)
    {
        if (month < 1)
        {
            return false;
        }
        constexpr static UInt8 max_days_in_month[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        static auto is_leap_year = [](UInt16 _year) {
            return ((_year % 4 == 0) && (_year % 100 != 0)) || (_year % 400 == 0);
        };
        max_day = max_days_in_month[month - 1];
        if (month == 2 && is_leap_year(year))
        {
            max_day = 29;
        }
    }
    if (day > max_day)
    {
        return false;
    }

    if (hour < 0 || hour >= 24)
    {
        return false;
    }
    if (minute >= 60)
    {
        return false;
    }
    if (second >= 60)
    {
        return false;
    }
    return true;
}

bool toCoreTimeChecked(const UInt64 & year, const UInt64 & month, const UInt64 & day, const UInt64 & hour, const UInt64 & minute, const UInt64 & second, const UInt64 & microsecond, MyDateTime & result)
{
    if (year >= (1 << MyTimeBase::YEAR_BIT_FIELD_WIDTH) || month >= (1 << MyTimeBase::MONTH_BIT_FIELD_WIDTH)
        || day >= (1 << MyTimeBase::DAY_BIT_FIELD_WIDTH) || hour >= (1 << MyTimeBase::HOUR_BIT_FIELD_WIDTH)
        || minute >= (1 << MyTimeBase::MINUTE_BIT_FIELD_WIDTH) || second >= (1 << MyTimeBase::SECOND_BIT_FIELD_WIDTH)
        || microsecond >= (1 << MyTimeBase::MICROSECOND_BIT_FIELD_WIDTH))
    {
        result = MyDateTime(0, 0, 0, 0, 0, 0, 0);
        return true;
    }
    result = MyDateTime(year, month, day, hour, minute, second, microsecond);
    return false;
}

UInt64 addSeconds(UInt64 t, Int64 delta)
{
    // todo support zero date
    if (t == 0)
    {
        return t;
    }
    MyDateTime my_time(t);
    Int64 current_second = my_time.hour * MyTimeBase::SECOND_IN_ONE_HOUR + my_time.minute * MyTimeBase::SECOND_IN_ONE_MINUTE + my_time.second;
    current_second += delta;
    if (current_second >= 0)
    {
        Int64 days = current_second / MyTimeBase::SECOND_IN_ONE_DAY;
        current_second = current_second % MyTimeBase::SECOND_IN_ONE_DAY;
        if (days != 0)
            addDays(my_time, days);
    }
    else
    {
        Int64 days = (-current_second) / MyTimeBase::SECOND_IN_ONE_DAY;
        if ((-current_second) % MyTimeBase::SECOND_IN_ONE_DAY != 0)
        {
            days++;
        }
        current_second += days * MyTimeBase::SECOND_IN_ONE_DAY;
        addDays(my_time, -days);
    }
    my_time.hour = current_second / MyTimeBase::SECOND_IN_ONE_HOUR;
    my_time.minute = (current_second % MyTimeBase::SECOND_IN_ONE_HOUR) / MyTimeBase::SECOND_IN_ONE_MINUTE;
    my_time.second = current_second % MyTimeBase::SECOND_IN_ONE_MINUTE;
    return my_time.toPackedUInt();
}

void fillMonthAndDay(int day_num, int & month, int & day, const int * accumulated_days_per_month)
{
    month = day_num / 31;
    if (accumulated_days_per_month[month] < day_num)
        month++;
    day = day_num - (month == 0 ? 0 : accumulated_days_per_month[month - 1] + 1);
}

void fromDayNum(MyDateTime & t, int day_num)
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
        if (num_of_100_years == 4)
        {
            num_of_100_years = 3;
            day_num = DAY_NUM_PER_100_YEARS;
        }

        int num_of_4_years = day_num / DAY_NUM_PER_4_YEARS;
        // can not use day_num % DAY_NUM_PER_4_YEARS
        day_num = day_num - (num_of_4_years * DAY_NUM_PER_4_YEARS);

        int num_of_years = day_num / DAY_NUM_PER_YEARS;
        // can not use day_num % DAY_NUM_PER_YEARS
        day_num = day_num - (num_of_years * DAY_NUM_PER_YEARS);

        if (num_of_years == 4)
        {
            num_of_years = 3;
            day_num = DAY_NUM_PER_YEARS;
        }

        year = 1 + num_of_400_years * 400 + num_of_100_years * 100 + num_of_4_years * 4 + num_of_years;
    }
    static const int ACCUMULATED_DAYS_PER_MONTH[] = {30, 58, 89, 119, 150, 180, 211, 242, 272, 303, 333, 364};
    static const int ACCUMULATED_DAYS_PER_MONTH_LEAP_YEAR[] = {30, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
    bool is_leap_year = year % 400 == 0 || (year % 4 == 0 && year % 100 != 0);
    fillMonthAndDay(day_num, month, day, is_leap_year ? ACCUMULATED_DAYS_PER_MONTH_LEAP_YEAR : ACCUMULATED_DAYS_PER_MONTH);
    if (year < 0 || year > 9999)
    {
        throw Exception("datetime overflow");
    }
    else if (year == 0)
    {
        t.year = 0;
        t.month = 0;
        t.day = 0;
        return;
    }
    t.year = year;
    t.month = month + 1;
    t.day = day + 1;
}

void addDays(MyDateTime & t, Int64 days)
{
    Int32 current_days = calcDayNum(t.year, t.month, t.day);
    current_days += days;
    fromDayNum(t, current_days);
}

void addMonths(MyDateTime & t, Int64 months)
{
    // month in my_time start from 1
    Int64 current_month = t.month - 1;
    current_month += months;
    Int64 current_year = 0;
    auto year = static_cast<Int64>(t.year);
    if (current_month >= 0)
    {
        current_year = current_month / 12;
        current_month = current_month % 12;
        year += current_year;
    }
    else
    {
        current_year = (-current_month) / 12;
        if ((-current_month) % 12 != 0)
            current_year++;
        current_month += current_year * 12;
        year -= current_year;
    }
    if (year < 0 || year > 9999)
    {
        throw Exception("datetime overflow");
    }
    else if (year == 0)
    {
        t.year = 0;
        t.month = 0;
        t.day = 0;
        return;
    }
    t.year = year;
    static const int day_num_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    static const int day_num_in_month_leap_year[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int max_day = 0;
    if (t.year % 400 == 0 || (t.year % 100 != 0 && t.year % 4 == 0))
        max_day = day_num_in_month_leap_year[current_month];
    else
        max_day = day_num_in_month[current_month];
    t.month = current_month + 1;
    t.day = t.day > max_day ? max_day : t.day;
}

MyDateTimeFormatter::MyDateTimeFormatter(const String & layout)
{
    bool in_pattern_match = false;
    for (char x : layout)
    {
        if (in_pattern_match)
        {
            switch (x)
            {
            case 'b':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if (unlikely(datetime.month == 0 || datetime.month > 12))
                        throw Exception("invalid time format");
                    result.append(abbrev_month_names[datetime.month - 1]);
                });
                break;
            case 'M':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if (unlikely(datetime.month == 0 || datetime.month > 12))
                        throw Exception("invalid time format");
                    result.append(month_names[datetime.month - 1]);
                });
                break;
            case 'm':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.month]); });
                break;
            case 'c':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_string[datetime.month]); });
                break;
            case 'D':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if (datetime.day >= 11 && datetime.day <= 13)
                        result.append(int_to_string[datetime.day]).append("th");
                    else
                        result.append(int_to_string[datetime.day]).append(abbr_day_of_month[datetime.day % 10]);
                });
                break;
            case 'd':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.day]); });
                break;
            case 'e':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_string[datetime.day]); });
                break;
            case 'j':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto year_day = datetime.yearDay();
                    result.append(int_to_string[year_day / 100]).append(int_to_2_width_string[year_day % 100]);
                });
                break;
            case 'H':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.hour]); });
                break;
            case 'k':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_string[datetime.hour]); });
                break;
            case 'h':
            case 'I':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if (datetime.hour % 12 == 0)
                        result.append("12");
                    else
                        result.append(int_to_2_width_string[datetime.hour]);
                });
                break;
            case 'l':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if (datetime.hour % 12 == 0)
                        result.append("12");
                    else
                        result.append(int_to_string[datetime.hour]);
                });
                break;
            case 'i':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.minute]); });
                break;
            case 'p':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    if ((datetime.hour / 12) % 2)
                        result.append("PM");
                    else
                        result.append("AM");
                });
                break;
            case 'r':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto h = datetime.hour % 24;
                    if (h == 0)
                        result.append("12:")
                            .append(int_to_2_width_string[datetime.minute])
                            .append(":")
                            .append(int_to_2_width_string[datetime.second])
                            .append(" AM");
                    else if (h == 12)
                        result.append("12:")
                            .append(int_to_2_width_string[datetime.minute])
                            .append(":")
                            .append(int_to_2_width_string[datetime.second])
                            .append(" PM");
                    else if (h < 12)
                        result.append(int_to_2_width_string[h])
                            .append(":")
                            .append(int_to_2_width_string[datetime.minute])
                            .append(":")
                            .append(int_to_2_width_string[datetime.second])
                            .append(" AM");
                    else
                        result.append(int_to_2_width_string[h - 12])
                            .append(":")
                            .append(int_to_2_width_string[datetime.minute])
                            .append(":")
                            .append(int_to_2_width_string[datetime.second])
                            .append(" PM");
                });
                break;
            case 'T':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    result.append(int_to_2_width_string[datetime.hour])
                        .append(":")
                        .append(int_to_2_width_string[datetime.minute])
                        .append(":")
                        .append(int_to_2_width_string[datetime.second]);
                });
                break;
            case 'S':
            case 's':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.second]); });
                break;
            case 'f':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    result.append(int_to_2_width_string[datetime.micro_second / 10000])
                        .append(int_to_2_width_string[datetime.micro_second % 10000 / 100])
                        .append(int_to_2_width_string[datetime.micro_second % 100]);
                });
                break;
            case 'U':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto w = datetime.week(0);
                    result.append(int_to_2_width_string[w]);
                });
                break;
            case 'u':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto w = datetime.week(1);
                    result.append(int_to_2_width_string[w]);
                });
                break;
            case 'V':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto w = datetime.week(2);
                    result.append(int_to_2_width_string[w]);
                });
                break;
            case 'v':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto [year, week] = datetime.calcWeek(3);
                    std::ignore = year;
                    result.append(int_to_2_width_string[week]);
                });
                break;
            case 'a':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto week_day = datetime.weekDay();
                    result.append(abbrev_weekday_names[week_day]);
                });
                break;
            case 'W':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto week_day = datetime.weekDay();
                    result.append(weekday_names[week_day]);
                });
                break;
            case 'w':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto week_day = datetime.weekDay();
                    result.append(int_to_string[week_day]);
                });
                break;
            case 'X':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto [year, week] = datetime.calcWeek(6);
                    std::ignore = week;
                    result.append(int_to_2_width_string[year / 100]).append(int_to_2_width_string[year % 100]);
                });
                break;
            case 'x':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    auto [year, week] = datetime.calcWeek(3);
                    std::ignore = week;
                    result.append(int_to_2_width_string[year / 100]).append(int_to_2_width_string[year % 100]);
                });
                break;
            case 'Y':
                formatters.emplace_back([](const MyTimeBase & datetime, String & result) {
                    result.append(int_to_2_width_string[datetime.year / 100]).append(int_to_2_width_string[datetime.year % 100]);
                });
                break;
            case 'y':
                formatters.emplace_back(
                    [](const MyTimeBase & datetime, String & result) { result.append(int_to_2_width_string[datetime.year % 100]); });
                break;
            default:
                formatters.emplace_back([x](const MyTimeBase &, String & result) { result.push_back(x); });
            }
            in_pattern_match = false;
            continue;
        }
        if (x == '%')
            in_pattern_match = true;
        else
            formatters.emplace_back([x](const MyTimeBase &, String & result) { result.push_back(x); });
    }
}

struct MyDateTimeParser::Context
{
    // Some state for `mysqlTimeFix`
    uint32_t state = 0;
    static constexpr uint32_t ST_DAY_OF_YEAR = 0x01;
    static constexpr uint32_t ST_MERIDIEM = 0x02;
    static constexpr uint32_t ST_HOUR_0_23 = 0x04;
    static constexpr uint32_t ST_HOUR_1_12 = 0x08;

    int32_t day_of_year = 0;
    // 0 - invalid, 1 - am, 2 - pm
    int32_t meridiem = 0;

    // The input string view
    const StringRef view;
    // The pos we are parsing from
    size_t pos = 0;

    explicit Context(StringRef view_)
        : view(std::move(view_))
    {}
};

// Try to parse digits with number of `limit` starting from view[pos]
// Return <n chars to step forward, number> if success.
// Return <0, _> if fail.
static std::tuple<size_t, int32_t> parseNDigits(const StringRef & view, const size_t pos, const size_t limit)
{
    size_t step = 0;
    int32_t num = 0;
    while (step < limit && (pos + step) < view.size && isNumericASCII(view.data[pos + step]))
    {
        num = num * 10 + (view.data[pos + step] - '0');
        step += 1;
    }
    return std::make_tuple(step, num);
}

static std::tuple<size_t, int32_t> parseYearNDigits(const StringRef & view, const size_t pos, const size_t limit)
{
    // Try to parse a "year" within `limit` digits
    size_t step = 0;
    int32_t year = 0;
    std::tie(step, year) = parseNDigits(view, pos, limit);
    if (step == 0)
        return std::make_tuple(step, 0);
    else if (step <= 2)
        year = adjustYear(year);
    return std::make_tuple(step, year);
}

enum class ParseState
{
    NORMAL = 0, // Parsing
    FAIL = 1, // Fail to parse
    END_OF_FILE = 2, // The end of input
};

//"%r": Time, 12-hour (hh:mm:ss followed by AM or PM)
static bool parseTime12Hour(MyDateTimeParser::Context & ctx, MyTimeBase & time)
{
    // Use temp_pos instead of changing `ctx.pos` directly in case of parsing failure
    size_t temp_pos = ctx.pos;
    auto check_if_end = [&temp_pos, &ctx]() -> ParseState {
        // To the end
        if (temp_pos == ctx.view.size)
            return ParseState::END_OF_FILE;
        return ParseState::NORMAL;
    };
    auto skip_whitespaces = [&temp_pos, &ctx, &check_if_end]() -> ParseState {
        while (temp_pos < ctx.view.size && isWhitespaceASCII(ctx.view.data[temp_pos]))
            ++temp_pos;
        return check_if_end();
    };
    auto parse_sep = [&temp_pos, &ctx, &skip_whitespaces]() -> ParseState {
        if (skip_whitespaces() == ParseState::END_OF_FILE)
            return ParseState::END_OF_FILE;
        // parse ":"
        if (ctx.view.data[temp_pos] != ':')
            return ParseState::FAIL;
        temp_pos += 1; // move forward
        return ParseState::NORMAL;
    };
    auto try_parse = [&]() -> ParseState {
        ParseState state = ParseState::NORMAL;
        /// Note that we should update `time` as soon as possible, or we
        /// can not get correct result for incomplete input like "12:13"
        /// that is less than "hh:mm:ssAM"

        // hh
        size_t step = 0;
        int32_t hour = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, hour) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || hour > 12 || hour == 0)
            return ParseState::FAIL;
        // Handle special case: 12:34:56 AM -> 00:34:56
        // For PM, we will add 12 it later
        if (hour == 12)
            hour = 0;
        time.hour = hour;
        temp_pos += step; // move forward

        if (state = parse_sep(); state != ParseState::NORMAL)
            return state;

        int32_t minute = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, minute) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || minute > 59)
            return ParseState::FAIL;
        time.minute = minute;
        temp_pos += step; // move forward

        if (state = parse_sep(); state != ParseState::NORMAL)
            return state;

        int32_t second = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, second) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || second > 59)
            return ParseState::FAIL;
        time.second = second;
        temp_pos += step; // move forward

        int meridiem = 0; // 0 - invalid, 1 - am, 2 - pm
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        // "AM"/"PM" must be parsed as a single element
        // "11:13:56a" is an invalid input for "%r".
        if (auto size_to_end = ctx.view.size - temp_pos; size_to_end < 2)
            return ParseState::FAIL;
        if (toLowerIfAlphaASCII(ctx.view.data[temp_pos]) == 'a')
            meridiem = 1;
        else if (toLowerIfAlphaASCII(ctx.view.data[temp_pos]) == 'p')
            meridiem = 2;

        if (toLowerIfAlphaASCII(ctx.view.data[temp_pos + 1]) != 'm')
            meridiem = 0;
        switch (meridiem)
        {
        case 0:
            return ParseState::FAIL;
        case 1:
            break;
        case 2:
            time.hour += 12;
            break;
        }
        temp_pos += 2; // move forward
        return ParseState::NORMAL;
    };
    if (auto state = try_parse(); state == ParseState::FAIL)
        return false;
    // Other state, forward the `ctx.pos` and return true
    ctx.pos = temp_pos;
    return true;
}

//"%T": Time, 24-hour (hh:mm:ss)
static bool parseTime24Hour(MyDateTimeParser::Context & ctx, MyTimeBase & time)
{
    // Use temp_pos instead of changing `ctx.pos` directly in case of parsing failure
    size_t temp_pos = ctx.pos;
    auto check_if_end = [&temp_pos, &ctx]() -> ParseState {
        // To the end
        if (temp_pos == ctx.view.size)
            return ParseState::END_OF_FILE;
        return ParseState::NORMAL;
    };
    auto skip_whitespaces = [&temp_pos, &ctx, &check_if_end]() -> ParseState {
        while (temp_pos < ctx.view.size && isWhitespaceASCII(ctx.view.data[temp_pos]))
            ++temp_pos;
        return check_if_end();
    };
    auto parse_sep = [&temp_pos, &ctx, &skip_whitespaces]() -> ParseState {
        if (skip_whitespaces() == ParseState::END_OF_FILE)
            return ParseState::END_OF_FILE;
        // parse ":"
        if (ctx.view.data[temp_pos] != ':')
            return ParseState::FAIL;
        temp_pos += 1; // move forward
        return ParseState::NORMAL;
    };
    auto try_parse = [&]() -> ParseState {
        ParseState state = ParseState::NORMAL;
        /// Note that we should update `time` as soon as possible, or we
        /// can not get correct result for incomplete input like "12:13"
        /// that is less than "hh:mm:ss"

        // hh
        size_t step = 0;
        int32_t hour = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, hour) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || hour > 23)
            return ParseState::FAIL;
        time.hour = hour;
        temp_pos += step; // move forward

        if (state = parse_sep(); state != ParseState::NORMAL)
            return state;

        int32_t minute = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, minute) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || minute > 59)
            return ParseState::FAIL;
        time.minute = minute;
        temp_pos += step; // move forward

        if (state = parse_sep(); state != ParseState::NORMAL)
            return state;

        int32_t second = 0;
        if (state = skip_whitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, second) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || second > 59)
            return ParseState::FAIL;
        time.second = second;
        temp_pos += step; // move forward

        return ParseState::NORMAL;
    };
    if (auto state = try_parse(); state == ParseState::FAIL)
        return false;
    // Other state, forward the `ctx.pos` and return true
    ctx.pos = temp_pos;
    return true;
}

// Refer: https://github.com/pingcap/tidb/blob/v5.0.1/types/time.go#L2946
MyDateTimeParser::MyDateTimeParser(String format_)
    : format(std::move(format_))
{
    // Ignore all prefix white spaces (TODO: handle unicode space?)
    size_t format_pos = 0;
    while (format_pos < format.size() && isWhitespaceASCII(format[format_pos]))
        format_pos++;

    bool in_pattern_match = false;
    while (format_pos < format.size())
    {
        char x = format[format_pos];
        if (in_pattern_match)
        {
            switch (x)
            {
            case 'b':
            {
                //"%b": Abbreviated month name (Jan..Dec)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    size_t step = 0;
                    auto v = removePrefix(ctx.view, ctx.pos);
                    for (size_t p = 0; p < 12; p++)
                    {
                        if (startsWithCI(v, abbrev_month_names[p]))
                        {
                            time.month = p + 1;
                            step = abbrev_month_names[p].size();
                            break;
                        }
                    }
                    if (step == 0)
                        return false;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'm':
                //"%m": Month, numeric (00..12)
                [[fallthrough]];
            case 'c':
            {
                //"%c": Month, numeric (0..12)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    // To be compatible with TiDB & MySQL, first try to take two digit and parse it as `num`
                    auto [step, month] = parseNDigits(ctx.view, ctx.pos, 2);
                    // Then check whether num is valid month
                    // Note that 0 is valid when sql_mode does not contain NO_ZERO_IN_DATE,NO_ZERO_DATE
                    if (step == 0 || month > 12)
                        return false;
                    time.month = month;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'd': //"%d": Day of the month, numeric (00..31)
                [[fallthrough]];
            case 'e': //"%e": Day of the month, numeric (0..31)
            {
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, day] = parseNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0 || day > 31)
                        return false;
                    time.day = day;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'f':
            {
                //"%f": Microseconds (000000..999999)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, ms] = parseNDigits(ctx.view, ctx.pos, 6);
                    // Empty string is a valid input
                    if (step == 0)
                    {
                        time.micro_second = 0;
                        return true;
                    }
                    // The suffix '0' can be ignored.
                    // "9" means 900000
                    for (size_t i = step; i < 6; i++)
                    {
                        ms *= 10;
                    }
                    time.micro_second = ms;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'k':
                //"%k": Hour (0..23)
                [[fallthrough]];
            case 'H':
            {
                //"%H": Hour (00..23)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, hour] = parseNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0 || hour > 23)
                        return false;
                    ctx.state |= MyDateTimeParser::Context::ST_HOUR_0_23;
                    time.hour = hour;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'l':
                //"%l": Hour (1..12)
                [[fallthrough]];
            case 'I':
                //"%I": Hour (01..12)
                [[fallthrough]];
            case 'h':
            {
                //"%h": Hour (01..12)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, hour] = parseNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0 || hour <= 0 || hour > 12)
                        return false;
                    ctx.state |= MyDateTimeParser::Context::ST_HOUR_1_12;
                    time.hour = hour;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'i':
            {
                //"%i": Minutes, numeric (00..59)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, num] = parseNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0 || num > 59)
                        return false;
                    time.minute = num;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'j':
            {
                //"%j": Day of year (001..366)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                    auto [step, num] = parseNDigits(ctx.view, ctx.pos, 3);
                    if (step == 0 || num == 0 || num > 366)
                        return false;
                    ctx.state |= MyDateTimeParser::Context::ST_DAY_OF_YEAR;
                    ctx.day_of_year = num;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'M':
            {
                //"%M": Month name (January..December)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto v = removePrefix(ctx.view, ctx.pos);
                    size_t step = 0;
                    for (size_t p = 0; p < 12; p++)
                    {
                        if (startsWithCI(v, month_names[p]))
                        {
                            time.month = p + 1;
                            step = month_names[p].size();
                            break;
                        }
                    }
                    if (step == 0)
                        return false;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'S':
                //"%S": Seconds (00..59)
                [[fallthrough]];
            case 's':
            {
                //"%s": Seconds (00..59)
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, second] = parseNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0 || second > 59)
                        return false;
                    time.second = second;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'p':
            {
                //"%p": AM or PM
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                    // Check the offset that will visit
                    if (ctx.view.size - ctx.pos < 2)
                        return false;

                    int meridiem = 0; // 0 - invalid, 1 - am, 2 - pm
                    if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos]) == 'a')
                        meridiem = 1;
                    else if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos]) == 'p')
                        meridiem = 2;

                    if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos + 1]) != 'm')
                        meridiem = 0;

                    if (meridiem == 0)
                        return false;

                    ctx.state |= MyDateTimeParser::Context::ST_MERIDIEM;
                    ctx.meridiem = meridiem;
                    ctx.pos += 2;
                    return true;
                });
                break;
            }
            case 'r':
            {
                //"%r": Time, 12-hour (hh:mm:ss followed by AM or PM)
                parsers.emplace_back(parseTime12Hour);
                break;
            }
            case 'T':
            {
                //"%T": Time, 24-hour (hh:mm:ss)
                parsers.emplace_back(parseTime24Hour);
                break;
            }
            case 'Y':
            {
                //"%Y": Year, numeric, four digits
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, year] = parseYearNDigits(ctx.view, ctx.pos, 4);
                    if (step == 0)
                        return false;
                    time.year = year;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case 'y':
            {
                //"%y": Year, numeric, two digits. Deprecated since MySQL 5.7.5
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                    auto [step, year] = parseYearNDigits(ctx.view, ctx.pos, 2);
                    if (step == 0)
                        return false;
                    time.year = year;
                    ctx.pos += step;
                    return true;
                });
                break;
            }
            case '#':
            {
                //"%#": Skip all numbers
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                    // TODO: Does ASCII numeric the same with unicode numeric?
                    size_t temp_pos = ctx.pos;
                    while (temp_pos < ctx.view.size && isNumericASCII(ctx.view.data[temp_pos]))
                        temp_pos++;
                    ctx.pos = temp_pos;
                    return true;
                });
                break;
            }
            case '.':
            {
                //"%.": Skip all punctation characters
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                    // TODO: Does ASCII punctuation the same with unicode punctuation?
                    size_t temp_pos = ctx.pos;
                    while (temp_pos < ctx.view.size && isPunctuation(ctx.view.data[temp_pos]))
                        temp_pos++;
                    ctx.pos = temp_pos;
                    return true;
                });
                break;
            }
            case '@':
            {
                //"%@": Skip all alpha characters
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                    // TODO: Does ASCII alpha the same with unicode alpha?
                    size_t temp_pos = ctx.pos;
                    while (temp_pos < ctx.view.size && isAlphaASCII(ctx.view.data[temp_pos]))
                        temp_pos++;
                    ctx.pos = temp_pos;
                    return true;
                });
                break;
            }
            case '%':
            {
                //"%%": A literal % character
                parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
#if 0
                        if (ctx.view.data[ctx.pos] != '%')
                            return false;
                        ctx.pos++;
                        return true;
#else
                    // FIXME: Ignored by now, both tidb 5.0.0 and mariadb 10.3.14 can not handle it
                    std::ignore = ctx;
                    return false;
#endif
                });
                break;
            }
            default:
                throw Exception(
                    "Unknown date format pattern, [format=" + format + "] [pattern=" + x + "] [pos=" + DB::toString(format_pos) + "]",
                    ErrorCodes::BAD_ARGUMENTS);
            }
            // end the state of pattern match
            in_pattern_match = false;
            // move format_pos forward
            format_pos++;
            continue;
        }

        if (x == '%')
        {
            in_pattern_match = true;
            // move format_pos forward
            format_pos++;
        }
        else
        {
            // Ignore whitespace for literal forwarding (TODO: handle unicode space?)
            while (format_pos < format.size() && isWhitespaceASCII(format[format_pos]))
                format_pos++;
            // Move forward ctx.view with a sequence of literal `format[format_pos:span_end]`
            size_t span_end = format_pos;
            while (span_end < format.size() && format[span_end] != '%' && !isWhitespaceASCII(format[span_end]))
                ++span_end;
            const size_t span_size = span_end - format_pos;
            if (span_size > 0)
            {
                StringRef format_view{format.data() + format_pos, span_size};
                parsers.emplace_back([format_view](MyDateTimeParser::Context & ctx, MyTimeBase &) {
                    assert(format_view.size > 0);
                    if (format_view.size == 1)
                    {
                        // Shortcut for only 1 char
                        if (ctx.view.data[ctx.pos] != format_view.data[0])
                            return false;
                        ctx.pos += 1;
                        return true;
                    }
                    // Try best to match input as most literal as possible
                    auto v = removePrefix(ctx.view, ctx.pos);
                    size_t v_step = 0;
                    for (size_t format_step = 0; format_step < format_view.size; ++format_step)
                    {
                        // Ignore prefix whitespace for input
                        while (v_step < v.size && isWhitespaceASCII(v.data[v_step]))
                            ++v_step;
                        if (v_step == v.size) // To the end
                            break;
                        // Try to match literal
                        if (v.data[v_step] != format_view.data[format_step])
                            return false;
                        ++v_step;
                    }
                    ctx.pos += v_step;
                    return true;
                });
            }
            // move format_pos forward
            format_pos = span_end;
        }
    }
}

bool mysqlTimeFix(const MyDateTimeParser::Context & ctx, MyTimeBase & my_time)
{
    // TODO: Implement the function that converts day of year to yy:mm:dd
    if (ctx.state & MyDateTimeParser::Context::ST_DAY_OF_YEAR)
    {
        // %j Day of year (001..366) set
        throw Exception("%j set, parsing day of year is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (ctx.state & MyDateTimeParser::Context::ST_MERIDIEM)
    {
        // %H (00..23) set, should not set AM/PM
        if (ctx.state & MyDateTimeParser::Context::ST_HOUR_0_23)
            return false;
        if (my_time.hour == 0)
            return false;
        if (my_time.hour == 12)
        {
            // 12 is a special hour.
            if (ctx.meridiem == 1) // AM
                my_time.hour = 0;
            else if (ctx.meridiem == 2) // PM
                my_time.hour = 12;
            return true;
        }
        if (ctx.meridiem == 2) // PM
            my_time.hour += 12;
    }
    else
    {
        // %h (01..12) set
        if ((ctx.state & MyDateTimeParser::Context::ST_HOUR_1_12) && my_time.hour == 12)
            my_time.hour = 0; // why?
    }
    return true;
}

std::optional<UInt64> MyDateTimeParser::parseAsPackedUInt(const StringRef & str_view) const
{
    MyTimeBase my_time{0, 0, 0, 0, 0, 0, 0};
    MyDateTimeParser::Context ctx(str_view);

    // TODO: can we return warnings to TiDB?
    for (const auto & f : parsers)
    {
        // Ignore all prefix white spaces before each pattern match (TODO: handle unicode space?)
        while (ctx.pos < str_view.size && isWhitespaceASCII(str_view.data[ctx.pos]))
            ctx.pos++;
        // To the end of input, exit (successfully) even if there is more patterns to match
        if (ctx.pos == ctx.view.size)
            break;

        if (!f(ctx, my_time))
        {
#ifndef NDEBUG
            LOG_FMT_TRACE(&Poco::Logger::get("MyDateTimeParser"),
                          "parse error, [str={}] [format={}] [parse_pos={}]",
                          ctx.view.toString(),
                          format,
                          ctx.pos);
#endif
            return std::nullopt;
        }

        // `ctx.pos` > `ctx.view.size` after callback, must be something wrong
        if (unlikely(ctx.pos > ctx.view.size))
        {
            throw Exception(String(__PRETTY_FUNCTION__) + ": parse error, pos overflow. [str=" + ctx.view.toString() + "] [format=" + format
                            + "] [parse_pos=" + DB::toString(ctx.pos) + "] [size=" + DB::toString(ctx.view.size) + "]");
        }
    }
    // Extra characters at the end of date are ignored, but a warning should be reported at this case
    // if (ctx.pos < ctx.view.size) {}

    // Handle the var in `ctx`
    if (!mysqlTimeFix(ctx, my_time))
        return std::nullopt;

    return my_time.toPackedUInt();
}

} // namespace DB
