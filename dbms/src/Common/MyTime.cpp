#include <Common/MyTime.h>
#include <Functions/FunctionsDateTime.h>
#include <Poco/String.h>

#include <cctype>
#include <initializer_list>
#include <vector>

namespace DB
{

int adjustYear(int year)
{
    if (year >= 0 && year <= 69)
        return 2000 + year;
    if (year >= 70 && year <= 99)
        return 1900 + year;
    return year;
}

void scanTimeArgs(const std::vector<String> & seps, std::initializer_list<int *> && list)
{
    int i = 0;
    for (auto * ptr : list)
    {
        *ptr = std::stoi(seps[i]);
        i++;
    }
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

    return previous_parts == 2 && (c == ' ' || c == 'T');
}

std::vector<String> parseDateFormat(String format)
{
    format = Poco::trimInPlace(format);

    if (format.size() == 0)
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
        {30, {2, 0}},  // 23:59:59+08
        {50, {4, 2}},  // 23:59:59+0800
        {63, {5, 2}},  // 23:59:59+08:00
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
        auto validate = [](const String & v) { return '0' <= v[0] && v[0] <= '9' && '0' <= v[1] && v[1] <= '9'; };
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
    return (c >= 0x21 && c <= 0x2F) || (c >= 0x3A && c <= 0x40) || (c >= 0x5B && c <= 0x60) || (c >= 0x7B && c <= 0x7E);
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
    return std::make_tuple(std::move(seps), std::move(frac), std::move(has_tz), std::move(tz_sign), std::move(tz_hour), std::move(tz_sep),
        std::move(tz_minute));
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
    : year(year_), month(month_), day(day_), hour(hour_), minute(minute_), second(second_), micro_second(micro_second_)
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
        week_day = (week_day + calcDaysInYear(year)) % 7;
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

// TODO: support parse time from float string
Field parseMyDateTime(const String & str, int8_t fsp)
{
    // Since we only use DateLUTImpl as parameter placeholder of AddSecondsImpl::execute
    // and it's costly to construct a DateLUTImpl, a shared static instance is enough.
    static const DateLUTImpl lut = DateLUT::instance("UTC");

    Int32 year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, delta_hour = 0, delta_minute = 0;

    bool hhmmss = false;

    auto [seps, frac_str, has_tz, tz_sign, tz_hour, tz_sep, tz_minute] = splitDatetime(str);

    bool truncated_or_incorrect = false;

    // noAbsorb tests if can absorb FSP or TZ
    auto noAbsorb = [](const std::vector<String> & seps) {
        // if we have more than 5 parts (i.e. 6), the tailing part can't be absorbed
        // or if we only have 1 part, but its length is longer than 4, then it is at least YYMMD, in this case, FSP can
        // not be absorbed, and it will be handled later, and the leading sign prevents TZ from being absorbed, because
        // if date part has no separators, we can't use -/+ as separators between date & time.
        return seps.size() > 5 || (seps.size() == 1 && seps[0].size() > 4);
    };

    if (!frac_str.empty())
    {
        if (!noAbsorb(seps))
        {
            seps.push_back(frac_str);
            frac_str = "";
        }
    }

    if (has_tz && !tz_sign.empty())
    {
        // if tz_sign is empty, it's sure that the string literal contains timezone (e.g., 2010-10-10T10:10:10Z),
        // therefore we could safely skip this branch.
        if (!noAbsorb(seps) && !(tz_minute != "" && tz_sep == ""))
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
                    std::sscanf(seps[0].c_str(), "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second);
                    hhmmss = true;
                    break;
                }
                case 12: // YYMMDDHHMMSS
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second);
                    year = adjustYear(year);
                    hhmmss = true;
                    break;
                }
                case 11: // YYMMDDHHMMS
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute, &second);
                    year = adjustYear(year);
                    hhmmss = true;
                    break;
                }
                case 10: // YYMMDDHHMM
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute);
                    year = adjustYear(year);
                    break;
                }
                case 9: // YYMMDDHHM
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute);
                    year = adjustYear(year);
                    break;
                }
                case 8: // YYYYMMDD
                {
                    std::sscanf(seps[0].c_str(), "%4d%2d%2d", &year, &month, &day);
                    break;
                }
                case 7: // YYMMDDH
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%1d", &year, &month, &day, &hour);
                    year = adjustYear(year);
                    break;
                }
                case 6: // YYMMDD
                case 5: // YYMMD
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d", &year, &month, &day);
                    year = adjustYear(year);
                    break;
                }
                default:
                {
                    throw TiFlashException("Wrong datetime format: " + str, Errors::Types::WrongValue);
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
                        ret = 1;
                        break;
                    case 1:
                    case 2:
                    {
                        ret = std::sscanf(frac_str.c_str(), "%2d ", &hour);
                        break;
                    }
                    case 3:
                    case 4:
                    {
                        ret = std::sscanf(frac_str.c_str(), "%2d%2d ", &hour, &minute);
                        break;
                    }
                    default:
                    {
                        ret = std::sscanf(frac_str.c_str(), "%2d%2d%2d ", &hour, &minute, &second);
                        break;
                    }
                }
                truncated_or_incorrect = (ret == 0);
            }
            if (l == 9 || l == 10)
            {
                if (frac_str.empty())
                {
                    second = 0;
                }
                else
                {
                    truncated_or_incorrect = (std::sscanf(frac_str.c_str(), "%2d ", &second) == 0);
                }
            }
            if (truncated_or_incorrect)
            {
                throw TiFlashException("Datetime truncated: " + str, Errors::Types::Truncated);
            }
            break;
        }
        case 3:
        {
            // YYYY-MM-DD
            scanTimeArgs(seps, {&year, &month, &day});
            break;
        }
        case 4:
        {
            // YYYY-MM-DD HH
            scanTimeArgs(seps, {&year, &month, &day, &hour});
            break;
        }
        case 5:
        {
            // YYYY-MM-DD HH-MM
            scanTimeArgs(seps, {&year, &month, &day, &hour, &minute});
            break;
        }
        case 6:
        {
            // We don't have fractional seconds part.
            // YYYY-MM-DD HH-MM-SS
            scanTimeArgs(seps, {&year, &month, &day, &hour, &minute, &second});
            hhmmss = true;
            break;
        }
        default:
        {
            throw Exception("Wrong datetime format");
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
                UInt64 result = AddSecondsImpl::execute(datetime.toPackedUInt(), 1, lut);
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

    MyDateTime result(year, month, day, hour, minute, second, micro_second);

    if (has_tz)
    {
        if (!hhmmss)
        {
            throw TiFlashException("Invalid datetime value: " + str, Errors::Types::WrongValue);
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
            throw TiFlashException("Invalid datetime value: " + str, Errors::Types::WrongValue);
        }
        // by default, if the temporal string literal does not contain timezone information, it will be in the timezone
        // specified by the time_zone system variable. However, if the timezone is specified in the string literal, we
        // will use the specified timezone to interpret the string literal and convert it into the system timezone.
        int offset = delta_hour * 60 * 60 + delta_minute * 60;
        if (tz_sign == "-")
        {
            offset = -offset;
        }
        auto tmp = AddSecondsImpl::execute(result.toPackedUInt(), -offset, lut);
        result = MyDateTime(tmp);
    }

    return result.toPackedUInt();
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

inline bool isZeroDate(UInt64 time) { return time == 0; }

inline bool supportedByDateLUT(const MyDateTime & my_time) { return my_time.year >= 1970; }

/// DateLUT only support time from year 1970, in some corner cases, the input date may be
/// 1969-12-31, need extra logical to handle it
inline time_t getEpochSecond(const MyDateTime & my_time, const DateLUTImpl & time_zone)
{
    if likely (supportedByDateLUT(my_time))
        return time_zone.makeDateTime(my_time.year, my_time.month, my_time.day, my_time.hour, my_time.minute, my_time.second);
    if likely (my_time.year == 1969 && my_time.month == 12 && my_time.day == 31)
    {
        /// - 3600 * 24 + my_time.hour * 3600 + my_time.minute * 60 + my_time.second is UTC based, need to adjust
        /// the epoch according to the input time_zone
        return -3600 * 24 + my_time.hour * 3600 + my_time.minute * 60 + my_time.second - time_zone.getOffsetAtStartEpoch();
    }
    else
    {
        throw Exception("Unsupported timestamp value , TiFlash only support timestamp after 1970-01-01 00:00:00 UTC)");
    }
}

void convertTimeZone(UInt64 from_time, UInt64 & to_time, const DateLUTImpl & time_zone_from, const DateLUTImpl & time_zone_to)
{
    if (isZeroDate(from_time))
    {
        to_time = from_time;
        return;
    }
    MyDateTime from_my_time(from_time);
    time_t epoch = getEpochSecond(from_my_time, time_zone_from);
    if (unlikely(epoch + time_zone_to.getOffsetAtStartEpoch() + SECONDS_PER_DAY < 0))
        throw Exception("Unsupported timestamp value , TiFlash only support timestamp after 1970-01-01 00:00:00 UTC)");
    MyDateTime to_my_time(time_zone_to.toYear(epoch), time_zone_to.toMonth(epoch), time_zone_to.toDayOfMonth(epoch),
        time_zone_to.toHour(epoch), time_zone_to.toMinute(epoch), time_zone_to.toSecond(epoch), from_my_time.micro_second);
    to_time = to_my_time.toPackedUInt();
}

void convertTimeZoneByOffset(UInt64 from_time, UInt64 & to_time, Int64 offset, const DateLUTImpl & time_zone)
{
    if (isZeroDate(from_time))
    {
        to_time = from_time;
        return;
    }
    MyDateTime from_my_time(from_time);
    time_t epoch = getEpochSecond(from_my_time, time_zone);
    epoch += offset;
    if (unlikely(epoch + SECONDS_PER_DAY < 0))
        throw Exception("Unsupported timestamp value , TiFlash only support timestamp after 1970-01-01 00:00:00 UTC)");
    MyDateTime to_my_time(time_zone.toYear(epoch), time_zone.toMonth(epoch), time_zone.toDayOfMonth(epoch), time_zone.toHour(epoch),
        time_zone.toMinute(epoch), time_zone.toSecond(epoch), from_my_time.micro_second);
    to_time = to_my_time.toPackedUInt();
}

// the implementation is the same as TiDB
int calcDayNum(int year, int month, int day)
{
    if (year == 0 || month == 0)
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

size_t maxFormattedDateTimeStringLength(const String & format)
{
    size_t result = 0;
    bool in_pattern_match = false;
    for (size_t i = 0; i < format.size(); i++)
    {
        char x = format[i];
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

void MyTimeBase::check(bool allow_zero_in_date, bool allow_invalid_date) const
{
    if (!(year == 0 && month == 0 && day == 0))
    {
        if (!allow_zero_in_date && (month == 0 || day == 0))
        {
            char buff[] = "0000-00-00";
            std::sprintf(buff, "%04d-%02d-%02d", year, month, day);
            throw TiFlashException("Incorrect datetime value: " + String(buff), Errors::Types::WrongValue);
        }
    }

    if (year >= 9999 || month > 12)
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }

    UInt8 max_day = 31;
    if (!allow_invalid_date)
    {
        constexpr static UInt8 max_days_in_month[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        static auto is_leap_year = [](UInt16 _year) { return ((_year % 4 == 0) && (_year % 100 != 0)) || (_year % 400 == 0); };
        max_day = max_days_in_month[month - 1];
        if (month == 2 && is_leap_year(year))
        {
            max_day = 29;
        }
    }
    if (day > max_day)
    {
        char buff[] = "0000-00-00";
        std::sprintf(buff, "%04d-%02d-%02d", year, month, day);
        throw TiFlashException("Incorrect datetime value: " + String(buff), Errors::Types::WrongValue);
    }

    if (hour < 0 || hour >= 24)
    {
        throw TiFlashException("Incorrect datetime value", Errors::Types::WrongValue);
    }
    if (minute >= 60)
    {
        throw TiFlashException("Incorrect datetime value", Errors::Types::WrongValue);
    }
    if (second >= 60)
    {
        throw TiFlashException("Incorrect datetime value", Errors::Types::WrongValue);
    }
    return;
}

bool toCoreTimeChecked(const UInt64 & year, const UInt64 & month, const UInt64 & day, const UInt64 & hour, const UInt64 & minute,
    const UInt64 & second, const UInt64 & microsecond, MyDateTime & result)
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

MyDateTimeFormatter::MyDateTimeFormatter(const String & layout)
{
    bool in_pattern_match = false;
    for (size_t i = 0; i < layout.size(); i++)
    {
        char x = layout[i];
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

} // namespace DB
