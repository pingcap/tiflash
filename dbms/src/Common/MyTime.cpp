#include <Common/MyTime.h>
#include <Functions/FunctionsDateTime.h>
#include <Poco/String.h>

#include <cctype>
#include <chrono>
#include <initializer_list>
#include <vector>

namespace DB
{

using std::sprintf;

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

std::vector<String> parseDateFormat(String format)
{
    format = Poco::trimInPlace(format);

    std::vector<String> seps;
    size_t start = 0;
    for (size_t i = 0; i < format.size(); i++)
    {
        if (i == 0 || i + 1 == format.size())
        {
            if (!std::isdigit(format[i]))
                return {};
            continue;
        }

        if (!std::isdigit(format[i]))
        {
            if (!std::isdigit(format[i - 1]))
                return {};
            seps.push_back(format.substr(start, i - start));
            start = i + 1;
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
std::tuple<int, String, String, String, String> getTimeZone(String literal)
{
    static auto valid_idx_combinations = std::map<int, std::tuple<int, int>>{
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
        auto [h, m] = valid_idx_combinations[k];
        int hidx = l - h;
        int midx = l - m;
        auto validate = [](const String & v) { return '0' <= v[0] && v[0] <= '9' && '0' <= v[1] && v[1] <= '9'; };
        if (sidx != -1)
        {
            tz_sign = literal.substr(sidx, sidx + 1);
            idx = sidx;
        }
        if (zidx != -1)
        {
            idx = zidx;
        }
        if ((l - spidx) == 3)
        {
            tz_sep = literal.substr(spidx, spidx + 1);
        }
        if (h != 0)
        {
            tz_hour = literal.substr(hidx, hidx + 2);
            if (!validate(tz_hour))
            {
                return std::make_tuple(-1, "", "", "", "");
            }
        }
        if (m != 0)
        {
            tz_minute = literal.substr(midx, midx + 2);
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
        while (frac_idx > 0 && isPunctuation(format[tz_idx - 1]))
        {
            // in case of multiple separators, e.g. 2020-10--10
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
String MyTimeBase::dateFormat(const String & layout) const
{
    String result;
    bool in_pattern_match = false;
    for (size_t i = 0; i < layout.size(); i++)
    {
        char x = layout[i];
        if (in_pattern_match)
        {
            convertDateFormat(x, result);
            in_pattern_match = false;
            continue;
        }

        if (x == '%')
            in_pattern_match = true;
        else
            result.push_back(x);
    }
    return result;
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

String abbrDayOfMonth(int day)
{
    switch (day)
    {
        case 1:
        case 21:
        case 31:
            return "st";
        case 2:
        case 22:
            return "nd";
        case 3:
        case 23:
            return "rd";
        default:
            return "th";
    }
}

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

// the implementation is the same as TiDB
void MyTimeBase::convertDateFormat(char c, String & result) const
{
    switch (c)
    {
        case 'b':
        {
            if (month == 0 || month > 12)
            {
                throw Exception("invalid time format");
            }
            result.append(abbrev_month_names[month - 1]);
            break;
        }
        case 'M':
        {
            if (month == 0 || month > 12)
            {
                throw Exception("invalid time format");
            }
            result.append(month_names[month - 1]);
            break;
        }
        case 'm':
        {
            char buf[16];
            sprintf(buf, "%02d", month);
            result.append(String(buf));
            break;
        }
        case 'c':
        {
            char buf[16];
            sprintf(buf, "%d", month);
            result.append(String(buf));
            break;
        }
        case 'D':
        {
            char buf[16];
            sprintf(buf, "%d", day);
            result.append(String(buf));
            result.append(abbrDayOfMonth(day));
            break;
        }
        case 'd':
        {
            char buf[16];
            sprintf(buf, "%02d", day);
            result.append(String(buf));
            break;
        }
        case 'e':
        {
            char buf[16];
            sprintf(buf, "%d", day);
            result.append(String(buf));
            break;
        }
        case 'j':
        {
            char buf[16];
            sprintf(buf, "%03d", yearDay());
            result.append(String(buf));
            break;
        }
        case 'H':
        {
            char buf[16];
            sprintf(buf, "%02d", hour);
            result.append(String(buf));
            break;
        }
        case 'k':
        {
            char buf[16];
            sprintf(buf, "%d", hour);
            result.append(String(buf));
            break;
        }
        case 'h':
        case 'I':
        {
            char buf[16];
            if (hour % 12 == 0)
                sprintf(buf, "%d", 12);
            else
                sprintf(buf, "%02d", hour);
            result.append(String(buf));
            break;
        }
        case 'l':
        {
            char buf[16];
            if (hour % 12 == 0)
                sprintf(buf, "%d", 12);
            else
                sprintf(buf, "%d", hour);
            result.append(String(buf));
            break;
        }
        case 'i':
        {
            char buf[16];
            sprintf(buf, "%02d", minute);
            result.append(String(buf));
            break;
        }
        case 'p':
        {
            if ((hour / 12) % 2)
                result.append(String("PM"));
            else
                result.append(String("AM"));
            break;
        }
        case 'r':
        {
            char buf[24];
            auto h = hour % 24;
            if (h == 0)
                sprintf(buf, "%02d:%02d:%02d AM", 12, minute, second);
            else if (h == 12)
                sprintf(buf, "%02d:%02d:%02d PM", 12, minute, second);
            else if (h < 12)
                sprintf(buf, "%02d:%02d:%02d AM", h, minute, second);
            else
                sprintf(buf, "%02d:%02d:%02d PM", h - 12, minute, second);
            result.append(String(buf));
            break;
        }
        case 'T':
        {
            char buf[24];
            sprintf(buf, "%02d:%02d:%02d", hour, minute, second);
            result.append(String(buf));
            break;
        }
        case 'S':
        case 's':
        {
            char buf[16];
            sprintf(buf, "%02d", second);
            result.append(String(buf));
            break;
        }
        case 'f':
        {
            char buf[16];
            sprintf(buf, "%06d", micro_second);
            result.append(String(buf));
            break;
        }
        case 'U':
        {
            char buf[16];
            auto w = week(0);
            sprintf(buf, "%02d", w);
            result.append(String(buf));
            break;
        }
        case 'u':
        {
            char buf[16];
            auto w = week(1);
            sprintf(buf, "%02d", w);
            result.append(String(buf));
            break;
        }
        case 'V':
        {
            char buf[16];
            auto w = week(2);
            sprintf(buf, "%02d", w);
            result.append(String(buf));
            break;
        }
        case 'v':
        {
            char buf[16];
            auto [year, week] = calcWeek(3);
            std::ignore = year;
            sprintf(buf, "%02d", week);
            result.append(String(buf));
            break;
        }
        case 'a':
        {
            auto week_day = weekDay();
            result.append(abbrev_weekday_names[week_day]);
            break;
        }
        case 'W':
        {
            auto week_day = weekDay();
            result.append(weekday_names[week_day]);
            break;
        }
        case 'w':
        {
            auto week_day = weekDay();
            result.append(std::to_string(week_day));
            break;
        }
        case 'X':
        {
            char buf[16];
            auto [year, week] = calcWeek(6);
            std::ignore = week;
            sprintf(buf, "%04d", year);
            result.append(String(buf));
            break;
        }
        case 'x':
        {
            char buf[16];
            auto [year, week] = calcWeek(3);
            std::ignore = week;
            sprintf(buf, "%04d", year);
            result.append(String(buf));
            break;
        }
        case 'Y':
        {
            char buf[16];
            sprintf(buf, "%04d", year);
            result.append(String(buf));
            break;
        }
        case 'y':
        {
            char buf[16];
            sprintf(buf, "%02d", year % 100);
            result.append(String(buf));
            break;
        }
        default:
        {
            result.push_back(c);
        }
    }
}

// TODO: support parse time from float string
Field parseMyDateTime(const String & str, int8_t fsp)
{
    Int32 year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, delta_hour = 0, delta_minute = 0;

    bool hhmmss = false;

    auto [seps, frac_str, has_tz, tz_sign, tz_hour, tz_sep, tz_minute] = splitDatetime(str);

    bool truncated_or_incorrect = false;

    // noAbsorb tests if can absorb FSP or TZ
    auto noAbsorb = [](std::vector<String> seps) {
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
                UInt64 result = AddSecondsImpl::execute(datetime.toPackedUInt(), 1, DateLUT::instance());
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
        auto tmp = AddSecondsImpl::execute(result.toPackedUInt(), -offset, DateLUT::instance());
        result = MyDateTime(tmp);
    }

    return result.toPackedUInt();
}

String MyDateTime::toString(int fsp) const
{
    String result = dateFormat("%Y-%m-%d %H:%i:%s");
    if (fsp > 0)
    {
        char buf[16];
        sprintf(buf, ".%06d", micro_second);
        result.append(String(buf, fsp + 1));
    }
    return result;
}

bool isZeroDate(UInt64 time) { return time == 0; }

void convertTimeZone(UInt64 from_time, UInt64 & to_time, const DateLUTImpl & time_zone_from, const DateLUTImpl & time_zone_to)
{
    if (isZeroDate(from_time))
    {
        to_time = from_time;
        return;
    }
    MyDateTime from_my_time(from_time);
    time_t epoch = time_zone_from.makeDateTime(
        from_my_time.year, from_my_time.month, from_my_time.day, from_my_time.hour, from_my_time.minute, from_my_time.second);
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
    time_t epoch = time_zone.makeDateTime(
        from_my_time.year, from_my_time.month, from_my_time.day, from_my_time.hour, from_my_time.minute, from_my_time.second);
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

MyDateTime numberToDateTime(Int64 number)
{
    MyDateTime datetime(0);

    auto get_datetime = [](const Int64 & num) {
        auto ymd = num / 1000000;
        auto hms = num - ymd * 1000000;

        UInt16 year = ymd / 10000;
        ymd %= 10000;
        UInt8 month = ymd / 100;
        UInt8 day = ymd % 100;

        UInt16 hour = hms / 10000;
        hms %= 10000;
        UInt8 minute = hms / 100;
        UInt8 second = hms % 100;

        return MyDateTime(year, month, day, hour, minute, second, 0);
    };

    if (number == 0)
        return datetime;

    // datetime type
    if (number >= 10000101000000)
    {
        return get_datetime(number);
    }

    // check MMDD
    if (number < 101)
    {
        throw TiFlashException("Cannot convert " + std::to_string(number) + " to Datetime", Errors::Types::WrongValue);
    }

    // check YYMMDD: 2000-2069
    if (number <= 69 * 10000 + 1231)
    {
        number = (number + 200000000) * 1000000;
        return get_datetime(number);
    }

    // check YYMMDD
    if (number <= 991231)
    {
        number = (number + 19000000) * 1000000;
        return get_datetime(number);
    }

    // check YYYYMMDD
    if (number <= 10000101)
    {
        throw TiFlashException("Cannot convert " + std::to_string(number) + " to Datetime", Errors::Types::WrongValue);
    }

    // check hhmmss
    if (number <= 99991231)
    {
        number = number * 1000000;
        return get_datetime(number);
    }

    // check MMDDhhmmss
    if (number < 101000000)
    {
        throw TiFlashException("Cannot convert " + std::to_string(number) + " to Datetime", Errors::Types::WrongValue);
    }

    // check YYMMDDhhmmss: 2000-2069
    if (number <= 69 * 10000000000 + 1231235959)
    {
        number += 20000000000000;
        return get_datetime(number);
    }

    // check YYMMDDhhmmss
    if (number < 70 * 10000000000 + 101000000)
    {
        throw TiFlashException("Cannot convert " + std::to_string(number) + " to Datetime", Errors::Types::WrongValue);
    }

    if (number <= 991231235959)
    {
        number += 19000000000000;
        return get_datetime(number);
    }

    return get_datetime(number);
}

} // namespace DB
