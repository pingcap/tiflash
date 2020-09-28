#include <Common/MyTime.h>
#include <Poco/String.h>

#include <cctype>
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

std::pair<std::vector<String>, String> splitDatetime(String format)
{
    int idx = getFracIndex(format);
    String frac;
    if (idx > 0)
    {
        frac = format.substr(idx + 1);
        format = format.substr(0, idx);
    }
    return std::make_pair(parseDateFormat(format), std::move(frac));
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

Field parseMyDateTime(const String & str)
{
    Int32 year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;

    auto [seps, frac_str] = splitDatetime(str);

    switch (seps.size())
    {
        // No delimiter
        case 1:
        {
            size_t l = seps[0].size();
            switch (l)
            {
                case 14:
                    // YYYYMMDDHHMMSS
                    {
                        std::sscanf(seps[0].c_str(), "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second);
                        break;
                    }
                case 12:
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second);
                    year = adjustYear(year);
                    break;
                }
                case 11:
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute, &second);
                    year = adjustYear(year);
                    break;
                }
                case 10:
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute);
                    year = adjustYear(year);
                    break;
                }
                case 9:
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute);
                    year = adjustYear(year);
                    break;
                }
                case 8:
                {
                    std::sscanf(seps[0].c_str(), "%4d%2d%2d", &year, &month, &day);
                    break;
                }
                case 6:
                case 5:
                {
                    std::sscanf(seps[0].c_str(), "%2d%2d%2d", &year, &month, &day);
                    year = adjustYear(year);
                    break;
                }
                default:
                {
                    throw Exception("Wrong datetime format");
                }
            }
            break;
        }
        case 3:
        {
            scanTimeArgs(seps, {&year, &month, &day});
            break;
        }
        case 6:
        {
            scanTimeArgs(seps, {&year, &month, &day, &hour, &minute, &second});
            break;
        }
        default:
        {
            throw Exception("Wrong datetime format");
        }
    }

    UInt32 micro_second = 0;
    // TODO This is a simple implement, without processing overflow.
    if (frac_str.size() > 6)
    {
        frac_str = frac_str.substr(0, 6);
    }

    if (frac_str.size() > 0)
    {
        micro_second = std::stoul(frac_str);
        for (size_t i = frac_str.size(); i < 6; i++)
            micro_second *= 10;
    }

    return MyDateTime(year, month, day, hour, minute, second, micro_second).toPackedUInt();
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
        throw Exception("Cannot convert " + std::to_string(number) + " to Datetime");
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
        throw Exception("Cannot convert " + std::to_string(number) + " to Datetime");
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
        throw Exception("Cannot convert " + std::to_string(number) + " to Datetime");
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
        throw Exception("Cannot convert " + std::to_string(number) + " to Datetime");
    }

    if (number <= 991231235959)
    {
        number += 19000000000000;
        return get_datetime(number);
    }

    return get_datetime(number);
}

} // namespace DB
