#include <Common/MyTime.h>

#include <cctype>
#include <initializer_list>
#include <vector>

#include <Poco/String.h>

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

void MyTimeBase::convertDateFormat(char c, String & result) const
{
    // TODO:: Implement other formats.
    switch (c)
    {
        //case 'b':
        //{
        //    if (month == 0 || month > 12)
        //    {
        //        throw Exception("invalid time format");
        //    }
        //    result.append(String(MonthNames[month-1], 3));
        //    break;
        //}
        //case 'M':
        //{
        //    if (month == 0 || month > 12)
        //    {
        //        throw Exception("invalid time format");
        //    }
        //    result.append(String(MonthNames[month-1], 3));
        //    break;
        //}
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
        //case 'D':
        //{
        //    char buf[16];
        //    sprintf(buf, "%d", month);
        //    result.append(String(buf));
        //    result.append(abbrDayOfMonth(day));
        //    break;
        //}
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
        //case 'j':
        //{
        //    char buf[16];
        //    sprintf(buf, "%03d", yearDay());
        //    result.append(String(buf));
        //    break;
        //}
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
        //case 'U':
        //{
        //    char buf[16];
        //    auto w = week(0);
        //    sprintf(buf, "%02d", w);
        //    result.append(String(buf));
        //    break;
        //}
        //case 'u':
        //{
        //    char buf[16];
        //    auto w = week(1);
        //    sprintf(buf, "%02d", w);
        //    result.append(String(buf));
        //    break;
        //}
        //case 'V':
        //{
        //    char buf[16];
        //    auto w = week(2);
        //    sprintf(buf, "%02d", w);
        //    result.append(String(buf));
        //    break;
        //}
        //case 'v':
        //{
        //    char buf[16];
        //    auto w = yearWeek(2);
        //    sprintf(buf, "%02d", w);
        //    result.append(String(buf));
        //    break;
        //}
        //case 'a':
        //{
        //    auto weekDay = weekDay();
        //    result.append(String(abbrevWeekdayName[weekDay]));
        //    break;
        //}
        //case 'w':
        //{
        //    auto weekDay = weekDay();
        //    result.append(std::to_string(weekDay));
        //    break;
        //}
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

bool isZeroDate(UInt64 time)
{
    return time == 0;
}

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
    MyDateTime to_my_time(time_zone.toYear(epoch), time_zone.toMonth(epoch), time_zone.toDayOfMonth(epoch),
                          time_zone.toHour(epoch), time_zone.toMinute(epoch), time_zone.toSecond(epoch), from_my_time.micro_second);
    to_time = to_my_time.toPackedUInt();
}

} // namespace DB
