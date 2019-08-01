#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <common/DateLUT.h>
#include <cctype>
#include <initializer_list>
#include <vector>

#include <iostream>

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
    size_t ltrim = 0;
    for (; ltrim < format.size(); ltrim++)
    {
        if (!std::isspace(ltrim))
            break;
    }
    format = format.substr(ltrim);
    size_t rtrim = format.size();
    for (; rtrim != 0; rtrim--)
        if (!std::isspace(rtrim))
            break;
    format = format.substr(0, rtrim);

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

std::vector<String> splitDatetime(String format)
{
    int idx = getFracIndex(format);
    if (idx > 0)
    {
        format = format.substr(0, idx);
    }
    return parseDateFormat(format);
}


Field parseMyDatetime(const String & str, bool is_date)
{
    Int32 year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;

    const auto & seps = splitDatetime(str);

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
                    // TODO Process frac!
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
    const auto & date_lut = DateLUT::instance();
    if (is_date)
    {
        auto date = date_lut.makeDayNum(year, month, day);
        Field date_field(static_cast<Int64>(date));
        return date_field;
    }
    else
    {
        auto datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
        return Field(datetime);
    }
}
} // namespace DB
