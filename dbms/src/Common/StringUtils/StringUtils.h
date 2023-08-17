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

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>


namespace detail
{
bool startsWith(const char * s, size_t size, const char * prefix, size_t prefix_size);
bool endsWith(const char * s, size_t size, const char * suffix, size_t suffix_size);

// case insensitive version
bool startsWithCI(const char * s, size_t size, const char * prefix, size_t prefix_size);
bool endsWithCI(const char * s, size_t size, const char * suffix, size_t suffix_size);
} // namespace detail


inline bool startsWith(const std::string & s, const std::string & prefix)
{
    return detail::startsWith(s.data(), s.size(), prefix.data(), prefix.size());
}

inline bool endsWith(const std::string & s, const std::string & suffix)
{
    return detail::endsWith(s.data(), s.size(), suffix.data(), suffix.size());
}


/// With GCC, strlen is evaluated compile time if we pass it a constant
/// string that is known at compile time.
inline bool startsWith(const std::string & s, const char * prefix)
{
    return detail::startsWith(s.data(), s.size(), prefix, strlen(prefix));
}

inline bool endsWith(const std::string & s, const char * suffix)
{
    return detail::endsWith(s.data(), s.size(), suffix, strlen(suffix)); //
}

/// Given an integer, return the adequate suffix for
/// printing an ordinal number.
template <typename T>
std::string getOrdinalSuffix(T n)
{
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>, "Unsigned integer value required");

    const auto last_digit = n % 10;

    if ((last_digit < 1 || last_digit > 3) || ((n > 10) && (((n / 10) % 10) == 1)))
        return "th";

    switch (last_digit)
    {
    case 1:
        return "st";
    case 2:
        return "nd";
    case 3:
        return "rd";
    default:
        return "th";
    };
}

/// More efficient than libc, because doesn't respect locale. But for some functions table implementation could be better.

inline bool isASCII(char c)
{
    return static_cast<unsigned char>(c) < 0x80;
}

inline bool isUpperAlphaASCII(char c)
{
    return (c >= 'A' && c <= 'Z');
}

inline bool isLowerAplhaASCII(char c)
{
    return (c >= 'a' && c <= 'z');
}

inline bool isAlphaASCII(char c)
{
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

inline bool isNumericASCII(char c)
{
    return (c >= '0' && c <= '9');
}

inline bool isHexDigit(char c)
{
    return isNumericASCII(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

inline bool isAlphaNumericASCII(char c)
{
    return isAlphaASCII(c) || isNumericASCII(c);
}

inline bool isWordCharASCII(char c)
{
    return isAlphaNumericASCII(c) || c == '_';
}

inline bool isValidIdentifierBegin(char c)
{
    return isAlphaASCII(c) || c == '_';
}

inline bool isWhitespaceASCII(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/// Works assuming isAlphaASCII.
inline char toLowerIfAlphaASCII(char c)
{
    return c | 0x20;
}

inline char toUpperIfAlphaASCII(char c)
{
    return c & (~0x20);
}

inline char alternateCaseIfAlphaASCII(char c)
{
    return c ^ 0x20;
}

inline bool equalsCaseInsensitive(char a, char b)
{
    return a == b || (isAlphaASCII(a) && alternateCaseIfAlphaASCII(a) == b);
}

inline bool isPunctuation(char c)
{
    return (c >= 0x21 && c <= 0x2F) || (c >= 0x3A && c <= 0x40) || (c >= 0x5B && c <= 0x60) || (c >= 0x7B && c <= 0x7E);
}

// match at lease x digits
inline std::tuple<std::string, std::string, bool> digit(const std::string & str, int x)
{
    for (size_t i = 0, times = x; i < times && i < str.length() && isdigit(str[i]); ++i, --x)
    {
        // find x digit
    }
    if (x > 0)
    {
        return {"", str, false};
    }
    size_t idx = 0;
    for (auto sz = str.length(); idx < sz && isdigit(str[idx]); ++idx)
    {
        // remove all remaining spaces
    }
    return {str.substr(0, idx), str.substr(idx), true};
}

// remove at lease x spaces
inline std::pair<std::string, bool> space(const std::string & str, int x)
{
    for (size_t i = 0, times = x, sz = str.length(); i < times && i < sz && isspace(str[i]); ++i, --x)
    {
        // find x spaces
    }
    if (x > 0)
    {
        return {str, false};
    }
    size_t idx = 0;
    for (auto sz = str.length(); idx < sz && isspace(str[idx]); ++idx)
    {
        // remove all remaining spaces
    }
    return {str.substr(idx), true};
}

// remove at least one space
inline std::pair<std::string, bool> space1(const std::string & str)
{
    return space(str, 1);
}

// remove at least zero space
inline std::string space0(const std::string & str)
{
    return space(str, 0).first;
}

inline std::pair<std::string, bool> anyPunct(const std::string & str)
{
    if (!str.empty() && isPunctuation(str[0]))
    {
        return {str.substr(1), true};
    }
    return {str, false};
}

inline std::pair<std::string, bool> matchColon(const std::string & str)
{
    std::string rest = space0(str);
    if (rest.empty() || rest[0] != ':')
    {
        return {str, false};
    }
    rest = rest.substr(1);
    rest = space0(rest);
    return {rest, true};
}

// number, idx of rest string, matched
inline std::tuple<int, int, bool> number(const std::string & str)
{
    size_t idx = 0;
    for (auto sz = str.length(); idx < sz && isdigit(str[idx]); ++idx)
    {
        // find digit string
    }
    std::string digits = str.substr(0, idx);
    try
    {
        int result = std::stoi(digits);
        return {result, idx, true};
    }
    catch (std::exception & e)
    {
        return {0, 0, false};
    }
}
