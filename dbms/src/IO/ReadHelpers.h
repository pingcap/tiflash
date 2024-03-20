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

#include <Common/Arena.h>
#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <common/StringRef.h>
#include <double-conversion/double-conversion.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iterator>
#include <limits>
#include <type_traits>

#define DEFAULT_MAX_STRING_SIZE 0x00FFFFFFULL

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PARSE_DATE;
extern const int CANNOT_PARSE_DATETIME;
extern const int CANNOT_PARSE_UUID;
extern const int CANNOT_READ_ARRAY_FROM_TEXT;
extern const int CANNOT_PARSE_NUMBER;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

/// Helper functions for formatted input.

inline char parseEscapeSequence(char c)
{
    switch (c)
    {
    case 'a':
        return '\a';
    case 'b':
        return '\b';
    case 'e':
        return '\x1B'; /// \e escape sequence is non standard for C and C++ but supported by gcc and clang.
    case 'f':
        return '\f';
    case 'n':
        return '\n';
    case 'r':
        return '\r';
    case 't':
        return '\t';
    case 'v':
        return '\v';
    case '0':
        return '\0';
    default:
        return c;
    }
}


/// These functions are located in VarInt.h
/// inline void throwReadAfterEOF()


inline void readChar(char & x, ReadBuffer & buf)
{
    if (!buf.eof())
    {
        x = *buf.position();
        ++buf.position();
    }
    else
        throwReadAfterEOF();
}

inline void readChar(unsigned char & x, ReadBuffer & buf)
{
    if (!buf.eof())
    {
        x = static_cast<unsigned char>(*buf.position());
        ++buf.position();
    }
    else
        throwReadAfterEOF();
}


/// Read POD-type in native format
template <typename T>
inline void readPODBinary(T & x, ReadBuffer & buf)
{
    buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x));
}

template <typename T>
inline void readIntBinary(T & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}

template <typename T>
inline void readFloatBinary(T & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}

inline void readStringBinary(std::string & s, ReadBuffer & buf, size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_STRING_SIZE)
        throw Poco::Exception("Too large string size.");

    s.resize(size);
    buf.readStrict(&s[0], size);
}

// Corresponding to `writeString(const char * data, size_t size, WriteBuffer & buf)`.
inline void readString(char * data, size_t size, ReadBuffer & buf)
{
    buf.readStrict(data, size);
}

inline StringRef readStringBinaryInto(Arena & arena, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    char * data = arena.alloc(size);
    buf.readStrict(data, size);

    return StringRef(data, size);
}


template <typename T>
void readVectorBinary(std::vector<T> & v, ReadBuffer & buf, size_t MAX_VECTOR_SIZE = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_VECTOR_SIZE)
        throw Poco::Exception("Too large vector size.");

    v.resize(size);
    for (size_t i = 0; i < size; ++i)
        readBinary(v[i], buf);
}


void assertString(const char * s, ReadBuffer & buf);
void assertEOF(ReadBuffer & buf);
void assertChar(char symbol, ReadBuffer & buf);

inline void assertString(const String & s, ReadBuffer & buf)
{
    assertString(s.c_str(), buf);
}

bool checkString(const char * s, ReadBuffer & buf);
inline bool checkString(const String & s, ReadBuffer & buf)
{
    return checkString(s.c_str(), buf);
}

inline bool checkChar(char c, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != c)
        return false;
    ++buf.position();
    return true;
}

bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline bool checkStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return checkStringCaseInsensitive(s.c_str(), buf);
}

void assertStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline void assertStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return assertStringCaseInsensitive(s.c_str(), buf);
}

/** Check that next character in buf matches first character of s.
  * If true, then check all characters in s and throw exception if it doesn't match.
  * If false, then return false, and leave position in buffer unchanged.
  */
bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf);
bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf);

inline bool checkStringByFirstCharacterAndAssertTheRest(const String & s, ReadBuffer & buf)
{
    return checkStringByFirstCharacterAndAssertTheRest(s.c_str(), buf);
}

inline bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(s.c_str(), buf);
}

inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}

inline void readBoolTextWord(bool & x, ReadBuffer & buf)
{
    if (buf.eof())
        throwReadAfterEOF();

    if (*buf.position() == 't')
    {
        assertString("true", buf);
        x = true;
    }
    else
    {
        assertString("false", buf);
        x = false;
    }
}

inline void decimalRound(Int256 & value, ReadBuffer & buf)
{
    ++buf.position();
    if (buf.eof())
    {
        return;
    }
    if (*buf.position() >= '5' && *buf.position() <= '9')
    {
        value++;
    }
    while (!buf.eof() && *buf.position() >= '0' && *buf.position() <= '9')
    {
        ++buf.position();
    }
}

template <typename T>
inline void readDecimalText(Decimal<T> & x, ReadBuffer & buf, PrecType precision, ScaleType scale)
{
    Int256 value(0); // Int256 is ok for 65 digits number at most.
    bool negative = false;
    bool fractional = false;
    if (buf.eof())
    {
        throwReadAfterEOF();
    }
    size_t cur_scale = 0;
    while (!buf.eof())
    {
        switch (*buf.position())
        {
        case '+':
            break;
        case '-':
            negative = !negative;
            break;
        case '.':
            if (fractional)
            {
                throw Exception("invalid format!");
            }
            fractional = true;
            if (scale == 0)
            {
                decimalRound(value, buf);
                if (negative)
                    value = -value;
                x.value = static_cast<T>(value);
                checkDecimalOverflow(x, precision);
                return;
            }
            break;
        case '0':
            [[fallthrough]];
        case '1':
            [[fallthrough]];
        case '2':
            [[fallthrough]];
        case '3':
            [[fallthrough]];
        case '4':
            [[fallthrough]];
        case '5':
            [[fallthrough]];
        case '6':
            [[fallthrough]];
        case '7':
            [[fallthrough]];
        case '8':
            [[fallthrough]];
        case '9':
            value *= 10;
            value += *buf.position() - '0';
            if (fractional)
            {
                cur_scale++;
                if (scale == cur_scale)
                {
                    decimalRound(value, buf);
                    if (negative)
                        value = -value;
                    x.value = static_cast<T>(value);
                    checkDecimalOverflow(x, precision);
                    return;
                }
            }
            break;
        default:
            for (; cur_scale < scale; cur_scale++)
            {
                value *= 10;
            }
            if (negative)
                value = -value;
            x.value = static_cast<T>(value);
            checkDecimalOverflow(x, precision);
            return;
        }
        ++buf.position();
    }
    for (; cur_scale < scale; cur_scale++)
    {
        value *= 10;
    }
    if (negative)
        value = -value;
    x.value = static_cast<T>(value);
    checkDecimalOverflow(x, precision);
}

template <typename T, typename ReturnType = void>
ReturnType readIntTextImpl(T & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    x = 0;
    if (buf.eof())
    {
        if (throw_exception)
            throwReadAfterEOF();
        else
            return ReturnType(false);
    }

    while (!buf.eof())
    {
        switch (*buf.position())
        {
        case '+':
            break;
        case '-':
            if (std::is_signed_v<T>)
                negative = true;
            else
            {
                if (throw_exception)
                    throw Exception("Unsigned type must not contain '-' symbol", ErrorCodes::CANNOT_PARSE_NUMBER);
                else
                    return ReturnType(false);
            }
            break;
        case '0':
            [[fallthrough]];
        case '1':
            [[fallthrough]];
        case '2':
            [[fallthrough]];
        case '3':
            [[fallthrough]];
        case '4':
            [[fallthrough]];
        case '5':
            [[fallthrough]];
        case '6':
            [[fallthrough]];
        case '7':
            [[fallthrough]];
        case '8':
            [[fallthrough]];
        case '9':
            x *= 10;
            x += *buf.position() - '0';
            break;
        default:
            if (negative)
                x = -x;
            return ReturnType(true);
        }
        ++buf.position();
    }

    /// NOTE Signed integer overflow is undefined behaviour. Consider we have '128' that is parsed as Int8 and overflowed.
    /// We are happy if it is overflowed to -128 and then 'x = -x' does nothing. But UBSan will warn.
    if (negative)
        x = -x;

    return ReturnType(true);
}

template <typename T>
void readIntText(T & x, ReadBuffer & buf)
{
    readIntTextImpl<T, void>(x, buf);
}

template <typename T>
bool tryReadIntText(T & x, ReadBuffer & buf)
{
    return readIntTextImpl<T, bool>(x, buf);
}

/** More efficient variant (about 1.5 times on real dataset).
  * Differs in following:
  * - for numbers starting with zero, parsed only zero;
  * - symbol '+' before number is not supported;
  * - symbols :;<=>? are parsed as some numbers.
  */
template <typename T, bool throw_on_error = true>
void readIntTextUnsafe(T & x, ReadBuffer & buf)
{
    bool negative = false;
    x = 0;

    auto on_error = [] {
        if (throw_on_error)
            throwReadAfterEOF();
    };

    if (unlikely(buf.eof()))
        return on_error();

    if (std::is_signed_v<T> && *buf.position() == '-')
    {
        ++buf.position();
        negative = true;
        if (unlikely(buf.eof()))
            return on_error();
    }

    if (*buf.position() == '0') /// There are many zeros in real datasets.
    {
        ++buf.position();
        return;
    }

    while (!buf.eof())
    {
        /// This check is suddenly faster than
        ///  unsigned char c = *buf.position() - '0';
        ///  if (c < 10)
        /// for unknown reason on Xeon E5645.

        if ((*buf.position() & 0xF0) == 0x30) /// It makes sense to have this condition inside loop.
        {
            x *= 10;
            x += *buf.position() & 0x0F;
            ++buf.position();
        }
        else
            break;
    }

    /// See note about undefined behaviour above.
    if (std::is_signed_v<T> && negative)
        x = -x;
}

template <typename T>
void tryReadIntTextUnsafe(T & x, ReadBuffer & buf)
{
    return readIntTextUnsafe<T, false>(x, buf);
}


/// Look at readFloatText.h
template <typename T>
void readFloatText(T & x, ReadBuffer & in);
template <typename T>
bool tryReadFloatText(T & x, ReadBuffer & in);


/// simple: all until '\n' or '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);
void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);
void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readJSONString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);
void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readStringUntilEOF(String & s, ReadBuffer & buf);


/** Read string in CSV format.
  * Parsing rules:
  * - string could be placed in quotes; quotes could be single: ' or double: ";
  * - or string could be unquoted - this is determined by first character;
  * - if string is unquoted, then it is read until next delimiter,
  *   either until end of line (CR or LF),
  *   or until end of stream;
  *   but spaces and tabs at begin and end of unquoted string are consumed but ignored (note that this behaviour differs from RFC).
  * - if string is in quotes, then it will be read until closing quote,
  *   but sequences of two consecutive quotes are parsed as single quote inside string;
  */
void readCSVString(String & s, ReadBuffer & buf, const char delimiter = ',');


/// Read and append result to array of characters.
template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const char delimiter = ',');

/// ReturnType is either bool or void. If bool, the function will return false instead of throwing an exception.
template <typename Vector, typename ReturnType = void>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
bool tryReadJSONStringInto(Vector & s, ReadBuffer & buf)
{
    return readJSONStringInto<Vector, bool>(s, buf);
}

/// This could be used as template parameter for functions above, if you want to just skip data.
struct NullSink
{
    void append(const char *, size_t){};
    void push_back(char){};
};

void parseUUID(const UInt8 * src36, UInt8 * dst16);
void parseUUID(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16);

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes);

template <typename ReturnType = void>
ReturnType readMyDateTextImpl(UInt64 & date, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// Optimistic path, when whole value is in buffer.
    if (buf.position() + 10 <= buf.buffer().end())
    {
        UInt16 year = (buf.position()[0] - '0') * 1000 + (buf.position()[1] - '0') * 100
            + (buf.position()[2] - '0') * 10 + (buf.position()[3] - '0');
        buf.position() += 5;

        UInt8 month = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            month = month * 10 + buf.position()[1] - '0';
            buf.position() += 3;
        }
        else
            buf.position() += 2;

        UInt8 day = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            day = day * 10 + buf.position()[1] - '0';
            buf.position() += 2;
        }
        else
            buf.position() += 1;

        date = MyDate(year, month, day).toPackedUInt();
        return ReturnType(true);
    }

    if constexpr (throw_exception)
        throw Exception("wrong date format.", ErrorCodes::CANNOT_PARSE_DATE);
    else
        return ReturnType(false);
}

inline void readMyDateText(UInt64 & date, ReadBuffer & buf)
{
    readMyDateTextImpl<void>(date, buf);
}

inline bool tryReadMyDateText(UInt64 & x, ReadBuffer & buf)
{
    UInt64 tmp(0);
    bool ret = readMyDateTextImpl<bool>(tmp, buf);
    if (ret)
        x = tmp;
    return ret;
}


void readDateTextFallback(LocalDate & date, ReadBuffer & buf);

/// In YYYY-MM-DD format.
/// For convenience, Month and Day parts can have single digit instead of two digits.
/// Any separators other than '-' are supported.
inline void readDateText(LocalDate & date, ReadBuffer & buf)
{
    /// Optimistic path, when whole value is in buffer.
    if (buf.position() + 10 <= buf.buffer().end())
    {
        UInt16 year = (buf.position()[0] - '0') * 1000 + (buf.position()[1] - '0') * 100
            + (buf.position()[2] - '0') * 10 + (buf.position()[3] - '0');
        buf.position() += 5;

        UInt8 month = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            month = month * 10 + buf.position()[1] - '0';
            buf.position() += 3;
        }
        else
            buf.position() += 2;

        UInt8 day = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            day = day * 10 + buf.position()[1] - '0';
            buf.position() += 2;
        }
        else
            buf.position() += 1;

        date = LocalDate(year, month, day);
    }
    else
        readDateTextFallback(date, buf);
}

inline void readDateText(DayNum & date, ReadBuffer & buf)
{
    LocalDate local_date;
    readDateText(local_date, buf);
    date = DateLUT::instance().makeDayNum(local_date.year(), local_date.month(), local_date.day());
}


inline void readUUIDText(UUID & uuid, ReadBuffer & buf)
{
    char s[36];
    size_t size = buf.read(s, 36);

    if (size != 36)
    {
        s[size] = 0;
        throw Exception(std::string("Cannot parse uuid ") + s, ErrorCodes::CANNOT_PARSE_UUID);
    }

    parseUUID(
        reinterpret_cast<const UInt8 *>(s),
        std::reverse_iterator<UInt8 *>(reinterpret_cast<UInt8 *>(&uuid) + 16));
}


template <typename T>
inline T parse(const char * data, size_t size);


void readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut);

template <typename ReturnType = void>
ReturnType readMyDateTimeTextImpl(UInt64 & packed, int fsp, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const char * s = buf.position();
    if (s + 19 <= buf.buffer().end())
    {
        if (s[4] < '0' || s[4] > '9')
        {
            UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
            UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
            UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

            UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
            UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
            UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

            UInt32 micro_second = 0;
            bool fractional = false;
            int digit = 0;
            buf.position() += 19;
            while (buf.position() <= buf.buffer().end())
            {
                char x = *buf.position();
                if (x == '.')
                {
                    fractional = true;
                }
                else if (!fractional)
                {
                    break;
                }
                else if (x <= '9' && x >= '0')
                {
                    if (digit < fsp)
                    {
                        micro_second = micro_second * 10 + (x - '0');
                        digit++;
                    }
                }
                else
                {
                    break;
                }
                buf.position()++;
            }
            for (; digit < 6; digit++)
                micro_second *= 10;

            packed = MyDateTime(year, month, day, hour, minute, second, micro_second).toPackedUInt();
            return ReturnType(true);
        }
    }
    else if (s + 10 <= buf.buffer().end())
    {
        // try to parse it as MyDate
        return readMyDateTextImpl<ReturnType>(packed, buf);
    }

    if constexpr (throw_exception)
        throw Exception("wrong datetime format.", ErrorCodes::CANNOT_PARSE_DATETIME);
    else
        return ReturnType(false);
}

inline void readMyDateTimeText(UInt64 & packed, int fsp, ReadBuffer & buf)
{
    readMyDateTimeTextImpl<void>(packed, fsp, buf);
}

inline bool tryReadMyDateTimeText(UInt64 & x, int fsp, ReadBuffer & buf)
{
    UInt64 tmp(0);
    bool ret = readMyDateTimeTextImpl<bool>(tmp, fsp, buf);
    if (ret)
        x = tmp;
    return ret;
}
/** In YYYY-MM-DD hh:mm:ss format, according to specified time zone.
  * As an exception, also supported parsing of unix timestamp in form of decimal number.
  */
inline void readDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    /** Read 10 characters, that could represent unix timestamp.
      * Only unix timestamp of 5-10 characters is supported.
      * Then look at 5th charater. If it is a number - treat whole as unix timestamp.
      * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss format.
      */

    /// Optimistic path, when whole value is in buffer.
    const char * s = buf.position();
    if (s + 19 <= buf.buffer().end())
    {
        if (s[4] < '0' || s[4] > '9')
        {
            UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
            UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
            UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

            UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
            UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
            UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

            if (unlikely(year == 0))
                datetime = 0;
            else
                datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);

            buf.position() += 19;
        }
        else
            /// Why not readIntTextUnsafe? Because for needs of AdFox, parsing of unix timestamp with leading zeros is supported: 000...NNNN.
            readIntText(datetime, buf);
    }
    else
        readDateTimeTextFallback(datetime, buf, date_lut);
}

inline void readDateTimeText(time_t & datetime, ReadBuffer & buf)
{
    readDateTimeText(datetime, buf, DateLUT::instance());
}

inline void readDateTimeText(LocalDateTime & datetime, ReadBuffer & buf)
{
    char s[19];
    size_t size = buf.read(s, 19);
    if (19 != size)
    {
        s[size] = 0;
        throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
    }

    datetime.year((s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0'));
    datetime.month((s[5] - '0') * 10 + (s[6] - '0'));
    datetime.day((s[8] - '0') * 10 + (s[9] - '0'));

    datetime.hour((s[11] - '0') * 10 + (s[12] - '0'));
    datetime.minute((s[14] - '0') * 10 + (s[15] - '0'));
    datetime.second((s[17] - '0') * 10 + (s[18] - '0'));
}


/// Generic methods to read value in native binary format.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void> readBinary(T & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}

inline void readBinary(String & x, ReadBuffer & buf)
{
    readStringBinary(x, buf);
}
inline void readBinary(UInt128 & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}
inline void readBinary(UInt256 & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}
inline void readBinary(LocalDate & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}
inline void readBinary(LocalDateTime & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}
template <typename T>
inline void readBinary(Decimal<T> & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}


/// Generic methods to read value in text tab-separated format.
template <typename T>
inline std::enable_if_t<std::is_integral_v<T>, void> readText(T & x, ReadBuffer & buf)
{
    readIntText(x, buf);
}

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, void> readText(T & x, ReadBuffer & buf)
{
    readFloatText(x, buf);
}

inline void readText(bool & x, ReadBuffer & buf)
{
    readBoolText(x, buf);
}
inline void readText(String & x, ReadBuffer & buf)
{
    readEscapedString(x, buf);
}
inline void readText(LocalDate & x, ReadBuffer & buf)
{
    readDateText(x, buf);
}
inline void readText(LocalDateTime & x, ReadBuffer & buf)
{
    readDateTimeText(x, buf);
}
inline void readText(UUID & x, ReadBuffer & buf)
{
    readUUIDText(x, buf);
}
inline void readText(UInt128 &, ReadBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be read as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// Generic methods to read value in text format,
///  possibly in single quotes (only for data types that use quotes in VALUES format of INSERT statement in SQL).
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void> readQuoted(T & x, ReadBuffer & buf)
{
    readText(x, buf);
}

inline void readQuoted(String & x, ReadBuffer & buf)
{
    readQuotedString(x, buf);
}

inline void readQuoted(LocalDate & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readDateText(x, buf);
    assertChar('\'', buf);
}

inline void readQuoted(LocalDateTime & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readDateTimeText(x, buf);
    assertChar('\'', buf);
}


/// Same as above, but in double quotes.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void> readDoubleQuoted(T & x, ReadBuffer & buf)
{
    readText(x, buf);
}

inline void readDoubleQuoted(String & x, ReadBuffer & buf)
{
    readDoubleQuotedString(x, buf);
}

inline void readDoubleQuoted(LocalDate & x, ReadBuffer & buf)
{
    assertChar('"', buf);
    readDateText(x, buf);
    assertChar('"', buf);
}

inline void readDoubleQuoted(LocalDateTime & x, ReadBuffer & buf)
{
    assertChar('"', buf);
    readDateTimeText(x, buf);
    assertChar('"', buf);
}


/// CSV, for numbers, dates: quotes are optional, no special escaping rules.
template <typename T>
inline void readCSVSimple(T & x, ReadBuffer & buf)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readText(x, buf);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

template <typename T>
inline void readCSVDecimal(Decimal<T> & x, ReadBuffer & buf, PrecType precision, ScaleType scale)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readDecimalText(x, buf, precision, scale);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

inline void readMyDateTimeCSV(UInt64 & datetime, int fsp, ReadBuffer & buf)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readMyDateTimeText(datetime, fsp, buf);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

inline void readDateTimeCSV(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readDateTimeText(datetime, buf, date_lut);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void> readCSV(T & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}

inline void readCSV(String & x, ReadBuffer & buf, const char delimiter = ',')
{
    readCSVString(x, buf, delimiter);
}
inline void readCSV(LocalDate & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}
inline void readCSV(LocalDateTime & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}
inline void readCSV(UUID & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}
inline void readCSV(UInt128 &, ReadBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be read as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <typename T>
void readBinary(std::vector<T> & x, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Poco::Exception("Too large vector size.");

    x.resize(size);
    for (size_t i = 0; i < size; ++i)
        readBinary(x[i], buf);
}

template <typename T>
void readQuoted(std::vector<T> & x, ReadBuffer & buf)
{
    bool first = true;
    assertChar('[', buf);
    while (!buf.eof() && *buf.position() != ']')
    {
        if (!first)
        {
            if (*buf.position() == ',')
                ++buf.position();
            else
                throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
        }

        first = false;

        x.push_back(T());
        readQuoted(x.back(), buf);
    }
    assertChar(']', buf);
}

template <typename T>
void readDoubleQuoted(std::vector<T> & x, ReadBuffer & buf)
{
    bool first = true;
    assertChar('[', buf);
    while (!buf.eof() && *buf.position() != ']')
    {
        if (!first)
        {
            if (*buf.position() == ',')
                ++buf.position();
            else
                throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
        }

        first = false;

        x.push_back(T());
        readDoubleQuoted(x.back(), buf);
    }
    assertChar(']', buf);
}

template <typename T>
void readText(std::vector<T> & x, ReadBuffer & buf)
{
    readQuoted(x, buf);
}


/// Skip whitespace characters.
inline void skipWhitespaceIfAny(ReadBuffer & buf)
{
    while (!buf.eof() && isWhitespaceASCII(*buf.position()))
        ++buf.position();
}

/// Skips json value. If the value contains objects (i.e. {...} sequence), an exception will be thrown.
void skipJSONFieldPlain(ReadBuffer & buf, const StringRef & name_of_filed);


/** Read serialized exception.
  * During serialization/deserialization some information is lost
  * (type is cut to base class, 'message' replaced by 'displayText', and stack trace is appended to 'message')
  * Some additional message could be appended to exception (example: you could add information about from where it was received).
  */
void readException(Exception & e, ReadBuffer & buf, const String & additional_message = "");
void readAndThrowException(ReadBuffer & buf, const String & additional_message = "");


/** Helper function for implementation.
  */
template <typename T>
static inline const char * tryReadIntText(T & x, const char * pos, const char * end)
{
    ReadBufferFromMemory in(pos, end - pos);
    tryReadIntText(x, in);
    return pos + in.count();
}


/// Convenient methods for reading something from string in text format.
template <typename T>
inline T parse(const char * data, size_t size)
{
    T res;
    ReadBufferFromMemory buf(data, size);
    readText(res, buf);
    return res;
}

template <typename T>
inline T parse(const char * data)
{
    return parse<T>(data, strlen(data));
}

template <typename T>
inline T parse(const String & s)
{
    return parse<T>(s.data(), s.size());
}


/** Skip UTF-8 BOM if it is under cursor.
  * As BOM is usually located at start of stream, and buffer size is usually larger than three bytes,
  *  the function expects, that all three bytes of BOM is fully in buffer (otherwise it don't skip anything).
  */
inline void skipBOMIfExists(ReadBuffer & buf)
{
    if (!buf.eof() && buf.position() + 3 < buf.buffer().end() && buf.position()[0] == '\xEF'
        && buf.position()[1] == '\xBB' && buf.position()[2] == '\xBF')
    {
        buf.position() += 3;
    }
}


/// Skip to next character after next \n. If no \n in stream, skip to end.
void skipToNextLineOrEOF(ReadBuffer & buf);

/// Skip to next character after next unescaped \n. If no \n in stream, skip to end. Does not throw on invalid escape sequences.
void skipToUnescapedNextLineOrEOF(ReadBuffer & buf);

} // namespace DB
