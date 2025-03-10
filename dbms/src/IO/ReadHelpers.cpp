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

#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Core/Defines.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/readFloatText.h>
#include <common/find_symbols.h>
#include <stdlib.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
extern const int CANNOT_PARSE_QUOTED_STRING;
extern const int INCORRECT_DATA;
} // namespace ErrorCodes

template <typename IteratorSrc, typename IteratorDst>
void parseHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; dst_pos < num_bytes; ++dst_pos)
    {
        dst[dst_pos] = static_cast<UInt8>(unhex(src[src_pos])) * 16 + static_cast<UInt8>(unhex(src[src_pos + 1]));
        src_pos += 2;
    }
}

void parseUUID(const UInt8 * src36, UInt8 * dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    parseHex(&src36[0], &dst16[0], 4);
    parseHex(&src36[9], &dst16[4], 2);
    parseHex(&src36[14], &dst16[6], 2);
    parseHex(&src36[19], &dst16[8], 2);
    parseHex(&src36[24], &dst16[10], 6);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void parseUUID(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    /// FIXME This code looks like trash.
    parseHex(&src36[0], dst16 + 8, 4);
    parseHex(&src36[9], dst16 + 12, 2);
    parseHex(&src36[14], dst16 + 14, 2);
    parseHex(&src36[19], dst16, 2);
    parseHex(&src36[24], dst16 + 2, 6);
}

static void __attribute__((__noinline__)) throwAtAssertionFailed(const char * s, ReadBuffer & buf)
{
    WriteBufferFromOwnString out;
    out << "Cannot parse input: expected " << escape << s;

    if (buf.eof())
        out << " at end of stream.";
    else
        out << " before: " << escape
            << String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position()));

    throw Exception(out.str(), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}


bool checkString(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof() || *buf.position() != *s)
            return false;
        ++buf.position();
    }
    return true;
}


bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof())
            return false;

        char c = *buf.position();
        if (!equalsCaseInsensitive(*s, c))
            return false;

        ++buf.position();
    }
    return true;
}


void assertString(const char * s, ReadBuffer & buf)
{
    if (!checkString(s, buf))
        throwAtAssertionFailed(s, buf);
}

void assertChar(char symbol, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != symbol)
    {
        char err[2] = {symbol, '\0'};
        throwAtAssertionFailed(err, buf);
    }
    ++buf.position();
}

void assertEOF(ReadBuffer & buf)
{
    if (!buf.eof())
        throwAtAssertionFailed("eof", buf);
}


void assertStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (!checkStringCaseInsensitive(s, buf))
        throwAtAssertionFailed(s, buf);
}


bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != *s)
        return false;

    assertString(s, buf);
    return true;
}

bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (buf.eof())
        return false;

    char c = *buf.position();
    if (!equalsCaseInsensitive(*s, c))
        return false;

    assertStringCaseInsensitive(s, buf);
    return true;
}


template <typename T>
static void appendToStringOrVector(T & s, const char * begin, const char * end)
{
    s.append(begin, end - begin);
}

template <>
inline void appendToStringOrVector(PaddedPODArray<UInt8> & s, const char * begin, const char * end)
{
    s.insert(begin, end); /// TODO memcpySmall
}


template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\t', '\n'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf.position(), next_pos);
        buf.position() += next_pos
            - buf.position(); /// Code looks complicated, because "buf.position() = next_pos" doens't work due to const-ness.

        if (buf.hasPendingData())
            return;
    }
}

void readString(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringInto(s, buf);
}

template void readStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        size_t bytes = buf.buffer().end() - buf.position();

        appendToStringOrVector(s, buf.position(), buf.position() + bytes);
        buf.position() += bytes;

        if (buf.hasPendingData())
            return;
    }
}

void readStringUntilEOF(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilEOFInto(s, buf);
}

template void readStringUntilEOFInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


/** Parse the escape sequence, which can be simple (one character after backslash) or more complex (multiple characters).
  * It is assumed that the cursor is located on the `\` symbol
  */
template <typename Vector>
static void parseComplexEscapeSequence(Vector & s, ReadBuffer & buf)
{
    ++buf.position();
    if (buf.eof())
        throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

    if (*buf.position() == 'x')
    {
        ++buf.position();
        /// escape sequence of the form \xAA
        char hex_code[2];
        readPODBinary(hex_code, buf);
        s.push_back(unhex2(hex_code));
    }
    else if (*buf.position() == 'N')
    {
        /// Support for NULLs: \N sequence must be parsed as empty string.
        ++buf.position();
    }
    else
    {
        /// The usual escape sequence of a single character.
        s.push_back(parseEscapeSequence(*buf.position()));
        ++buf.position();
    }
}


template <typename Vector, typename ReturnType>
static ReturnType parseJSONEscapeSequence(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message, int code) {
        if (throw_exception)
            throw Exception(message, code);
        return static_cast<ReturnType>(false);
    };

    ++buf.position();
    if (buf.eof())
        return error("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

    switch (*buf.position())
    {
    case '"':
        s.push_back('"');
        break;
    case '\\':
        s.push_back('\\');
        break;
    case '/':
        s.push_back('/');
        break;
    case 'b':
        s.push_back('\b');
        break;
    case 'f':
        s.push_back('\f');
        break;
    case 'n':
        s.push_back('\n');
        break;
    case 'r':
        s.push_back('\r');
        break;
    case 't':
        s.push_back('\t');
        break;
    case 'u':
    {
        ++buf.position();

        char hex_code[4];
        if (4 != buf.read(hex_code, 4))
            return error(
                "Cannot parse escape sequence: less than four bytes after \\u",
                ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

        /// \u0000 - special case
        if (0 == memcmp(hex_code, "0000", 4))
        {
            s.push_back(0);
            return static_cast<ReturnType>(true);
        }

        UInt16 code_point = unhex4(hex_code);

        if (code_point <= 0x7F)
        {
            s.push_back(code_point);
        }
        else if (code_point <= 0x07FF)
        {
            s.push_back(((code_point >> 6) & 0x1F) | 0xC0);
            s.push_back((code_point & 0x3F) | 0x80);
        }
        else
        {
            /// Surrogate pair.
            if (code_point >= 0xD800 && code_point <= 0xDBFF)
            {
                if (!checkString("\\u", buf))
                    return error(
                        "Cannot parse escape sequence: missing second part of surrogate pair",
                        ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

                char second_hex_code[4];
                if (4 != buf.read(second_hex_code, 4))
                    return error(
                        "Cannot parse escape sequence: less than four bytes after \\u of second part of surrogate pair",
                        ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

                UInt16 second_code_point = unhex4(second_hex_code);

                if (second_code_point >= 0xDC00 && second_code_point <= 0xDFFF)
                {
                    UInt32 full_code_point = 0x10000 + (code_point - 0xD800) * 1024 + (second_code_point - 0xDC00);

                    s.push_back(((full_code_point >> 18) & 0x07) | 0xF0);
                    s.push_back(((full_code_point >> 12) & 0x3F) | 0x80);
                    s.push_back(((full_code_point >> 6) & 0x3F) | 0x80);
                    s.push_back((full_code_point & 0x3F) | 0x80);
                }
                else
                    return error(
                        "Incorrect surrogate pair of unicode escape sequences in JSON",
                        ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
            }
            else
            {
                s.push_back(((code_point >> 12) & 0x0F) | 0xE0);
                s.push_back(((code_point >> 6) & 0x3F) | 0x80);
                s.push_back((code_point & 0x3F) | 0x80);
            }
        }

        return static_cast<ReturnType>(true);
    }
    default:
        s.push_back(*buf.position());
        break;
    }

    ++buf.position();
    return static_cast<ReturnType>(true);
}


template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\t', '\n', '\\'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf.position(), next_pos);
        buf.position() += next_pos
            - buf.position(); /// Code looks complicated, because "buf.position() = next_pos" doens't work due to const-ness.

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }
}

void readEscapedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringInto(s, buf);
}

template void readEscapedStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readEscapedStringInto<NullSink>(NullSink & s, ReadBuffer & buf);


/** If enable_sql_style_quoting == true,
  *  strings like 'abc''def' will be parsed as abc'def.
  * Please note, that even with SQL style quoting enabled,
  *  backslash escape sequences are also parsed,
  *  that could be slightly confusing.
  */
template <char quote, bool enable_sql_style_quoting, typename Vector>
static void readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != quote)
        throw Exception("Cannot parse quoted string: expected opening quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
    ++buf.position();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\\', quote>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf.position(), next_pos);
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == quote)
        {
            ++buf.position();

            if (enable_sql_style_quoting && !buf.eof() && *buf.position() == quote)
            {
                s.push_back(quote);
                ++buf.position();
                continue;
            }

            return;
        }

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }

    throw Exception("Cannot parse quoted string: expected closing quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'\'', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'"', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'`', enable_sql_style_quoting>(s, buf);
}


void readQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<false>(s, buf);
}

void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<true>(s, buf);
}


template void readQuotedStringInto<true>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readDoubleQuotedStringInto<false>(NullSink & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<false>(s, buf);
}

void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<true>(s, buf);
}

void readBackQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<false>(s, buf);
}

void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<true>(s, buf);
}

template <typename Vector, typename ReturnType>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message, int code) {
        if (throw_exception)
            throw Exception(message, code);
        return static_cast<ReturnType>(false);
    };

    if (buf.eof() || *buf.position() != '"')
        return error("Cannot parse JSON string: expected opening quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
    ++buf.position();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\\', '"'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf.position(), next_pos);
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '"')
        {
            ++buf.position();
            return static_cast<ReturnType>(true);
        }

        if (*buf.position() == '\\')
            parseJSONEscapeSequence<Vector, ReturnType>(s, buf);
    }

    return error("Cannot parse JSON string: expected closing quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

void readJSONString(String & s, ReadBuffer & buf)
{
    s.clear();
    readJSONStringInto(s, buf);
}

template void readJSONStringInto<PaddedPODArray<UInt8>, void>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template bool readJSONStringInto<PaddedPODArray<UInt8>, bool>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readJSONStringInto<NullSink>(NullSink & s, ReadBuffer & buf);


void readDateTextFallback(LocalDate & date, ReadBuffer & buf)
{
    char chars_year[4];
    readPODBinary(chars_year, buf);
    UInt16 year = (chars_year[0] - '0') * 1000 + (chars_year[1] - '0') * 100 + (chars_year[2] - '0') * 10
        + (chars_year[3] - '0');

    buf.ignore();

    char chars_month[2];
    readPODBinary(chars_month, buf);
    UInt8 month = chars_month[0] - '0';
    if (isNumericASCII(chars_month[1]))
    {
        month = month * 10 + chars_month[1] - '0';
        buf.ignore();
    }

    char char_day;
    readChar(char_day, buf);
    UInt8 day = char_day - '0';
    if (!buf.eof() && isNumericASCII(*buf.position()))
    {
        day = day * 10 + *buf.position() - '0';
        ++buf.position();
    }

    date = LocalDate(year, month, day);
}


void readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    static constexpr auto DATE_TIME_BROKEN_DOWN_LENGTH = 19;
    static constexpr auto UNIX_TIMESTAMP_MAX_LENGTH = 10;

    char s[DATE_TIME_BROKEN_DOWN_LENGTH];
    char * s_pos = s;

    /// A piece similar to unix timestamp.
    while (s_pos < s + UNIX_TIMESTAMP_MAX_LENGTH && !buf.eof() && isNumericASCII(*buf.position()))
    {
        *s_pos = *buf.position();
        ++s_pos;
        ++buf.position();
    }

    /// 2015-01-01 01:02:03
    if (s_pos == s + 4 && !buf.eof() && (*buf.position() < '0' || *buf.position() > '9'))
    {
        const size_t remaining_size = DATE_TIME_BROKEN_DOWN_LENGTH - (s_pos - s);
        size_t size = buf.read(s_pos, remaining_size);
        if (remaining_size != size)
        {
            s_pos[size] = 0;
            throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
        }

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
    }
    else
        datetime = parse<time_t>(s, s_pos - s);
}


void skipJSONFieldPlain(ReadBuffer & buf, const StringRef & name_of_filed)
{
    if (buf.eof())
        throw Exception("Unexpected EOF for key '" + name_of_filed.toString() + "'", ErrorCodes::INCORRECT_DATA);
    else if (*buf.position() == '"') /// skip double-quoted string
    {
        NullSink sink;
        readJSONStringInto(sink, buf);
    }
    else if (
        isNumericASCII(*buf.position()) || *buf.position() == '-' || *buf.position() == '+'
        || *buf.position() == '.') /// skip number
    {
        if (*buf.position() == '+')
            ++buf.position();

        double v;
        if (!tryReadFloatText(v, buf))
            throw Exception(
                "Expected a number field for key '" + name_of_filed.toString() + "'",
                ErrorCodes::INCORRECT_DATA);
    }
    else if (*buf.position() == 'n') /// skip null
    {
        assertString("null", buf);
    }
    else if (*buf.position() == 't') /// skip true
    {
        assertString("true", buf);
    }
    else if (*buf.position() == 'f') /// skip false
    {
        assertString("false", buf);
    }
    else if (*buf.position() == '[')
    {
        ++buf.position();
        skipWhitespaceIfAny(buf);

        if (!buf.eof() && *buf.position() == ']') /// skip empty array
        {
            ++buf.position();
            return;
        }

        while (true)
        {
            skipJSONFieldPlain(buf, name_of_filed);
            skipWhitespaceIfAny(buf);

            if (!buf.eof() && *buf.position() == ',')
            {
                ++buf.position();
                skipWhitespaceIfAny(buf);
            }
            else if (!buf.eof() && *buf.position() == ']')
            {
                ++buf.position();
                break;
            }
            else
                throw Exception(
                    "Unexpected symbol for key '" + name_of_filed.toString() + "'",
                    ErrorCodes::INCORRECT_DATA);
        }
    }
    else if (*buf.position() == '{') /// fail on objects
    {
        throw Exception(
            "Unexpected nested field for key '" + name_of_filed.toString() + "'",
            ErrorCodes::INCORRECT_DATA);
    }
    else
    {
        throw Exception(
            "Unexpected symbol '" + std::string(*buf.position(), 1) + "' for key '" + name_of_filed.toString() + "'",
            ErrorCodes::INCORRECT_DATA);
    }
}


void readException(Exception & e, ReadBuffer & buf, const String & additional_message)
{
    int code = 0;
    String name;
    String message;
    String stack_trace;
    bool has_nested = false;

    readBinary(code, buf);
    readBinary(name, buf);
    readBinary(message, buf);
    readBinary(stack_trace, buf);
    readBinary(has_nested, buf);

    WriteBufferFromOwnString out;

    if (!additional_message.empty())
        out << additional_message << ". ";

    if (name != "DB::Exception")
        out << name << ". ";

    out << message << ". Stack trace:\n\n" << stack_trace;

    if (has_nested)
    {
        Exception nested;
        readException(nested, buf);
        e = Exception(out.str(), nested, code);
    }
    else
        e = Exception(out.str(), code);
}

void readAndThrowException(ReadBuffer & buf, const String & additional_message)
{
    Exception e;
    readException(e, buf, additional_message);
    e.rethrow();
}


void skipToNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


void skipToUnescapedNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }

        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (buf.eof())
                return;

            /// Skip escaped character. We do not consider escape sequences with more than one charater after backslash (\x01).
            /// It's ok for the purpose of this function, because we are interested only in \n and \\.
            ++buf.position();
            continue;
        }
    }
}

} // namespace DB
