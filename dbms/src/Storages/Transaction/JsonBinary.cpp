// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/JsonBinary.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Common/utf8Utility.h>

#include <cmath>

#include "Common/MyDuration.h"
#include "Flash/Coprocessor/TiDBTime.h"
#include "Poco/Base64Decoder.h"
#include "Poco/Base64Encoder.h"
#include "Poco/MemoryStream.h"
#include "Poco/StreamCopier.h"
#pragma GCC diagnostic pop

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using JsonVar = Poco::Dynamic::Var;

constexpr size_t HEADER_SIZE = 8; // element size + data size.
constexpr size_t VALUE_ENTRY_SIZE = 5;
constexpr size_t VALUE_TYPE_SIZE = 1;
constexpr size_t KEY_ENTRY_SIZE = 6; // keyOff +  keyLen
constexpr size_t DATA_SIZE_OFFSET = 4;
//constexpr size_t KEY_LENGTH_OFFSET = 4;

template <typename T, typename S>
inline T decodeNumeric(size_t & cursor, const S & raw_value)
{
    T res = *(reinterpret_cast<const T *>(raw_value.data() + cursor));
    toLittleEndianInPlace(res);
    cursor += sizeof(T);
    return res;
}

template <typename T>
inline T decodeNumeric(size_t & cursor, const StringRef & raw_value)
{
    T res = *(reinterpret_cast<const T *>(raw_value.data + cursor));
    toLittleEndianInPlace(res);
    cursor += sizeof(T);
    return res;
}

UInt32 JsonBinary::getElementCount() const
{
    size_t cursor = 0;
    return decodeNumeric<UInt32>(cursor, data);
}

JsonBinary JsonBinary::getArrayElement(size_t index) const
{
    return getValueEntry(HEADER_SIZE + index * VALUE_ENTRY_SIZE);
}

StringRef JsonBinary::getObjectKey(size_t index) const
{
    size_t cursor = HEADER_SIZE + index * KEY_ENTRY_SIZE;
    size_t key_offset = decodeNumeric<UInt32>(cursor, data);
    size_t key_length = decodeNumeric<UInt16>(cursor, data);
    return StringRef(data.data + key_offset, key_length);
}

JsonBinary JsonBinary::getObjectValue(size_t index) const
{
    auto element_count = getElementCount();
    return getValueEntry(HEADER_SIZE + element_count * KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE);
}

JsonBinary JsonBinary::getValueEntry(size_t value_entry_offset) const
{
    JsonType entry_type = data.data[value_entry_offset];
    size_t cursor = value_entry_offset + VALUE_TYPE_SIZE;
    size_t value_offset = decodeNumeric<UInt32>(cursor, data); /// Literal type would padding zeros, thus it wouldn't cross array bound
    switch (entry_type) {
    case JsonBinary::TYPE_CODE_LITERAL:
        return JsonBinary(JsonBinary::TYPE_CODE_LITERAL, StringRef(data.data + (value_entry_offset + VALUE_TYPE_SIZE), 1));
    case JsonBinary::TYPE_CODE_INT64:
    case JsonBinary::TYPE_CODE_UINT64:
    case JsonBinary::TYPE_CODE_FLOAT64:
        return JsonBinary(entry_type, StringRef(data.data + value_offset, 8));
    case JsonBinary::TYPE_CODE_STRING:
    {
        cursor = value_offset;
        auto str_length = DecodeVarUInt(cursor, data);
        auto str_length_length = cursor - value_offset;
        return JsonBinary(entry_type, StringRef(data.data + value_offset, str_length_length + str_length));
    }
    case JsonBinary::TYPE_CODE_OPAQUE:
    {
        cursor = value_offset + 1;
        auto data_length = DecodeVarUInt(cursor, data);
        auto data_length_length = cursor - value_offset - 1;
        return JsonBinary(entry_type, StringRef(data.data + value_offset, data_length_length + data_length + 1));
    }
    case JsonBinary::TYPE_CODE_DATE:
    case JsonBinary::TYPE_CODE_DATETIME:
    case JsonBinary::TYPE_CODE_TIMESTAMP:
        return JsonBinary(entry_type, StringRef(data.data + value_offset, 8));
    case JsonBinary::TYPE_CODE_DURATION:
        return JsonBinary(entry_type, StringRef(data.data + value_offset, 12));
    }
    cursor = value_offset + DATA_SIZE_OFFSET;
    auto data_size = decodeNumeric<size_t>(cursor, data);
    return JsonBinary(entry_type, StringRef(data.data + value_offset, data_size));
}

Int64 JsonBinary::getInt64() const
{
    size_t cursor = 0;
    return decodeNumeric<Int64>(cursor, data);
}

UInt64 JsonBinary::getUInt64() const
{
    size_t cursor = 0;
    return decodeNumeric<UInt64>(cursor, data);
}

double JsonBinary::getFloat64() const
{
    size_t cursor = 0;
    return decodeNumeric<double>(cursor, data);
}

StringRef JsonBinary::getString() const
{
    size_t cursor = 0;
    size_t str_length = DecodeVarUInt(cursor, data);
    size_t str_length_length = cursor;
    return StringRef(data.data + str_length_length, str_length);
}

JsonBinary::Opaque JsonBinary::getOpaque() const
{
    auto opaque_type = static_cast<UInt8>(data.data[0]);
    size_t cursor = 1;
    size_t data_length = DecodeVarUInt(cursor, data);
    size_t data_start = cursor;
    return Opaque{ opaque_type, StringRef(data.data + data_start, data_length) };
}

template <bool doDecode>
struct need_decode
{
};

template <>
struct need_decode<true>
{
    typedef String type;
};

template <>
struct need_decode<false>
{
    typedef void type;
};

template <bool doDecode>
typename need_decode<doDecode>::type DecodeJson(size_t & cursor, const String & raw_value)
{
    size_t base = cursor;
    UInt8 type = raw_value[cursor++];
    size_t size = 0;

    switch (type) // JSON Root element type
    {
    case JsonBinary::TYPE_CODE_OBJECT:
        cursor += 4;
        size = decodeNumeric<UInt32>(cursor, raw_value);
        break;
    case JsonBinary::TYPE_CODE_ARRAY:
        cursor += 4;
        size = decodeNumeric<UInt32>(cursor, raw_value);
        break;
    case JsonBinary::TYPE_CODE_LITERAL:
        size = 1;
        break;
    case JsonBinary::TYPE_CODE_INT64:
    case JsonBinary::TYPE_CODE_UINT64:
    case JsonBinary::TYPE_CODE_FLOAT64:
        size = 8;
        break;
    case JsonBinary::TYPE_CODE_STRING:
        size = DecodeVarUInt(cursor, raw_value);
        size += (cursor - base - 1);
        break;
    default:
        throw Exception("DecodeJsonBinary: Unknown JSON Element Type:" + std::to_string(type), ErrorCodes::LOGICAL_ERROR);
    }

    size++;
    cursor = base + size;
    if (!doDecode)
        return static_cast<typename need_decode<doDecode>::type>(0);
    else
        return static_cast<typename need_decode<doDecode>::type>(raw_value.substr(base, size));
}

void JsonBinary::SkipJson(size_t & cursor, const String & raw_value)
{
    DecodeJson<false>(cursor, raw_value);
}

String JsonBinary::DecodeJsonAsBinary(size_t & cursor, const String & raw_value)
{
    return DecodeJson<true>(cursor, raw_value);
}

void JsonBinary::marshalObjectTo(String & str) const
{
    auto element_count = getElementCount();
    str.append("{");
    for (size_t i = 0; i < element_count; ++i) {
        if (i != 0)
            str.append(", ");

        marshalStringTo(str, getObjectKey(i));
        str.append(": ");
        getObjectValue(i).marshalTo(str);
    }
    str.append("}");
}

void JsonBinary::marshalArrayTo(String & str) const
{
    auto element_count = getElementCount();
	str.append("[");
	for (size_t i = 0; i < element_count; ++i) {
        if (i != 0)
            str.append(", ");

        getArrayElement(i).marshalTo(str);
    }
    str.append("]");
}

void JsonBinary::marshalFloat64To(String & str, double f)
{
    RUNTIME_CHECK(!isinf(f) && !isnan(f));
    /// Comments from TiDB:
    /// Convert as if by ES6 number to string conversion.
    /// This matches most other JSON generators.
    /// See golang.org/issue/6384 and golang.org/issue/14135.
    /// Like fmt %g, but the exponent cutoffs are different:
    /// and exponents themselves are not padded to two digits.
    /// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.

    /// Differences with TiDB:
    /// TiDB use scientific formatted for f_abs in (0, 1e-6) or [1e21, +Inf)
    /// TiFlash use scientific formatted for f_abs in (0, 1e-4) or [1e21, +Inf)
    /// Differences are introduced by fmt::format's 'none' type float format behavior
    /// which uses scientific formatted for f_abs in: (0, 1e-4) or [1e16, +Inf)
    double f_abs = fabs(f);
    String result;
    if likely (f_abs < 1e16 || f_abs >= 1e21)
        result = fmt::format("{:}", f); /// use 'none' type, like 'g', while with shortest precisions
    else
        result = fmt::format("{:.0f}", f); /// for f_abs in [1e16, 1e21), use 'f' mode with 0 precision is all right

    size_t length = result.length();
    // clean up e-09 to e-9; e+09 won't be scientific formatted
    if (length >= 4 && result[length - 4] == 'e' && result[length - 3] == '-' && result[length - 2] == '0')
    {
        str.append(result.data(), length - 2);
        str.append(result.data() + length - 1, 1);
    }
    else
    {
        str.append(result);
    }
}

void JsonBinary::marshalLiteralTo(String & str, UInt8 lit_type)
{
    switch (lit_type) {
    case LITERAL_NIL:
        str.append("null");
        break;
    case LITERAL_TRUE:
        str.append("true");
        break;
    case LITERAL_FALSE:
        str.append("false");
        break;
    }
}

void JsonBinary::marshalStringTo(String & str, const StringRef & ref)
{
    const UInt8 ascii_max = 0x7F;
    str.append("\"");
    size_t start = 0;
    size_t ref_size = ref.size;

    for (size_t i = 0; i < ref_size;)
    {
        auto byte_c = static_cast<UInt8>(ref.data[i]);
        if (byte_c <= ascii_max)
        {
            if (JsonSafeAscii[byte_c])
            {
                ++i;
                continue;
            }
            if (start < i)
                str.append(ref.data + start, i - start);
            switch (byte_c)
            {
            case '\\':
                str.append("\\\\");
                break;
            case '"':
                str.append("\\\"");
                break;
            case '\n':
                str.append("\\n");
                break;
            case '\r':
                str.append("\\r");
                break;
            case '\t':
                str.append("\\t");
                break;
            default:
                // This encodes bytes < 0x20 except for \t, \n and \r.
                // If escapeHTML is set, it also escapes <, >, and &
                // because they can lead to security holes when
                // user-controlled strings are rendered into JSON
                // and served to some browsers.
                str.append("\\u00");
                str.append(JsonHexChars[byte_c >> 4], 1);
                str.append(JsonHexChars[byte_c & 0xF], 1);
            }
            ++i;
            start = i;
            continue;
        }
        auto res = utf8Decode(ref.data + i, ref_size - i);
        if (res.first == UTF8Error && res.second == 1)
        {
            if (start < i)
                str.append(ref.data + start, i - start);
            str.append("\\ufffd"); /// append 0xFFFD as "Unknown Character" aligned with golang
            i += 1;
            start = i;
            continue;
        }
        // U+2028 is LINE SEPARATOR.
        // U+2029 is PARAGRAPH SEPARATOR.
        // They are both technically valid characters in JSON strings,
        // but don't work in JSONP, which has to be evaluated as JavaScript,
        // and can lead to security holes there. It is valid JSON to
        // escape them, so we do so unconditionally.
        // See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
        if (res.first == 0x2028 || res.first == 0x2029)
        {
            if (start < i)
                str.append(ref.data + start, i - start);
            str.append("\\u202");
            str.append(JsonHexChars[res.first & 0xF], 1);
            i += res.second;
            start = i;
            continue;
        }
        i += res.second;
    }

    if (start < ref_size)
        str.append(ref.data + start, ref_size - start);

    str.append("\"");
}

String encodeBase64(const StringRef & str)
{
    std::ostringstream oss;
    Poco::Base64Encoder encoder(oss, Poco::BASE64_NO_PADDING);
    encoder.rdbuf()->setLineLength(0); /// No newline characters would be added
    Poco::MemoryInputStream mis(str.data, str.size);
    Poco::StreamCopier::copyStream(mis, encoder);
    encoder.close();
    return oss.str();
}

void JsonBinary::marshalOpaqueTo(String & str, const Opaque & opaque)
{
    const String base64_padding_ends[] = {"", "===", "==", "="};
    String out = encodeBase64(opaque.data);
    size_t out_length = out.length();
    size_t padding_index = out_length & 0x3; /// Padding to be 4 bytes alignment
    auto output = fmt::format("\"base64:type{}:{}{}\"", static_cast<UInt32>(opaque.type), out, base64_padding_ends[padding_index]);
    str.append(output);
}

void JsonBinary::marshalDurationTo(String & str, Int64 duration, UInt32 fsp)
{
    str.append("\"");
    str.append(MyDuration(duration, static_cast<UInt8>(fsp)).toString());
    str.append("\"");
}

void JsonBinary::marshalTo(String & str) const
{
    switch (type)
    {
    case TYPE_CODE_OPAQUE:
        return marshalOpaqueTo(str, getOpaque());
    case TYPE_CODE_STRING:
        return marshalStringTo(str, getString());
    case TYPE_CODE_LITERAL:
        return marshalLiteralTo(str, data.data[0]);
    case TYPE_CODE_INT64:
        str.append(fmt::format("{}", getInt64()));
        return;
    case TYPE_CODE_UINT64:
        str.append(fmt::format("{}", getUInt64()));
        return;
    case TYPE_CODE_FLOAT64:
        return marshalFloat64To(str, getFloat64());
    case TYPE_CODE_ARRAY:
        return marshalArrayTo(str);
    case TYPE_CODE_OBJECT:
        return marshalObjectTo(str);
    case TYPE_CODE_DATE:
        str.append("\"");
        str.append(createMyDateFromCoreTime(getUInt64()).toString());
        str.append("\"");
        return;
    case TYPE_CODE_DATETIME:
    case TYPE_CODE_TIMESTAMP:
        str.append("\"");
        str.append(createMyDateTimeFromCoreTime(getUInt64()).toString(6));
        str.append("\"");
        return;
    case TYPE_CODE_DURATION:
    {
        size_t cursor = 8;
        return marshalDurationTo(str, getInt64(), decodeNumeric<UInt32>(cursor, data));
    }
    /// Do nothing for default
    }
}

String JsonBinary::toString() const
{
    String res;
    res.reserve(data.size * 3 / 2);
    marshalTo(res);
    return res;
}

String JsonBinary::unquoteJsonString(const StringRef & ref)
{
    String result;
    size_t ref_size = ref.size;
    result.reserve(ref_size);
	for (size_t i = 0; i < ref_size; ++i)
    {
        if (ref.data[i] == '\\')
        {
            ++i;
            RUNTIME_CHECK (i != ref_size);
            switch (ref.data[i])
            {
            case '"':
                result.append("\"");
                break;
            case 'b':
                result.append("\b");
                break;
            case 'f':
                result.append("\f");
                break;
            case 'n':
                result.append("\n");
                break;
            case 'r':
                result.append("\r");
                break;
            case 't':
                result.append("\t");
                break;
            case '\\':
                result.append("\\");
                break;
            case 'u':
            {
                RUNTIME_CHECK(i + 4 <= ref_size);
                for (size_t j = i; j < i + 4; ++j)
                    RUNTIME_CHECK(isHexDigit(ref.data[j]));
                auto str = String(ref.data[i], 4);
                auto val = std::stoul(str, nullptr, 16);
                size_t utf_length = 0;
                char utf_code_array[4];
                utf8Encode(utf_code_array, utf_length, val);
                result.append(utf_code_array, utf_length);
                i += 4;
                break;
            }
            default:
                // For all other escape sequences, backslash is ignored.
                result.append(ref.data + i, 1);
            }
        }
        else
        {
            result.append(ref.data + i, 1);
        }
    }
    return result;
}

String JsonBinary::unquoteString(const StringRef & ref)
{
    auto length = ref.size;
    if (length < 2)
        return ref.toString();

    if (ref.data[0] == '"' && ref.data[length - 1] == '"')
    {
        // Remove prefix and suffix '"' before unquoting
        return unquoteJsonString(StringRef(ref.data + 1, length -2));
    }
    // if value is not double quoted, do nothing
    return ref.toString();
}

String JsonBinary::unquote() const
{
    String res;
    switch (type)
    {
    case TYPE_CODE_STRING:
        return unquoteString(getString());
    default:
        return toString();
    }
}

UInt64 GetJsonLength(const std::string_view & raw_value)
{
    if (raw_value.empty())
    {
        return 0;
    }
    size_t cursor = 0;
    switch (raw_value[0]) // JSON Root element type
    {
    case JsonBinary::TYPE_CODE_OBJECT:
    case JsonBinary::TYPE_CODE_ARRAY:
        ++cursor;
        return decodeNumeric<UInt32>(cursor, raw_value);
    default:
        return 1;
    }
}
} // namespace DB
