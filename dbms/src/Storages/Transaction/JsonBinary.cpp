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

#include <Common/MyDuration.h>
#include <Flash/Coprocessor/TiDBTime.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/JsonBinary.h>
#include <Storages/Transaction/JsonPathExprRef.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>

#include <cmath>
#pragma GCC diagnostic pop

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr char ELEM_SEPARATOR[] = ", ";
constexpr char KEY_VALUE_SEPARATOR[] = ": ";
constexpr size_t HEADER_SIZE = 8; // element size + data size.
constexpr size_t VALUE_ENTRY_SIZE = 5;
constexpr size_t VALUE_TYPE_SIZE = 1;
constexpr size_t KEY_ENTRY_SIZE = 6; // keyOff +  keyLen
constexpr size_t DATA_SIZE_OFFSET = 4;
//constexpr size_t KEY_LENGTH_OFFSET = 4;

template <typename T, typename S>
inline T decodeNumeric(size_t & cursor, const S & raw_value)
{
    RUNTIME_CHECK(cursor + sizeof(T) <= raw_value.length());
    T res = *(reinterpret_cast<const T *>(raw_value.data() + cursor));
    toLittleEndianInPlace(res);
    cursor += sizeof(T);
    return res;
}

template <typename T>
inline T decodeNumeric(size_t & cursor, const StringRef & raw_value)
{
    RUNTIME_CHECK(cursor + sizeof(T) <= raw_value.size);
    T res = *(reinterpret_cast<const T *>(raw_value.data + cursor));
    toLittleEndianInPlace(res);
    cursor += sizeof(T);
    return res;
}

char JsonBinary::getChar(size_t offset) const
{
    RUNTIME_CHECK(offset < data.size);
    return data.data[offset];
}

StringRef JsonBinary::getSubRef(size_t offset, size_t length) const
{
    RUNTIME_CHECK(offset + length <= data.size);
    return StringRef(data.data + offset, length);
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

String JsonBinary::getObjectKey(size_t index) const
{
    size_t cursor = HEADER_SIZE + index * KEY_ENTRY_SIZE;
    auto key_offset = decodeNumeric<UInt32>(cursor, data);
    auto key_length = decodeNumeric<UInt16>(cursor, data);
    return String(data.data + key_offset, key_length);
}

JsonBinary JsonBinary::getObjectValue(size_t index) const
{
    auto element_count = getElementCount();
    return getValueEntry(HEADER_SIZE + element_count * KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE);
}

JsonBinary JsonBinary::getValueEntry(size_t value_entry_offset) const
{
    JsonType entry_type = getChar(value_entry_offset);
    size_t cursor = value_entry_offset + VALUE_TYPE_SIZE;
    size_t value_offset = decodeNumeric<UInt32>(cursor, data); /// Literal type would padding zeros, thus it wouldn't cross array bound
    switch (entry_type)
    {
    case JsonBinary::TYPE_CODE_LITERAL:
        return JsonBinary(JsonBinary::TYPE_CODE_LITERAL, getSubRef(value_entry_offset + VALUE_TYPE_SIZE, 1));
    case JsonBinary::TYPE_CODE_INT64:
    case JsonBinary::TYPE_CODE_UINT64:
    case JsonBinary::TYPE_CODE_FLOAT64:
        return JsonBinary(entry_type, StringRef(data.data + value_offset, 8));
    case JsonBinary::TYPE_CODE_STRING:
    {
        cursor = value_offset;
        auto str_length = DecodeVarUInt(cursor, data);
        auto str_length_length = cursor - value_offset;
        return JsonBinary(entry_type, getSubRef(value_offset, str_length_length + str_length));
    }
    case JsonBinary::TYPE_CODE_OPAQUE:
    {
        cursor = value_offset + 1;
        auto data_length = DecodeVarUInt(cursor, data);
        auto data_length_length = cursor - value_offset - 1;
        return JsonBinary(entry_type, getSubRef(value_offset, data_length_length + data_length + 1)); /// one more byte for type
    }
    case JsonBinary::TYPE_CODE_DATE:
    case JsonBinary::TYPE_CODE_DATETIME:
    case JsonBinary::TYPE_CODE_TIMESTAMP:
        return JsonBinary(entry_type, getSubRef(value_offset, 8));
    case JsonBinary::TYPE_CODE_DURATION:
        return JsonBinary(entry_type, getSubRef(value_offset, 12));
    }
    cursor = value_offset + DATA_SIZE_OFFSET;
    auto data_size = decodeNumeric<UInt32>(cursor, data);
    return JsonBinary(entry_type, getSubRef(value_offset, data_size));
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
    return getSubRef(str_length_length, str_length);
}

JsonBinary::Opaque JsonBinary::getOpaque() const
{
    auto opaque_type = static_cast<UInt8>(data.data[0]);
    size_t cursor = 1;
    size_t data_length = DecodeVarUInt(cursor, data);
    size_t data_start = cursor;
    return Opaque{opaque_type, getSubRef(data_start, data_length)};
}

template <bool doDecode>
struct NeedDecode
{
};

template <>
struct NeedDecode<true>
{
    using Type = String;
};

template <>
struct NeedDecode<false>
{
    using Type = void;
};

template <bool doDecode>
typename NeedDecode<doDecode>::Type DecodeJson(size_t & cursor, const String & raw_value)
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
    if constexpr (!doDecode)
        return static_cast<typename NeedDecode<false>::Type>(0);
    else
        return static_cast<typename NeedDecode<true>::Type>(raw_value.substr(base, size));
}

void JsonBinary::SkipJson(size_t & cursor, const String & raw_value)
{
    DecodeJson<false>(cursor, raw_value);
}

String JsonBinary::DecodeJsonAsBinary(size_t & cursor, const String & raw_value)
{
    return DecodeJson<true>(cursor, raw_value);
}

void JsonBinary::marshalObjectTo(JsonBinaryWriteBuffer & write_buffer) const
{
    auto element_count = getElementCount();
    write_buffer.write('{');
    for (size_t i = 0; i < element_count; ++i)
    {
        if (i != 0)
            write_buffer.write(ELEM_SEPARATOR, 2);

        marshalStringTo(write_buffer, getObjectKey(i));
        write_buffer.write(KEY_VALUE_SEPARATOR, 2);
        getObjectValue(i).marshalTo(write_buffer);
    }
    write_buffer.write('}');
}

void JsonBinary::marshalArrayTo(JsonBinaryWriteBuffer & write_buffer) const
{
    auto element_count = getElementCount();
    write_buffer.write('[');
    for (size_t i = 0; i < element_count; ++i)
    {
        if (i != 0)
            write_buffer.write(ELEM_SEPARATOR, 2);

        getArrayElement(i).marshalTo(write_buffer);
    }
    write_buffer.write(']');
}

void JsonBinary::marshalFloat64To(JsonBinaryWriteBuffer & write_buffer, double f)
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
        write_buffer.write(result.data(), length - 2);
        write_buffer.write(result.data() + length - 1, 1);
    }
    else
    {
        write_buffer.write(result.c_str(), result.length());
    }
}

void JsonBinary::marshalLiteralTo(JsonBinaryWriteBuffer & write_buffer, UInt8 lit_type)
{
    switch (lit_type)
    {
    case LITERAL_NIL:
        write_buffer.write("null", 4);
        break;
    case LITERAL_TRUE:
        write_buffer.write("true", 4);
        break;
    case LITERAL_FALSE:
        write_buffer.write("false", 5);
        break;
    }
}

void JsonBinary::marshalStringTo(JsonBinaryWriteBuffer & write_buffer, const StringRef & ref)
{
    write_buffer.write('"');
    size_t start = 0;
    size_t ref_size = ref.size;

    for (size_t i = 0; i < ref_size;)
    {
        auto byte_c = static_cast<UInt8>(ref.data[i]);
        if (isASCII(byte_c))
        {
            if (JsonSafeAscii[byte_c])
            {
                ++i;
                continue;
            }
            if (start < i)
                write_buffer.write(ref.data + start, i - start);
            switch (byte_c)
            {
            case '\\':
                write_buffer.write("\\\\", 2);
                break;
            case '"':
                write_buffer.write("\\\"", 2);
                break;
            case '\n':
                write_buffer.write("\\n", 2);
                break;
            case '\r':
                write_buffer.write("\\r", 2);
                break;
            case '\t':
                write_buffer.write("\\t", 2);
                break;
            default:
                // This encodes bytes < 0x20 except for \t, \n and \r.
                // If escapeHTML is set, it also escapes <, >, and &
                // because they can lead to security holes when
                // user-controlled strings are rendered into JSON
                // and served to some browsers.
                write_buffer.write("\\u00", 4);
                write_buffer.write(JsonHexChars[byte_c >> 4]);
                write_buffer.write(JsonHexChars[byte_c & 0xF]);
            }
            ++i;
            start = i;
            continue;
        }
        auto res = UTF8::utf8Decode(ref.data + i, ref_size - i);
        if (res.first == UTF8::UTF8_Error && res.second == 1)
        {
            if (start < i)
                write_buffer.write(ref.data + start, i - start);
            write_buffer.write("\\ufffd", 6); /// append 0xFFFD as "Unknown Character" aligned with golang
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
                write_buffer.write(ref.data + start, i - start);
            write_buffer.write("\\u202", 5);
            write_buffer.write(JsonHexChars[res.first & 0xF]);
            i += res.second;
            start = i;
            continue;
        }
        i += res.second;
    }

    if (start < ref_size)
        write_buffer.write(ref.data + start, ref_size - start);

    write_buffer.write('"');
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

void JsonBinary::marshalOpaqueTo(JsonBinaryWriteBuffer & write_buffer, const Opaque & opaque)
{
    const String base64_padding_ends[] = {"", "===", "==", "="};
    String out = encodeBase64(opaque.data);
    size_t out_length = out.length();
    size_t padding_index = out_length & 0x3; /// Padding to be 4 bytes alignment
    auto output = fmt::format("\"base64:type{}:{}{}\"", static_cast<UInt32>(opaque.type), out, base64_padding_ends[padding_index]);
    write_buffer.write(output.c_str(), output.length());
}

void JsonBinary::marshalDurationTo(JsonBinaryWriteBuffer & write_buffer, Int64 duration, UInt32 fsp)
{
    write_buffer.write('"');
    const String & duration_str = MyDuration(duration, static_cast<UInt8>(fsp)).toString();
    write_buffer.write(duration_str.c_str(), duration_str.length());
    write_buffer.write('"');
}

void JsonBinary::marshalTo(JsonBinaryWriteBuffer & write_buffer) const
{
    switch (type)
    {
    case TYPE_CODE_OPAQUE:
        return marshalOpaqueTo(write_buffer, getOpaque());
    case TYPE_CODE_STRING:
        return marshalStringTo(write_buffer, getString());
    case TYPE_CODE_LITERAL:
        return marshalLiteralTo(write_buffer, data.data[0]);
    case TYPE_CODE_INT64:
    {
        auto str = fmt::format("{}", getInt64());
        write_buffer.write(str.c_str(), str.length());
        return;
    }
    case TYPE_CODE_UINT64:
    {
        auto str = fmt::format("{}", getUInt64());
        write_buffer.write(str.c_str(), str.length());
        return;
    }
    case TYPE_CODE_FLOAT64:
        return marshalFloat64To(write_buffer, getFloat64());
    case TYPE_CODE_ARRAY:
        return marshalArrayTo(write_buffer);
    case TYPE_CODE_OBJECT:
        return marshalObjectTo(write_buffer);
    case TYPE_CODE_DATE:
    {
        write_buffer.write('"');
        const auto & time_str = createMyDateFromCoreTime(getUInt64()).toString();
        write_buffer.write(time_str.c_str(), time_str.length());
        write_buffer.write('"');
        return;
    }
    case TYPE_CODE_DATETIME:
    case TYPE_CODE_TIMESTAMP:
    {
        write_buffer.write('"');
        const auto & time_str = createMyDateTimeFromCoreTime(getUInt64()).toString(6);
        write_buffer.write(time_str.c_str(), time_str.length());
        write_buffer.write('"');
        return;
    }
    case TYPE_CODE_DURATION:
    {
        size_t cursor = 8;
        return marshalDurationTo(write_buffer, getInt64(), decodeNumeric<UInt32>(cursor, data));
    }
        /// Do nothing for default
    }
}

String JsonBinary::toString() const
{
    ColumnString::Chars_t data_to;
    data_to.reserve(data.size * 3 / 2);
    JsonBinaryWriteBuffer write_buffer(data_to);
    marshalTo(write_buffer);
    write_buffer.finalize();
    return String(reinterpret_cast<char *>(data_to.data()), data_to.size());
}

void JsonBinary::toStringInBuffer(JsonBinaryWriteBuffer & write_buffer) const
{
    marshalTo(write_buffer);
}

String JsonBinary::unquoteJsonString(const StringRef & ref)
{
    String result;
    WriteBufferFromVector<String> write_buffer(result);
    unquoteJsonStringInBuffer(ref, write_buffer);
    write_buffer.finalize();
    return result;
}

void JsonBinary::unquoteStringInBuffer(const StringRef & ref, JsonBinaryWriteBuffer & write_buffer)
{
    auto length = ref.size;
    if (length < 2)
    {
        write_buffer.write(ref.data, ref.size);
        return;
    }

    if (ref.data[0] == '"' && ref.data[length - 1] == '"')
    {
        // Remove prefix and suffix '"' before unquoting
        unquoteJsonStringInBuffer(StringRef(ref.data + 1, length - 2), write_buffer);
    }
    else
    {
        // if value is not double quoted, do nothing
        write_buffer.write(ref.data, ref.size);
    }
}

String JsonBinary::unquoteString(const StringRef & ref)
{
    auto length = ref.size;
    if (length < 2)
        return ref.toString();

    if (ref.data[0] == '"' && ref.data[length - 1] == '"')
    {
        // Remove prefix and suffix '"' before unquoting
        return unquoteJsonString(StringRef(ref.data + 1, length - 2));
    }
    // if value is not double quoted, do nothing
    return ref.toString();
}

bool JsonBinary::extract(std::vector<JsonPathExprRefContainerPtr> & path_expr_container_vec, JsonBinaryWriteBuffer & write_buffer)
{
    std::vector<JsonBinary> extracted_json_binary_vec;
    for (auto & path_expr_container : path_expr_container_vec)
    {
        DupCheckSet dup_check_set = std::make_unique<std::unordered_set<const char *>>();
        const auto * first_path_ref = path_expr_container->firstRef();
        extractTo(extracted_json_binary_vec, first_path_ref, dup_check_set, false);
    }

    bool found;
    if (extracted_json_binary_vec.empty())
    {
        found = false;
    }
    else if (path_expr_container_vec.size() == 1 && extracted_json_binary_vec.size() == 1)
    {
        found = true;
        // Fix https://github.com/pingcap/tidb/issues/30352
        if (path_expr_container_vec[0]->firstRef() && path_expr_container_vec[0]->firstRef()->couldMatchMultipleValues())
        {
            buildBinaryJsonArrayInBuffer(extracted_json_binary_vec, write_buffer);
        }
        else
        {
            write_buffer.write(extracted_json_binary_vec[0].type);
            write_buffer.write(extracted_json_binary_vec[0].data.data, extracted_json_binary_vec[0].data.size);
        }
    }
    else
    {
        found = true;
        buildBinaryJsonArrayInBuffer(extracted_json_binary_vec, write_buffer);
    }
    return found;
}

bool jsonFinished(std::vector<JsonBinary> & json_binary_vec, bool one)
{
    return one && !json_binary_vec.empty();
}

std::optional<JsonBinary> JsonBinary::searchObjectKey(JsonPathObjectKey & key) const
{
    auto element_count = getElementCount();
    if (element_count == 0)
        return std::nullopt;

    UInt32 found_index;
    switch (key.status)
    {
    case JsonPathObjectKeyCached:
        found_index = key.cached_index;
        if (found_index < element_count && getObjectKey(found_index) == key.key)
            return {getObjectValue(found_index)};
        else
            found_index = binarySearchKey(key, element_count);
        break;
    case JsonPathObjectKeyCacheDisabled:
    case JsonPathObjectKeyUncached:
    default:
        found_index = binarySearchKey(key, element_count);
        break;
    }

    if (found_index < element_count && getObjectKey(found_index) == key.key)
    {
        if (key.status == JsonPathObjectKeyUncached)
        {
            key.cached_index = found_index;
            key.status = JsonPathObjectKeyCached;
        }
        return {getObjectValue(found_index)};
    }
    return std::nullopt;
}

/// Use binary search, since keys are guaranteed to be ascending ordered
UInt32 JsonBinary::binarySearchKey(const JsonPathObjectKey & key, UInt32 element_count) const
{
    RUNTIME_CHECK(element_count > 0);
    Int32 first = 0;
    Int32 distance = element_count - 1;
    while (distance > 0)
    {
        Int32 step = distance >> 2;
        const String & current_key = getObjectKey(first + step);
        if (current_key < key.key)
        {
            first += step;
            ++first;
            distance -= (step + 1);
        }
        else
            distance = step;
    }

    RUNTIME_CHECK(first >= 0);
    return static_cast<UInt32>(first);
}

void JsonBinary::extractTo(std::vector<JsonBinary> & json_binary_vec, ConstJsonPathExprRawPtr path_expr_ptr, DupCheckSet & dup_check_set, bool one) const
{
    if (!path_expr_ptr)
    {
        if (dup_check_set)
        {
            if (dup_check_set->find(data.data) != dup_check_set->end())
                return;
            dup_check_set->insert(data.data);
        }
        json_binary_vec.push_back(*this);
        return;
    }
    auto current_leg_pair = path_expr_ptr->popOneLeg();
    const auto & current_leg = current_leg_pair.first;
    RUNTIME_CHECK(current_leg);
    const auto * sub_path_expr_ptr = current_leg_pair.second;
    if (current_leg->type == JsonPathLeg::JsonPathLegArraySelection)
    {
        if (type != TYPE_CODE_ARRAY)
        {
            // If the current object is not an array, still append them if the selection includes
            // 0. But for asterisk, it still returns NULL.
            //
            // don't call `getIndexRange` or `getIndexFromStart`, they will panic if the argument
            // is not array.
            auto selection = current_leg->array_selection;
            switch (selection.type)
            {
            case JsonPathArraySelectionIndex:
                if (selection.index == 0)
                    extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
                break;
            case JsonPathArraySelectionRange:
                // for [0 to Non-negative Number] and [0 to last], it extracts itself
                if (selection.index_range[0] == 0 && selection.index_range[1] >= -1)
                    extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
                break;
            default:
                break;
            }
            return;
        }

        auto result = current_leg->array_selection.getIndexRange(*this);
        if (result.first >= 0 && result.first <= result.second)
        {
            auto start = static_cast<size_t>(result.first);
            auto end = static_cast<size_t>(result.second);
            for (size_t i = start; i <= end; ++i)
            {
                getArrayElement(i).extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
            }
        }
    }
    else if (current_leg->type == JsonPathLeg::JsonPathLegKey && type == TYPE_CODE_OBJECT)
    {
        auto element_count = getElementCount();
        if (current_leg->dot_key.key == "*")
        {
            for (size_t i = 0; i < element_count && !jsonFinished(json_binary_vec, one); ++i)
            {
                getObjectValue(i).extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
            }
        }
        else
        {
            auto search_result = searchObjectKey(current_leg->dot_key);
            if (search_result)
            {
                search_result->extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
            }
        }
    }
    else if (current_leg->type == JsonPathLeg::JsonPathLegDoubleAsterisk)
    {
        extractTo(json_binary_vec, sub_path_expr_ptr, dup_check_set, one);
        if (type == TYPE_CODE_ARRAY)
        {
            auto element_count = getElementCount();
            for (size_t i = 0; i < element_count && !jsonFinished(json_binary_vec, one); ++i)
            {
                getArrayElement(i).extractTo(json_binary_vec, path_expr_ptr, dup_check_set, one);
            }
        }
        else if (type == TYPE_CODE_OBJECT)
        {
            auto element_count = getElementCount();
            for (size_t i = 0; i < element_count && !jsonFinished(json_binary_vec, one); ++i)
            {
                getObjectValue(i).extractTo(json_binary_vec, path_expr_ptr, dup_check_set, one);
            }
        }
    }
}

void JsonBinary::buildBinaryJsonElementsInBuffer(const std::vector<JsonBinary> & json_binary_vec, JsonBinaryWriteBuffer & write_buffer)
{
    /// first, write value entry with value offset
    UInt32 value_offset = HEADER_SIZE + json_binary_vec.size() * VALUE_ENTRY_SIZE;
    for (const auto & bj : json_binary_vec)
    {
        write_buffer.write(bj.type);
        if (bj.type == TYPE_CODE_LITERAL)
        {
            /// Literal values are inlined in the value entry, total takes 4 bytes
            write_buffer.write(bj.data.data[0]);
            write_buffer.write(0);
            write_buffer.write(0);
            write_buffer.write(0);
        }
        else
        {
            auto endian_value_offset = value_offset;
            toLittleEndianInPlace<UInt32>(endian_value_offset);
            write_buffer.write(reinterpret_cast<const char *>(&endian_value_offset), sizeof(endian_value_offset));
            /// update value_offset
            value_offset += bj.data.size;
        }
    }

    /// second, write actual data
    for (const auto & bj : json_binary_vec)
    {
        if (bj.type != TYPE_CODE_LITERAL)
            write_buffer.write(bj.data.data, bj.data.size);
    }
}

void JsonBinary::buildBinaryJsonArrayInBuffer(const std::vector<JsonBinary> & json_binary_vec, JsonBinaryWriteBuffer & write_buffer)
{
    UInt32 total_size = HEADER_SIZE + json_binary_vec.size() * VALUE_ENTRY_SIZE;
    for (const auto & bj : json_binary_vec)
    {
        /// Literal type value are inlined in the value_entry memory
        if (bj.type != TYPE_CODE_LITERAL)
            total_size += bj.data.size;
    }

    write_buffer.write(TYPE_CODE_ARRAY);
    UInt32 element_count = json_binary_vec.size();
    toLittleEndianInPlace<UInt32>(element_count);
    write_buffer.write(reinterpret_cast<const char *>(&element_count), sizeof(element_count));

    toLittleEndianInPlace<UInt32>(total_size);
    write_buffer.write(reinterpret_cast<const char *>(&total_size), sizeof(total_size));
    buildBinaryJsonElementsInBuffer(json_binary_vec, write_buffer);
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
