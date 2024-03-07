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

#include <Columns/ColumnString.h>
#include <Common/MyTime.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWriter.h>
#include <Core/Types.h>
#include <IO/Buffer/WriteBufferFromVector.h>
#include <common/StringRef.h>
#include <common/memcpy.h>
#include <simdjson.h>

#include <string_view>
#include <unordered_set>

namespace DB
{
struct JsonPathExprRef;
using ConstJsonPathExprRawPtr = JsonPathExprRef const *;
class JsonPathExprRefContainer;
using JsonPathExprRefContainerPtr = std::unique_ptr<JsonPathExprRefContainer>;
struct JsonPathObjectKey;
/**
 * https://github.com/pingcap/tidb/blob/release-6.4/types/json_binary.go
 * https://github.com/pingcap/tidb/blob/release-6.4/types/json_constants.go
   The binary JSON format from MySQL 5.7 is as follows:
   JSON doc ::= type value
   type ::=
       0x01 |       // large JSON object
       0x03 |       // large JSON array
       0x04 |       // literal (true/false/null)
       0x05 |       // int16
       0x06 |       // uint16
       0x07 |       // int32
       0x08 |       // uint32
       0x09 |       // int64
       0x0a |       // uint64
       0x0b |       // double
       0x0c |       // utf8mb4 string
       0x0d |       // opaque value
       0x0e |       // date
       0x0f |       // datetime
       0x10 |       // timestamp
       0x11 |       // time
   value ::=
       object  |
       array   |
       literal |
       number  |
       string  |
       opaque  |
       time    |
       duration |
   object ::= element-count size key-entry* value-entry* key* value*
   array ::= element-count size value-entry* value*
   // number of members in object or number of elements in array
   element-count ::= uint32
   // number of bytes in the binary representation of the object or array
   size ::= uint32
   key-entry ::= key-offset key-length
   key-offset ::= uint32
   key-length ::= uint16    // key length must be less than 64KB
   value-entry ::= type offset-or-inlined-value
   // This field holds either the offset to where the value is stored,
   // or the value itself if it is small enough to be inlined (that is,
   // if it is a JSON literal or a small enough [u]int).
   offset-or-inlined-value ::= uint32
   key ::= utf8mb4-data
   literal ::=
       0x00 |   // JSON null literal
       0x01 |   // JSON true literal
       0x02 |   // JSON false literal
   number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
                       // double is stored in a platform-independent, eight-byte
                       // format using float8store()
   string ::= data-length utf8mb4-data
   data-length ::= uint8*    // If the high bit of a byte is 1, the length
                             // field is continued in the next byte,
                             // otherwise it is the last byte of the length
                             // field. So we need 1 byte to represent
                             // lengths up to 127, 2 bytes to represent
                             // lengths up to 16383, and so on...
   opaque ::= typeId data-length byte*
   time ::= uint64
   duration ::= uint64 uint32
   typeId ::= byte
 */
class JsonBinary
{
public:
    using JsonType = UInt8;
    using DupCheckSet = std::unique_ptr<std::unordered_set<const char *>>;
    using JsonBinaryWriteBuffer = VectorWriter<ColumnString::Chars_t>;
    static constexpr JsonType TYPE_CODE_OBJECT = 0x01; // TypeCodeObject indicates the JSON is an object.
    static constexpr JsonType TYPE_CODE_ARRAY = 0x03; // TypeCodeArray indicates the JSON is an array.
    static constexpr JsonType TYPE_CODE_LITERAL = 0x04; // TypeCodeLiteral indicates the JSON is a literal.
    static constexpr JsonType TYPE_CODE_INT64 = 0x09; // TypeCodeInt64 indicates the JSON is a signed integer.
    static constexpr JsonType TYPE_CODE_UINT64 = 0x0a; // TypeCodeUInt64 indicates the JSON is a unsigned integer.
    static constexpr JsonType TYPE_CODE_FLOAT64 = 0x0b; // TypeCodeFloat64 indicates the JSON is a double float number.
    static constexpr JsonType TYPE_CODE_STRING = 0x0c; // TypeCodeString indicates the JSON is a string.
    static constexpr JsonType TYPE_CODE_OPAQUE = 0x0d; // TypeCodeOpaque indicates the JSON is an opaque.
    static constexpr JsonType TYPE_CODE_DATE = 0x0e; // TypeCodeDate indicates the JSON is a date.
    static constexpr JsonType TYPE_CODE_DATETIME = 0x0f; // TypeCodeDatetime indicates the JSON is a datetime.
    static constexpr JsonType TYPE_CODE_TIMESTAMP = 0x10; // TypeCodeTimestamp indicates the JSON is a timestamp.
    static constexpr JsonType TYPE_CODE_DURATION = 0x11; // TypeCodeDuration indicates the JSON is a duration.

    static constexpr UInt8 LITERAL_NIL = 0x00; // LiteralNil represents JSON null.
    static constexpr UInt8 LITERAL_TRUE = 0x01; // LiteralTrue represents JSON true.
    static constexpr UInt8 LITERAL_FALSE = 0x02; // LiteralFalse represents JSON false.

    static constexpr UInt64 MAX_JSON_DEPTH = 100;

    /// Opaque represents a raw database binary type
    struct Opaque
    {
        Opaque(UInt8 type_, const StringRef & data_)
            : type(type_)
            , data(data_)
        {}

        // TypeCode is the same with TiDB database type code
        UInt8 type;
        // Buf is the underlying bytes of the data
        StringRef data;
    };

    JsonBinary(JsonType type_, const StringRef & ref)
        : type(type_)
        , data(ref)
    {}

    /// getElementCount gets the count of Object or Array only.
    UInt32 getElementCount() const;
    String toString() const; /// For test usage, not efficient at all
    void toStringInBuffer(JsonBinaryWriteBuffer & write_buffer) const;

    std::vector<JsonBinary> extract(const std::vector<JsonPathExprRefContainerPtr> & path_expr_container_vec);
    /// Extract receives several path expressions as arguments, matches them in bj, and returns true if any match:
    ///	Serialize final results in 'write_buffer'
    bool extract(
        const std::vector<JsonPathExprRefContainerPtr> & path_expr_container_vec,
        JsonBinaryWriteBuffer & write_buffer);

    UInt64 getDepth() const;

    JsonType getType() const { return type; }

    std::vector<StringRef> getKeys() const;

    static String unquoteString(const StringRef & ref);
    static void unquoteStringInBuffer(const StringRef & ref, JsonBinaryWriteBuffer & write_buffer);
    static String unquoteJsonString(const StringRef & ref);


    static void SkipJson(size_t & cursor, const String & raw_value);
    static String DecodeJsonAsBinary(size_t & cursor, const String & raw_value);

    static void buildBinaryJsonArrayInBuffer(
        const std::vector<JsonBinary> & json_binary_vec,
        JsonBinaryWriteBuffer & write_buffer);
    static void buildKeyArrayInBuffer(const std::vector<StringRef> & keys, JsonBinaryWriteBuffer & write_buffer);

    static void appendNumber(JsonBinaryWriteBuffer & write_buffer, bool value);
    static void appendNumber(JsonBinaryWriteBuffer & write_buffer, UInt64 value);
    static void appendNumber(JsonBinaryWriteBuffer & write_buffer, Int64 value);
    static void appendNumber(JsonBinaryWriteBuffer & write_buffer, Float64 value);
    static void appendStringRef(JsonBinaryWriteBuffer & write_buffer, const StringRef & value);
    static void appendOpaque(JsonBinaryWriteBuffer & write_buffer, const Opaque & value);
    static void appendDate(JsonBinaryWriteBuffer & write_buffer, const MyDate & value);
    static void appendTimestamp(JsonBinaryWriteBuffer & write_buffer, const MyDateTime & value);
    static void appendDatetime(JsonBinaryWriteBuffer & write_buffer, const MyDateTime & value);
    static void appendDuration(JsonBinaryWriteBuffer & write_buffer, Int64 duration, UInt64 fsp);
    static void appendNull(JsonBinaryWriteBuffer & write_buffer);

    static void appendSIMDJsonElem(JsonBinaryWriteBuffer & write_buffer, const simdjson::dom::element & elem);

    static void assertJsonDepth(UInt64 depth);

private:
    Int64 getInt64() const;
    UInt64 getUInt64() const;
    double getFloat64() const;
    StringRef getString() const;
    Opaque getOpaque() const;
    char getChar(size_t offset) const;
    StringRef getSubRef(size_t offset, size_t length) const;

    JsonBinary getArrayElement(size_t index) const;
    StringRef getObjectKey(size_t index) const;
    JsonBinary getObjectValue(size_t index) const;
    JsonBinary getValueEntry(size_t value_entry_offset) const;

    void marshalTo(JsonBinaryWriteBuffer & write_buffer) const;
    void marshalObjectTo(JsonBinaryWriteBuffer & write_buffer) const;
    void marshalArrayTo(JsonBinaryWriteBuffer & write_buffer) const;

    /// 'one' parameter is not used now, copied from TiDB's implementation, for future usage
    void extractTo(
        std::vector<JsonBinary> & json_binary_vec,
        ConstJsonPathExprRawPtr path_expr_ptr,
        DupCheckSet & dup_check_set,
        bool one) const;
    /// Use binary search, since keys are guaranteed to be ascending ordered
    UInt32 binarySearchKey(const JsonPathObjectKey & key, UInt32 element_count) const;
    std::optional<JsonBinary> searchObjectKey(JsonPathObjectKey & key) const;

    template <class WriteBuffer>
    static void unquoteJsonStringInBuffer(const StringRef & ref, WriteBuffer & write_buffer);
    static void buildBinaryJsonElementsInBuffer(
        const std::vector<JsonBinary> & json_binary_vec,
        JsonBinaryWriteBuffer & write_buffer);
    static void marshalFloat64To(JsonBinaryWriteBuffer & write_buffer, double f);
    static void marshalLiteralTo(JsonBinaryWriteBuffer & write_buffer, UInt8 lit_type);
    static void marshalStringTo(JsonBinaryWriteBuffer & write_buffer, const StringRef & ref);

    static void marshalOpaqueTo(JsonBinaryWriteBuffer & write_buffer, const Opaque & o);
    static void marshalDurationTo(JsonBinaryWriteBuffer & write_buffer, Int64 duration, UInt32 fsp);

private:
    JsonType type;
    /// 'data' doesn't contain type byte.
    /// In this way, when we construct new JsonBinary object for child field, new object's 'data' field can directly reference original object's data memory as a slice
    /// Avoid memory re-allocations
    StringRef data;
};

static const char JsonHexChars[] = "0123456789abcdef";

/// JsonSafeAscii holds the value true if the ASCII character with the given array
/// position can be represented inside a JSON string without any further
/// escaping.

/// All values are true except for the ASCII control characters (0-31), the
/// double quote ("), and the backslash character ("\").
/// Avoid expanding const arrays one element per line
// clang-format off
static const bool JsonSafeAscii[] = {
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    true, true, /* '"' */ false, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, /* '\\' */ false, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true
};
// clang-format on

template <class WriteBuffer>
void JsonBinary::unquoteJsonStringInBuffer(const StringRef & ref, WriteBuffer & write_buffer)
{
    size_t ref_size = ref.size;
    for (size_t i = 0; i < ref_size; ++i)
    {
        if (ref.data[i] == '\\')
        {
            ++i;
            RUNTIME_CHECK(i != ref_size);
            switch (ref.data[i])
            {
            case '"':
                write_buffer.write('\"');
                break;
            case 'b':
                write_buffer.write('\b');
                break;
            case 'f':
                write_buffer.write('\f');
                break;
            case 'n':
                write_buffer.write('\n');
                break;
            case 'r':
                write_buffer.write('\r');
                break;
            case 't':
                write_buffer.write('\t');
                break;
            case '\\':
                write_buffer.write('\\');
                break;
            case 'u':
            {
                RUNTIME_CHECK(i + 4 < ref_size);
                for (size_t j = i + 1; j < i + 5; ++j)
                    RUNTIME_CHECK(isHexDigit(ref.data[j]));
                auto str = String(ref.data + i + 1, 4);
                auto val = std::stoul(str, nullptr, 16);
                size_t utf_length = 0;
                char utf_code_array[4];
                UTF8::utf8Encode(utf_code_array, utf_length, val);
                write_buffer.write(utf_code_array, utf_length);
                i += 4;
                break;
            }
            default:
                // For all other escape sequences, backslash is ignored.
                write_buffer.write(ref.data[i]);
            }
        }
        else
        {
            write_buffer.write(ref.data[i]);
        }
    }
}
} // namespace DB