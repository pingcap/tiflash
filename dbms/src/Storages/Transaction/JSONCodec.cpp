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
#include <Storages/Transaction/JSONCodec.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

/**
 * https://github.com/pingcap/tidb/blob/release-3.0/types/json/binary.go
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
   value ::=
       object  |
       array   |
       literal |
       number  |
       string  |
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
 */
namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using JsonVar = Poco::Dynamic::Var;

extern const UInt8 TYPE_CODE_OBJECT = 0x01; // TypeCodeObject indicates the JSON is an object.
extern const UInt8 TYPE_CODE_ARRAY = 0x03; // TypeCodeArray indicates the JSON is an array.
extern const UInt8 TYPE_CODE_LITERAL = 0x04; // TypeCodeLiteral indicates the JSON is a literal.
extern const UInt8 TYPE_CODE_INT64 = 0x09; // TypeCodeInt64 indicates the JSON is a signed integer.
extern const UInt8 TYPE_CODE_UINT64 = 0x0a; // TypeCodeUint64 indicates the JSON is a unsigned integer.
extern const UInt8 TYPE_CODE_FLOAT64 = 0x0b; // TypeCodeFloat64 indicates the JSON is a double float number.
extern const UInt8 TYPE_CODE_STRING = 0x0c; // TypeCodeString indicates the JSON is a string.
extern const UInt8 LITERAL_NIL = 0x00; // LiteralNil represents JSON null.
extern const UInt8 LITERAL_TRUE = 0x01; // LiteralTrue represents JSON true.
extern const UInt8 LITERAL_FALSE = 0x02; // LiteralFalse represents JSON false.

constexpr size_t VALUE_ENTRY_SIZE = 5;
constexpr size_t KEY_ENTRY_LENGTH = 6;
constexpr size_t PREFIX_LENGTH = 8;

using JsonArrayPtr = Poco::JSON::Array::Ptr;
using JsonObjectPtr = Poco::JSON::Object::Ptr;

JsonArrayPtr decodeArray(size_t & cursor, const String & raw_value);
JsonObjectPtr decodeObject(size_t & cursor, const String & raw_value);
inline JsonVar decodeLiteral(size_t & cursor, const String & raw_value);
inline String decodeString(size_t & cursor, const String & raw_value);
JsonVar decodeValue(UInt8 type, size_t & cursor, const String & raw_value);

// Below funcs decode via relative offset and base offset does not move
JsonVar decodeValueEntry(size_t base, const String & raw_value, size_t value_offset);
inline String decodeString(size_t base, const String & raw_value, size_t length);

template <typename T>
inline T decodeNumeric(size_t & cursor, const String & raw_value)
{
    T res = *(reinterpret_cast<const T *>(raw_value.data() + cursor));
    cursor += sizeof(T);
    return res;
}

JsonObjectPtr decodeObject(size_t & cursor, const String & raw_value)
{
    UInt32 element_count = decodeNumeric<UInt32>(cursor, raw_value);
    size_t size = decodeNumeric<UInt32>(cursor, raw_value);
    size_t base = cursor;
    JsonObjectPtr obj_ptr = new Poco::JSON::Object();

    for (UInt32 i = 0; i < element_count; i++)
    {
        // offset points to head of string content instead of length so - 2
        size_t entry_base = base + i * KEY_ENTRY_LENGTH;
        size_t key_offset = base + decodeNumeric<UInt32>(entry_base, raw_value) - 8;
        size_t key_length = decodeNumeric<UInt16>(entry_base, raw_value);
        String key = decodeString(key_offset, raw_value, key_length);

        JsonVar val = decodeValueEntry(base, raw_value, element_count * KEY_ENTRY_LENGTH + i * VALUE_ENTRY_SIZE);
        obj_ptr->set(key, val);
    }
    cursor += size - 8;

    return obj_ptr;
}

JsonArrayPtr decodeArray(size_t & cursor, const String & raw_value)
{
    UInt32 element_count = decodeNumeric<UInt32>(cursor, raw_value);
    size_t size = decodeNumeric<UInt32>(cursor, raw_value);

    JsonArrayPtr array_ptr = new Poco::JSON::Array();
    for (UInt32 i = 0; i < element_count; i++)
    {
        JsonVar val = decodeValueEntry(cursor, raw_value, VALUE_ENTRY_SIZE * i);
        array_ptr->add(val);
    }
    cursor += size - 8;
    return array_ptr;
}

JsonVar decodeValueEntry(size_t base, const String & raw_value, size_t value_entry_offset)
{
    UInt8 type = raw_value[base + value_entry_offset];
    size_t abs_entry_offset = base + value_entry_offset + 1;

    if (type == TYPE_CODE_LITERAL)
    {
        return decodeLiteral(abs_entry_offset, raw_value);
    }

    size_t value_offset = base + decodeNumeric<UInt32>(abs_entry_offset, raw_value) - PREFIX_LENGTH;
    return decodeValue(type, value_offset, raw_value);
}

JsonVar decodeValue(UInt8 type, size_t & cursor, const String & raw_value)
{
    switch (type) // JSON Root element type
    {
    case TYPE_CODE_OBJECT:
        return decodeObject(cursor, raw_value);
    case TYPE_CODE_ARRAY:
        return decodeArray(cursor, raw_value);
    case TYPE_CODE_LITERAL:
        return decodeLiteral(cursor, raw_value);
    case TYPE_CODE_INT64:
        return JsonVar(decodeNumeric<Int64>(cursor, raw_value));
    case TYPE_CODE_UINT64:
        return JsonVar(decodeNumeric<UInt64>(cursor, raw_value));
    case TYPE_CODE_FLOAT64:
        return JsonVar(decodeNumeric<Float64>(cursor, raw_value));
    case TYPE_CODE_STRING:
        return JsonVar(decodeString(cursor, raw_value));
    default:
        throw Exception("decodeValue: Unknown JSON Element Type:" + std::to_string(type), ErrorCodes::LOGICAL_ERROR);
    }
}

inline JsonVar decodeLiteral(size_t & cursor, const String & raw_value)
{
    UInt8 type = raw_value[cursor++];
    switch (type)
    {
    case LITERAL_FALSE:
        return JsonVar(false);
    case LITERAL_NIL:
        return JsonVar();
    case LITERAL_TRUE:
        return JsonVar(true);
    default:
        throw Exception("decodeLiteral: Unknown JSON Literal Type:" + std::to_string(type), ErrorCodes::LOGICAL_ERROR);
    }
}

inline String decodeString(size_t base, const String & raw_value, size_t length)
{
    return String(raw_value, base, length);
}

inline String decodeString(size_t & cursor, const String & raw_value)
{
    size_t length = DecodeVarUInt(cursor, raw_value);
    String val = String(raw_value, cursor, length);
    cursor += length;
    return val;
}

String DecodeJsonAsString(size_t & cursor, const String & raw_value)
{
    UInt8 type = raw_value[cursor++];
    return decodeValue(type, cursor, raw_value);
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
    case TYPE_CODE_OBJECT:
        cursor += 4;
        size = decodeNumeric<UInt32>(cursor, raw_value);
        break;
    case TYPE_CODE_ARRAY:
        cursor += 4;
        size = decodeNumeric<UInt32>(cursor, raw_value);
        break;
    case TYPE_CODE_LITERAL:
        size = 1;
        break;
    case TYPE_CODE_INT64:
    case TYPE_CODE_UINT64:
    case TYPE_CODE_FLOAT64:
        size = 8;
        break;
    case TYPE_CODE_STRING:
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

void SkipJson(size_t & cursor, const String & raw_value)
{
    DecodeJson<false>(cursor, raw_value);
}

String DecodeJsonAsBinary(size_t & cursor, const String & raw_value)
{
    return DecodeJson<true>(cursor, raw_value);
}

UInt64 GetJsonLength(std::string_view raw_value)
{
    if (raw_value.empty())
    {
        return 0;
    }
    switch (raw_value[0]) // JSON Root element type
    {
    case TYPE_CODE_OBJECT:
    case TYPE_CODE_ARRAY:
        return *(reinterpret_cast<const UInt32 *>(&raw_value[1]));
    default:
        return 1;
    }
}

} // namespace DB
