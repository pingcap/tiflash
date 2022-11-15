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
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using JsonVar = Poco::Dynamic::Var;

constexpr size_t VALUE_ENTRY_SIZE = 5;
//constexpr size_t VALUE_TYPE_SIZE = 1;
constexpr size_t KEY_ENTRY_LENGTH = 6;
constexpr size_t PREFIX_LENGTH = 8;
//constexpr size_t DATA_SIZE_OFF = 4;
//constexpr size_t KEY_LENGTH_OFF = 4;

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

void JsonBinary::SkipJson(size_t & cursor, const String & raw_value)
{
    DecodeJson<false>(cursor, raw_value);
}

String JsonBinary::DecodeJsonAsBinary(size_t & cursor, const String & raw_value)
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
