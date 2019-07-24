#include "JSONCodec.h"
#include "Codec.h"


namespace DB
{

constexpr UInt8 TYPE_CODE_OBJECT = 0x01;  // TypeCodeObject indicates the JSON is an object.
constexpr UInt8 TYPE_CODE_ARRAY = 0x03;   // TypeCodeArray indicates the JSON is an array.
constexpr UInt8 TYPE_CODE_LITERAL = 0x04; // TypeCodeLiteral indicates the JSON is a literal.
constexpr UInt8 TYPE_CODE_INT64 = 0x09;   // TypeCodeInt64 indicates the JSON is a signed integer.
constexpr UInt8 TYPE_CODE_UINT64 = 0x0a;  // TypeCodeUint64 indicates the JSON is a unsigned integer.
constexpr UInt8 TYPE_CODE_FLOAT64 = 0x0b; // TypeCodeFloat64 indicates the JSON is a double float number.
constexpr UInt8 TYPE_CODE_STRING = 0x0c;  // TypeCodeString indicates the JSON is a string.
constexpr UInt8 LITERAL_NIL = 0x00;       // LiteralNil represents JSON null.
constexpr UInt8 LITERAL_TRUE = 0x01;      // LiteralTrue represents JSON true.
constexpr UInt8 LITERAL_FALSE = 0x02;     // LiteralFalse represents JSON false.

constexpr int VALUE_ENTRY_SIZE = 5;
constexpr int KEY_ENTRY_LENGTH = 6;

JsonArrayPtr decodeArray(size_t & cursor, const String & raw_value);
JsonObjectPtr decodeObject(size_t & cursor, const String & raw_value);
JsonVar decodeValue(int type, size_t & cursor, const String & raw_value);
JsonVar decodeValueEntry(size_t & cursor, const String & raw_value);
JsonVar decodeLiteral(size_t & cursor, const String & raw_value);
const String decodeString(size_t & cursor, const String & raw_value);

template <typename T>
T decodeNumeric(size_t & cursor, const String & raw_value)
{
    T res = *(reinterpret_cast<const T *>(raw_value.data() + cursor));
    cursor += sizeof(T);
    return res;
}

JsonObjectPtr decodeObject(size_t & cursor, const String & raw_value)
{
    long elementCount = decodeNumeric<UInt32>(cursor, raw_value);
    decodeNumeric<UInt32>(cursor, raw_value);
    size_t base = cursor;
    JsonObjectPtr objPtr = new Poco::JSON::Object();

    for (int i = 0; i < elementCount; i++)
    {
        // offset points to head of string content instead of length so - 2
        size_t key_offset = base + decodeNumeric<UInt32>(cursor, raw_value) -2;
        String key = decodeString(key_offset, raw_value);

        size_t val_offset = base + elementCount * KEY_ENTRY_LENGTH + i * VALUE_ENTRY_SIZE;
        JsonVar val = decodeValueEntry(val_offset, raw_value);
        objPtr->set(key, val);
    }

    return objPtr;
}

JsonVar decodeLiteral(size_t & cursor, const String & raw_value)
{
    int type = raw_value[cursor++];
    switch (type)
    {
        case LITERAL_FALSE:
            return JsonVar(false);
        case LITERAL_NIL:
            return JsonVar();
        case LITERAL_TRUE:
            return JsonVar(true);
        default:
            throw Exception("Unknown JSON Literal Type:" + std::to_string(type), 0);
    }
}

const String decodeString(size_t & cursor, const String & raw_value)
{
    size_t length = DecodeVarUInt(cursor, raw_value);
    return String(raw_value, cursor, length);
}

JsonVar decodeValue(int type, size_t & cursor, const String & raw_value)
{
    switch (type)
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
            throw Exception("Unknown JSON Element Type:" + std::to_string(type), 0);
    }
}

JsonVar decodeValueEntry(size_t & cursor, const String & raw_value)
{
    int type = raw_value[cursor++];
    switch (type)
    {
        case TYPE_CODE_LITERAL:
            return decodeLiteral(cursor, raw_value);
        default:
            size_t value_offset = cursor + decodeNumeric<UInt32>(cursor, raw_value);
            return decodeValue(type, value_offset, raw_value);
    }
}

JsonArrayPtr decodeArray(size_t & cursor, const String & raw_value)
{
    long elementCount = decodeNumeric<UInt32>(cursor, raw_value); // elementCount
    decodeNumeric<UInt32>(cursor, raw_value);

    JsonArrayPtr arrayPtr = new Poco::JSON::Array();
    for (int i = 0; i < elementCount; i++)
    {
        size_t offset = cursor + VALUE_ENTRY_SIZE * i;
        JsonVar val = decodeValueEntry(offset, raw_value);
        arrayPtr->add(val);
    }
    return arrayPtr;
}

template <typename T>
String stringfy(T value)
{
    std::ostringstream os;
    value->stringify(os);
    return os.str();
}

String DecodeJson(size_t & cursor, const String & raw_value)
{
    UInt8 type = raw_value[cursor++];
    decodeNumeric<UInt32>(cursor, raw_value); // elementCount
    size_t size = decodeNumeric<UInt32>(cursor, raw_value);
    cursor += (size < 8 ? 0 : (size - 8));

    switch (type) // JSON Root element type
    {
        case TYPE_CODE_OBJECT:
            return stringfy(decodeObject(cursor, raw_value));
        case TYPE_CODE_ARRAY:
            return stringfy(decodeArray(cursor, raw_value));
        case TYPE_CODE_LITERAL:
            return decodeLiteral(cursor, raw_value).toString();
        case TYPE_CODE_INT64:
            return JsonVar(decodeNumeric<Int64>(cursor, raw_value));
        case TYPE_CODE_UINT64:
            return JsonVar(decodeNumeric<UInt64>(cursor, raw_value));
        case TYPE_CODE_FLOAT64:
            return JsonVar(decodeNumeric<Float64>(cursor, raw_value));
        case TYPE_CODE_STRING:
            return JsonVar(decodeString(cursor, raw_value));
        default:
            throw Exception("Unknown JSON Element Type:" + std::to_string(type), 0);
    }
}


}