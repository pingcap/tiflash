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

#pragma once

#include <Core/Types.h>
#include <common/memcpy.h>
#include <Common/PODArray.h>

namespace DB
{
using JsonType = UInt8;
using Bytes_t = PODArray<UInt8>;
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
    const JsonType TYPE_CODE_OBJECT = 0x01; // TypeCodeObject indicates the JSON is an object.
    const JsonType TYPE_CODE_ARRAY = 0x03; // TypeCodeArray indicates the JSON is an array.
    const JsonType TYPE_CODE_LITERAL = 0x04; // TypeCodeLiteral indicates the JSON is a literal.
    const JsonType TYPE_CODE_INT64 = 0x09; // TypeCodeInt64 indicates the JSON is a signed integer.
    const JsonType TYPE_CODE_UINT64 = 0x0a; // TypeCodeUint64 indicates the JSON is a unsigned integer.
    const JsonType TYPE_CODE_FLOAT64 = 0x0b; // TypeCodeFloat64 indicates the JSON is a double float number.
    const JsonType TYPE_CODE_STRING = 0x0c; // TypeCodeString indicates the JSON is a string.
    const JsonType TYPE_CODE_Opaque = 0x0d; // TypeCodeOpaque indicates the JSON is an opaque.
    const JsonType TYPE_CODE_Date = 0x0e; // TypeCodeDate indicates the JSON is a date.
    const JsonType TYPE_CODE_Datetime = 0x0f; // TypeCodeDatetime indicates the JSON is a datetime.
    const JsonType TYPE_CODE_Timestamp = 0x10; // TypeCodeTimestamp indicates the JSON is a timestamp.
    const JsonType TYPE_CODE_Duration = 0x11; // TypeCodeDuration indicates the JSON is a duration.

    const UInt8 LITERAL_NIL = 0x00; // LiteralNil represents JSON null.
    const UInt8 LITERAL_TRUE = 0x01; // LiteralTrue represents JSON true.
    const UInt8 LITERAL_FALSE = 0x02; // LiteralFalse represents JSON false.

    explicit JsonBinary(const String & json_str);
    explicit JsonBinary(const char * pos, size_t length)
    {
        data.resize(length);
        inline_memcpy(&data[0], pos, length);
    }

    static void SkipJson(size_t & cursor, const String & raw_value);
    static String DecodeJsonAsBinary(size_t & cursor, const String & raw_value);

private:
    JsonType type;
    std::vector<UInt8> data; // First byte is JsonType
};

} // namespace DB