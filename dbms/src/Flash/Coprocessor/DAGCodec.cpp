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

#include <Flash/Coprocessor/DAGCodec.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TiDB/Decode/DatumCodec.h>

namespace DB
{
void encodeDAGInt64(Int64 i, WriteBuffer & ss)
{
    RecordKVFormat::encodeInt64(i, ss);
}

void encodeDAGUInt64(UInt64 i, WriteBuffer & ss)
{
    RecordKVFormat::encodeUInt64(i, ss);
}

void encodeDAGFloat32(Float32 f, WriteBuffer & ss)
{
    EncodeFloat64(f, ss);
}

void encodeDAGFloat64(Float64 f, WriteBuffer & ss)
{
    EncodeFloat64(f, ss);
}

void encodeDAGString(const String & s, WriteBuffer & ss)
{
    writeString(s, ss);
}

void encodeDAGBytes(const String & bytes, WriteBuffer & ss)
{
    writeString(bytes, ss);
}

void encodeDAGDecimal(const Field & field, WriteBuffer & ss)
{
    EncodeDecimal(field, ss);
}

void encodeDAGVectorFloat32(const Array & v, WriteBuffer & ss)
{
    EncodeVectorFloat32(v, ss);
}

Int64 decodeDAGInt64(const String & s)
{
    auto u = *(reinterpret_cast<const UInt64 *>(s.data()));
    return RecordKVFormat::decodeInt64(u);
}

UInt64 decodeDAGUInt64(const String & s)
{
    auto u = *(reinterpret_cast<const UInt64 *>(s.data()));
    return RecordKVFormat::decodeUInt64(u);
}

Float32 decodeDAGFloat32(const String & s)
{
    size_t cursor = 0;
    return DecodeFloat64(cursor, s);
}

Float64 decodeDAGFloat64(const String & s)
{
    size_t cursor = 0;
    return DecodeFloat64(cursor, s);
}

String decodeDAGString(const String & s)
{
    return s;
}

String decodeDAGBytes(const String & s)
{
    return s;
}

Field decodeDAGDecimal(const String & s)
{
    size_t cursor = 0;
    return DecodeDecimal(cursor, s);
}

Field decodeDAGVectorFloat32(const String & s)
{
    size_t cursor = 0;
    return DecodeVectorFloat32(cursor, s);
}

} // namespace DB
