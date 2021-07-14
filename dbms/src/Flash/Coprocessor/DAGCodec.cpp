#include <Flash/Coprocessor/DAGCodec.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{

void encodeDAGInt64(Int64 i, WriteBuffer & ss) { RecordKVFormat::encodeInt64(i, ss); }

void encodeDAGUInt64(UInt64 i, WriteBuffer & ss) { RecordKVFormat::encodeUInt64(i, ss); }

void encodeDAGFloat32(Float32 f, WriteBuffer & ss) { EncodeFloat64(f, ss); }

void encodeDAGFloat64(Float64 f, WriteBuffer & ss) { EncodeFloat64(f, ss); }

void encodeDAGString(const String & s, WriteBuffer & ss) { writeString(s, ss); }

void encodeDAGBytes(const String & bytes, WriteBuffer & ss) { writeString(bytes, ss); }

void encodeDAGDecimal(const Field & field, WriteBuffer & ss) { EncodeDecimal(field, ss); }

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

String decodeDAGString(const String & s) { return s; }

String decodeDAGBytes(const String & s) { return s; }

Field decodeDAGDecimal(const String & s)
{
    size_t cursor = 0;
    return DecodeDecimal(cursor, s);
}

} // namespace DB
