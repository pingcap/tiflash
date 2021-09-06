#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>

namespace DB
{

void encodeDAGInt64(Int64, WriteBuffer &);
void encodeDAGUInt64(UInt64, WriteBuffer &);
void encodeDAGFloat32(Float32, WriteBuffer &);
void encodeDAGFloat64(Float64, WriteBuffer &);
void encodeDAGString(const String &, WriteBuffer &);
void encodeDAGBytes(const String &, WriteBuffer &);
void encodeDAGDecimal(const Field &, WriteBuffer &);

Int64 decodeDAGInt64(const String &);
UInt64 decodeDAGUInt64(const String &);
Float32 decodeDAGFloat32(const String &);
Float64 decodeDAGFloat64(const String &);
String decodeDAGString(const String &);
String decodeDAGBytes(const String &);
Field decodeDAGDecimal(const String &);

} // namespace DB
