#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>

namespace DB
{

void encodeDAGInt64(Int64, std::stringstream &);
void encodeDAGUInt64(UInt64, std::stringstream &);
void encodeDAGFloat32(Float32, std::stringstream &);
void encodeDAGFloat64(Float64, std::stringstream &);
void encodeDAGString(const String &, std::stringstream &);
void encodeDAGBytes(const String &, std::stringstream &);
void encodeDAGDecimal(const Field &, std::stringstream &);

Int64 decodeDAGInt64(const String &);
UInt64 decodeDAGUInt64(const String &);
Float32 decodeDAGFloat32(const String &);
Float64 decodeDAGFloat64(const String &);
String decodeDAGString(const String &);
String decodeDAGBytes(const String &);
Field decodeDAGDecimal(const String &);

} // namespace DB