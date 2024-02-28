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
void encodeDAGVectorFloat32(const Array &, WriteBuffer &);

Int64 decodeDAGInt64(const String &);
UInt64 decodeDAGUInt64(const String &);
Float32 decodeDAGFloat32(const String &);
Float64 decodeDAGFloat64(const String &);
String decodeDAGString(const String &);
String decodeDAGBytes(const String &);
Field decodeDAGDecimal(const String &);
Field decodeDAGVectorFloat32(const String &);

} // namespace DB
