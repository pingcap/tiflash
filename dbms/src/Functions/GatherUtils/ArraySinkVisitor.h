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
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
template <typename T>
struct NumericArraySink;

struct GenericArraySink;

template <typename ArraySink>
struct NullableArraySink;

using NumericArraySinks = typename TypeListMap<NumericArraySink, TypeListNumbers>::Type;
using BasicArraySinks = typename AppendToTypeList<GenericArraySink, NumericArraySinks>::Type;
using NullableArraySinks = typename TypeListMap<NullableArraySink, BasicArraySinks>::Type;
using TypeListArraySinks = typename TypeListConcat<BasicArraySinks, NullableArraySinks>::Type;

class ArraySinkVisitor : public ApplyTypeListForClass<Visitor, TypeListArraySinks>::Type
{
};

template <typename Derived>
class ArraySinkVisitorImpl : public VisitorImpl<Derived, ArraySinkVisitor>
{
};

} // namespace DB::GatherUtils
