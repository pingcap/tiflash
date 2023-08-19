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
struct NumericArraySource;

struct GenericArraySource;

template <typename ArraySource>
struct NullableArraySource;

template <typename Base>
struct ConstSource;

using NumericArraySources = typename TypeListMap<NumericArraySource, TypeListNumbers>::Type;
using BasicArraySources = typename AppendToTypeList<GenericArraySource, NumericArraySources>::Type;
using NullableArraySources = typename TypeListMap<NullableArraySource, BasicArraySources>::Type;
using BasicAndNullableArraySources = typename TypeListConcat<BasicArraySources, NullableArraySources>::Type;
using ConstArraySources = typename TypeListMap<ConstSource, BasicAndNullableArraySources>::Type;
using TypeListArraySources = typename TypeListConcat<BasicAndNullableArraySources, ConstArraySources>::Type;

class ArraySourceVisitor : public ApplyTypeListForClass<Visitor, TypeListArraySources>::Type
{
};

template <typename Derived>
class ArraySourceVisitorImpl : public VisitorImpl<Derived, ArraySourceVisitor>
{
};

} // namespace DB::GatherUtils
