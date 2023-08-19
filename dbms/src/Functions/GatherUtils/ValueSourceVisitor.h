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
struct NumericValueSource;

struct GenericValueSource;

template <typename ArraySource>
struct NullableValueSource;

template <typename Base>
struct ConstSource;

using NumericValueSources = typename TypeListMap<NumericValueSource, TypeListNumbers>::Type;
using BasicValueSources = typename AppendToTypeList<GenericValueSource, NumericValueSources>::Type;
using NullableValueSources = typename TypeListMap<NullableValueSource, BasicValueSources>::Type;
using BasicAndNullableValueSources = typename TypeListConcat<BasicValueSources, NullableValueSources>::Type;
using ConstValueSources = typename TypeListMap<ConstSource, BasicAndNullableValueSources>::Type;
using TypeListValueSources = typename TypeListConcat<BasicAndNullableValueSources, ConstValueSources>::Type;

class ValueSourceVisitor : public ApplyTypeListForClass<Visitor, TypeListValueSources>::Type
{
};

template <typename Derived>
class ValueSourceVisitorImpl : public VisitorImpl<Derived, ValueSourceVisitor>
{
};

} // namespace DB::GatherUtils
