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

#include <Columns/IColumn.h>

namespace DB::GatherUtils
{
template <typename T>
struct NumericArraySlice
{
    const T * data;
    size_t size;
};

struct GenericArraySlice
{
    const IColumn * elements;
    size_t begin;
    size_t size;
};

template <typename Slice>
struct NullableSlice : public Slice
{
    const UInt8 * null_map = nullptr;

    NullableSlice() = default;
    NullableSlice(const Slice & base)
        : Slice(base)
    {}
};

template <typename T>
struct NumericValueSlice
{
    T value;
    static constexpr size_t size = 1;
};

struct GenericValueSlice
{
    const IColumn * elements;
    size_t position;
    static constexpr size_t size = 1;
};

} // namespace DB::GatherUtils
