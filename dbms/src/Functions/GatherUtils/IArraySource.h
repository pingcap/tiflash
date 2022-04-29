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

#include <Columns/ColumnArray.h>
#include <Functions/GatherUtils/ArraySourceVisitor.h>

namespace DB::GatherUtils
{
struct IArraySource
{
    virtual ~IArraySource() = default;

    virtual size_t getSizeForReserve() const = 0;
    virtual const typename ColumnArray::Offsets & getOffsets() const = 0;
    virtual size_t getColumnSize() const = 0;
    virtual bool isConst() const { return false; }
    virtual bool isNullable() const { return false; }

    virtual void accept(ArraySourceVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()));
    }
};

template <typename Derived>
class ArraySourceImpl : public Visitable<Derived, IArraySource, ArraySourceVisitor>
{
};

} // namespace DB::GatherUtils
