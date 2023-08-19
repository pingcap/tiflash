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
#include <Common/Exception.h>
#include <Functions/GatherUtils/ValueSourceVisitor.h>

namespace DB::GatherUtils
{
struct IValueSource
{
    virtual ~IValueSource() = default;

    virtual void accept(ValueSourceVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()));
    }

    virtual bool isConst() const { return false; }
};

template <typename Derived>
class ValueSourceImpl : public Visitable<Derived, IValueSource, ValueSourceVisitor>
{
};

} // namespace DB::GatherUtils
