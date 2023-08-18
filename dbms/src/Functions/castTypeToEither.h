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

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
class IDataType;

template <typename... Ts, typename F>
static bool castTypeToEither(const IDataType * input_type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    const IDataType * type = input_type;
    bool is_nullable = type->isNullable();
    if (is_nullable)
        type = typeid_cast<const DataTypeNullable *>(type)->getNestedType().get();
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type), is_nullable) : false) || ...);
}

} // namespace DB
