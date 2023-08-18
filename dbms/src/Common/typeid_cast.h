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
#include <common/demangle.h>

#include <string>
#include <type_traits>
#include <typeindex>
#include <typeinfo>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_CAST;
}
} // namespace DB


/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
std::enable_if_t<std::is_reference_v<To>, To> typeid_cast(From & from)
{
    if (typeid(from) == typeid(To))
        return static_cast<To>(from);
    else
        throw DB::Exception(
            "Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
            DB::ErrorCodes::BAD_CAST);
}

template <typename To, typename From>
To typeid_cast(From * from)
{
    if (typeid(*from) == typeid(std::remove_pointer_t<To>))
        return static_cast<To>(from);
    else
        return nullptr;
}
