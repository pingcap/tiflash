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
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes
} // namespace DB

/** Perform static_cast in release build.
  * Checks type by comparing typeid and throw an exception in debug build.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  */
template <typename To, typename From>
To assert_cast(From && from)
{
#ifndef NDEBUG
    try
    {
        if constexpr (std::is_pointer_v<To>)
        {
            if (typeid(*from) == typeid(std::remove_pointer_t<To>))
                return static_cast<To>(from);
        }
        else
        {
            if (typeid(from) == typeid(To))
                return static_cast<To>(from);
        }
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }

    throw DB::Exception(
        "Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
        DB::ErrorCodes::BAD_CAST);
#else
    return static_cast<To>(from);
#endif
}
