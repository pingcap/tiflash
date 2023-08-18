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
#include <common/likely.h>

#include <limits>
namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_DIVISION;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}


#pragma GCC diagnostic pop

} // namespace DB