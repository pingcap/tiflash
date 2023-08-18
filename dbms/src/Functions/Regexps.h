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
#include <Common/OptimizedRegularExpression.h>
#include <Common/ProfileEvents.h>
#include <Functions/ObjectPool.h>
#include <Functions/likePatternToRegexp.h>

namespace DB
{
namespace Regexps
{
using Regexp = OptimizedRegularExpressionImpl<false>;
using Pool = ObjectPoolMap<Regexp, String>;

template <bool like>
inline Regexp createRegexp(const std::string & pattern, int flags)
{
    return Regexp{pattern, flags};
}

template <>
inline Regexp createRegexp<true>(const std::string & pattern, int flags)
{
    return Regexp{likePatternToRegexp(pattern), flags};
}

template <bool like, bool no_capture>
inline Pool::Pointer get(const std::string & pattern, int flags)
{
    /// C++11 has thread-safe function-local statics on most modern compilers.
    static Pool known_regexps; /// Different variables for different pattern parameters.

    return known_regexps.get(pattern, [&pattern, &flags] {
        if (no_capture)
            flags |= OptimizedRegularExpression::RE_NO_CAPTURE;

        return new Regexp{createRegexp<like>(pattern, flags)};
    });
}
} // namespace Regexps

} // namespace DB
