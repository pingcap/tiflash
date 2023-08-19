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

#include <common/defines.h>

#include <cmath>
#include <limits>
#include <type_traits>

template <typename T>
inline bool isNaN(T x)
{
    UNUSED(x);
    /// To be sure, that this function is zero-cost for non-floating point types.
    if constexpr (std::is_floating_point_v<T>)
        return std::isnan(x);
    else
        return false;
}


template <typename T>
inline bool isFinite(T x)
{
    UNUSED(x);
    if constexpr (std::is_floating_point_v<T>)
        return std::isfinite(x);
    else
        return true;
}


template <typename T>
T NaNOrZero()
{
    if constexpr (std::is_floating_point_v<T>)
        return std::numeric_limits<T>::quiet_NaN();
    else
        return {};
}
