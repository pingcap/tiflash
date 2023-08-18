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

#include <cstdlib>


namespace ext
{
/** \brief Returns number of elements in an automatic array. */
template <typename T, std::size_t N>
constexpr std::size_t size(const T (&)[N]) noexcept
{
    return N;
}

/** \brief Returns number of in a container providing size() member function. */
template <typename T>
constexpr auto size(const T & t)
{
    return t.size();
}
} // namespace ext
