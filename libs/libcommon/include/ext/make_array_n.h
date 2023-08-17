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

#include <array>
#include <type_traits>
#include <utility>


/** \brief Produces std::array of specified size, containing copies of provided object.
  *    Copy is performed N-1 times, and the last element is being moved.
  * This helper allows to initialize std::array in place.
  */
namespace ext
{
namespace detail
{

template <std::size_t size, typename T, std::size_t... indexes>
constexpr auto make_array_n_impl(T && value, std::index_sequence<indexes...>)
{
    /// Comma is used to make N-1 copies of value
    return std::array<std::decay_t<T>, size>{(static_cast<void>(indexes), value)..., std::forward<T>(value)};
}

} // namespace detail

template <typename T>
constexpr auto make_array_n(std::integral_constant<std::size_t, 0>, T &&)
{
    return std::array<std::decay_t<T>, 0>{};
}

template <std::size_t size, typename T>
constexpr auto make_array_n(std::integral_constant<std::size_t, size>, T && value)
{
    return detail::make_array_n_impl<size>(std::forward<T>(value), std::make_index_sequence<size - 1>{});
}

template <std::size_t size, typename T>
constexpr auto make_array_n(T && value)
{
    return make_array_n(std::integral_constant<std::size_t, size>{}, std::forward<T>(value));
}
} // namespace ext
