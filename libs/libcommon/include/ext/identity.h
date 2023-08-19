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

#include <utility>

namespace ext
{
/// \brief Identity function for use with other algorithms as a pass-through.
class Identity
{
    /** \brief Function pointer type template for converting identity to a function pointer.
         *    Presumably useless, provided for completeness. */
    template <typename T>
    using function_ptr_t = T && (*)(T &&);

    /** \brief Implementation of identity as a non-instance member function for taking function pointer. */
    template <typename T>
    static T && invoke(T && t)
    {
        return std::forward<T>(t);
    }

public:
    /** \brief Returns the value passed as a sole argument using perfect forwarding. */
    template <typename T>
    T && operator()(T && t) const
    {
        return std::forward<T>(t);
    }

    /** \brief Allows conversion of identity instance to a function pointer. */
    template <typename T>
    explicit operator function_ptr_t<T>() const
    {
        return &invoke;
    };
};
} // namespace ext
