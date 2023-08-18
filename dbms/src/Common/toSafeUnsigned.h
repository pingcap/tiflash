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

#include <common/types.h>

namespace DB
{
// toSafeUnsigned evaluates absolute value of argument `value` and cast it to unsigned type of `To`.
// it guarantees that no undefined behavior will occur and exact result can be represented by unsigned `To`.
template <typename To, typename From>
constexpr make_unsigned_t<To> toSafeUnsigned(const From & value)
{
    static_assert(is_integer_v<From>, "type From must be integral");
    static_assert(is_integer_v<To>, "type To must be integral");

    using ReturnType = make_unsigned_t<To>;

    static_assert(
        actual_size_v<ReturnType> >= actual_size_v<From>,
        "type unsigned To can't hold all values of type From");

    if constexpr (is_signed_v<From>)
    {
        if constexpr (is_boost_number_v<ReturnType>)
        {
            // assert that negation of std::numeric_limits<From>::min() will not result in overflow.
            // TODO: find credible source that describes numeric limits of boost multiprecision *checked* integers.
            static_assert(-std::numeric_limits<From>::max() == std::numeric_limits<From>::min());
            return static_cast<ReturnType>(boost::multiprecision::abs(value));
        }
        else
        {
            if (value < 0)
            {
                // both signed to unsigned conversion [1] and negation of unsigned integers [2] are well defined in C++.
                //
                // see:
                // [1]: https://en.cppreference.com/w/c/language/conversion#Integer_conversions
                // [2]: https://en.cppreference.com/w/cpp/language/operator_arithmetic#Unary_arithmetic_operators
                return -static_cast<ReturnType>(value);
            }
            else
                return static_cast<ReturnType>(value);
        }
    }
    else
        return static_cast<ReturnType>(value);
}

} // namespace DB
