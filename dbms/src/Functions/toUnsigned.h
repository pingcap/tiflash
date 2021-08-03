#pragma once

#include <common/types.h>

namespace DB
{

template <typename To, typename From>
static constexpr make_unsigned_t<To> toUnsigned(const From & value)
{
    using ReturnType = make_unsigned_t<To>;

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
