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
#include <Common/TiFlashException.h>
#include <Core/Types.h>

#include <ext/singleton.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DECIMAL_OVERFLOW_ERROR;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using PrecType = UInt32;
using ScaleType = UInt32;

constexpr PrecType decimal_max_prec = 65;
constexpr ScaleType decimal_max_scale = 30;

// IntPrec indicates the max precision of different integer types.
// For now, the binary arithmetic functions use it to calculate result precision.
// And cast function use it to do some optimizations, such as skipping overflow check.
// But in TiDB the signed types will plus 1, for example IntPrec<int8_t>::prec is 4.
// This is a little confusing because we will add 1 when return result to client.
// Here we make sure TiFlash code is clean and will fix TiDB later.
template <typename T>
struct IntPrec
{
};
template <>
struct IntPrec<int8_t>
{
    static constexpr PrecType prec = 3;
};
template <>
struct IntPrec<uint8_t>
{
    static constexpr PrecType prec = 3;
};
template <>
struct IntPrec<int16_t>
{
    static constexpr PrecType prec = 5;
};
template <>
struct IntPrec<uint16_t>
{
    static constexpr PrecType prec = 5;
};
template <>
struct IntPrec<int32_t>
{
    static constexpr PrecType prec = 10;
};
template <>
struct IntPrec<uint32_t>
{
    static constexpr PrecType prec = 10;
};
template <>
struct IntPrec<int64_t>
{
    static constexpr PrecType prec = 19;
};
template <>
struct IntPrec<uint64_t>
{
    static constexpr PrecType prec = 20;
};

template <>
struct IntPrec<Int128>
{
    static constexpr PrecType prec = 39;
};

template <>
struct IntPrec<Int256>
{
    static constexpr PrecType prec = 78;
};

//  1) If the declared type of both operands of a dyadic arithmetic operator is exact numeric, then the declared
//  type of the result is an implementation-defined exact numeric type, with precision and scale determined as
//  follows:
//    a) Let S1 and S2 be the scale of the first and second operands respectively.
//    b) The precision of the result of addition and subtraction is implementation-defined, and the scale is the
//       maximum of S1 and S2.
//    c) The precision of the result of multiplication is implementation-defined, and the scale is S1 + S2.
//    d) The precision and scale of the result of division are implementation-defined.

struct PlusDecimalInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType left_prec,
        ScaleType left_scale,
        PrecType right_prec,
        ScaleType right_scale,
        ScaleType /*div_precincrement is not used*/)
    {
        ScaleType result_scale = std::max(left_scale, right_scale);
        PrecType result_int = std::max(left_prec - left_scale, right_prec - right_scale);
        PrecType result_prec = std::min(result_scale + result_int + 1, decimal_max_prec);
        return {result_prec, result_scale};
    }
};

struct MulDecimalInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType left_prec,
        ScaleType left_scale,
        PrecType right_prec,
        ScaleType right_scale,
        ScaleType /*div_precincrement is not used*/)
    {
        return {
            std::min(left_prec + right_prec, decimal_max_prec),
            std::min(left_scale + right_scale, decimal_max_scale)};
    }
};

struct DivDecimalInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType left_prec,
        ScaleType left_scale,
        PrecType /* right_prec is not used */,
        ScaleType right_scale,
        ScaleType div_precincrement)
    {
        return {
            std::min(left_prec + right_scale + div_precincrement, decimal_max_prec),
            std::min(left_scale + div_precincrement, decimal_max_scale)};
    }
};

struct SumDecimalInferer
{
    static constexpr PrecType decimal_longlong_digits = 22;
    static std::tuple<PrecType, ScaleType> infer(PrecType prec, ScaleType scale)
    {
        return {std::min(prec + decimal_longlong_digits, decimal_max_prec), scale};
    }
};

struct AvgDecimalInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType left_prec,
        ScaleType left_scale,
        ScaleType div_precincrement)
    {
        return {
            std::min(left_prec + div_precincrement, decimal_max_prec),
            std::min(left_scale + div_precincrement, decimal_max_scale)};
    }
};

struct ModDecimalInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType left_prec,
        ScaleType left_scale,
        PrecType right_prec,
        ScaleType right_scale,
        ScaleType /*div_precincrement is not used*/)
    {
        return {std::max(left_prec, right_prec), std::max(left_scale, right_scale)};
    }
};

struct OtherInferer
{
    static std::tuple<PrecType, ScaleType> infer(PrecType, ScaleType, PrecType, ScaleType, ScaleType) { return {}; }
};

template <typename T>
struct Decimal
{
    T value;

    using NativeType = T;

    Decimal(const Decimal<T> & d) = default;
    Decimal() = default;
    Decimal(T v_) // NOLINT(google-explicit-constructor)
        : value(v_)
    {}

    constexpr Decimal<T> & operator=(Decimal<T> &&) = default;
    constexpr Decimal<T> & operator=(const Decimal<T> &) = default;

    String toString(ScaleType) const;

    template <
        typename U,
        std::enable_if_t<
            std::is_same_v<
                U,
                Int256> || std::is_same_v<U, Int512> || std::is_integral_v<U> || std::is_same_v<U, Int128>> * = nullptr>
    operator U() const // NOLINT(google-explicit-constructor)
    {
        return static_cast<U>(value);
    }

    template <typename U, std::enable_if_t<sizeof(U) >= sizeof(T)> * = nullptr>
    operator Decimal<U>() const // NOLINT(google-explicit-constructor)
    {
        return static_cast<U>(value);
    }

    operator T() const // NOLINT(google-explicit-constructor)
    {
        return value;
    }

    template <typename U>
    std::enable_if_t<std::is_floating_point_v<U>, U> toFloat(ScaleType scale) const
    {
        U result = static_cast<U>(value);
        for (ScaleType i = 0; i < scale; i++)
        {
            result /= 10;
        }
        return result;
    }

    template <typename U>
    const Decimal<T> & operator+=(const Decimal<U> & x)
    {
        value += static_cast<NativeType>(x.value);
        return *this;
    }
    const Decimal<T> & operator-=(const T & x)
    {
        value -= x;
        return *this;
    }
    const Decimal<T> & operator*=(const T & x)
    {
        value *= x;
        return *this;
    }
    const Decimal<T> & operator/=(const T & x)
    {
        value /= x;
        return *this;
    }
    const Decimal<T> & operator%=(const T & x)
    {
        value %= x;
        return *this;
    }
};

template <typename T>
inline bool operator<(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value < y.value;
}
template <typename T>
inline bool operator<=(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value <= y.value;
}
template <typename T>
inline bool operator>(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value > y.value;
}
template <typename T>
inline bool operator>=(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value >= y.value;
}
template <typename T>
inline bool operator==(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value == y.value;
}
template <typename T>
inline bool operator!=(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value != y.value;
}

template <typename T>
inline Decimal<T> operator+(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value + y.value;
}
template <typename T>
inline Decimal<T> operator-(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value - y.value;
}
template <typename T>
inline Decimal<T> operator*(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value * y.value;
}
template <typename T>
inline Decimal<T> operator/(const Decimal<T> & x, const Decimal<T> & y)
{
    return x.value / y.value;
}
template <typename T>
inline Decimal<T> operator-(const Decimal<T> & x)
{
    return -x.value;
}

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128 = Decimal<Int128>;
using Decimal256 = Decimal<Int256>;

template <typename T>
static constexpr PrecType maxDecimalPrecision()
{
    return 0;
}
template <>
constexpr PrecType maxDecimalPrecision<Decimal32>()
{
    return 9;
}
template <>
constexpr PrecType maxDecimalPrecision<Decimal64>()
{
    return 18;
}
template <>
constexpr PrecType maxDecimalPrecision<Decimal128>()
{
    return 38;
}
template <>
constexpr PrecType maxDecimalPrecision<Decimal256>()
{
    return 65;
}

template <typename T>
constexpr PrecType minDecimalPrecision()
{
    /// return a invalid value
    return maxDecimalPrecision<Decimal256>() + 1;
}
template <>
constexpr PrecType minDecimalPrecision<Decimal32>()
{
    return 1;
}
template <>
constexpr PrecType minDecimalPrecision<Decimal64>()
{
    return maxDecimalPrecision<Decimal32>() + 1;
}
template <>
constexpr PrecType minDecimalPrecision<Decimal128>()
{
    return maxDecimalPrecision<Decimal64>() + 1;
}
template <>
constexpr PrecType minDecimalPrecision<Decimal256>()
{
    return maxDecimalPrecision<Decimal128>() + 1;
}

template <typename T>
struct PromoteType
{
};

template <>
struct PromoteType<Int32>
{
    using Type = Int64;
};
template <>
struct PromoteType<Int64>
{
    using Type = Int128;
};
template <>
struct PromoteType<Int128>
{
    using Type = Int256;
};
template <>
struct PromoteType<Int256>
{
    using Type = Int512;
};

template <typename DataType>
constexpr bool IsDecimal = false;
template <>
inline constexpr bool IsDecimal<Decimal32> = true;
template <>
inline constexpr bool IsDecimal<Decimal64> = true;
template <>
inline constexpr bool IsDecimal<Decimal128> = true;
template <>
inline constexpr bool IsDecimal<Decimal256> = true;

class Field;

// parse str into integer representation.
// it returns std::nullopt if parsing fails. Otherwise, return value, corresponding precision and scale.
std::optional<std::tuple<Int256, PrecType, ScaleType>> parseDecimal(const char * str, size_t len);

// parse str into field. str should not contain "+" or "-".
bool parseDecimal(const char * str, size_t len, bool negative, Field & field);

class DecimalMaxValue final : public ext::Singleton<DecimalMaxValue>
{
    friend class ext::Singleton<DecimalMaxValue>;

    Int256 number[decimal_max_prec + 1];

public:
    static Int256 get(PrecType idx)
    {
        // In case DecimalMaxValue::get(IntPrec<Int256>::prec), where IntPrec<Int256>::prec > 65.
        assert(idx <= decimal_max_prec);
        return instance().getInternal(idx);
    }

    static Int256 maxValue() { return get(maxDecimalPrecision<Decimal256>()); }

private:
    DecimalMaxValue()
    {
        for (PrecType i = 1; i <= decimal_max_prec; i++)
        {
            number[i] = number[i - 1] * 10 + 9;
        }
    }

    Int256 getInternal(PrecType idx) const { return number[idx]; }
};

// In some case, getScaleMultiplier and its callee may not be auto inline by the compiler.
// This may hurt performance. __attribute__((flatten)) tells compliler to inline the callee of this function.
template <typename T, std::enable_if_t<IsDecimal<T>> * = nullptr>
__attribute__((flatten)) inline typename T::NativeType getScaleMultiplier(ScaleType scale)
{
    return static_cast<typename T::NativeType>(DecimalMaxValue::get(scale) + 1);
}

template <typename T, std::enable_if_t<is_integer_v<T>> * = nullptr>
__attribute__((flatten)) inline T getScaleMultiplier(ScaleType scale)
{
    return static_cast<T>(DecimalMaxValue::get(scale) + 1);
}

template <typename T>
inline void checkDecimalOverflow(Decimal<T> v, PrecType prec)
{
    auto max_value = DecimalMaxValue::get(prec);
    if (v.value > max_value || v.value < -max_value)
    {
        throw TiFlashException("Decimal value overflow", Errors::Decimal::Overflow);
    }
}

template <>
struct TypeName<Decimal32>
{
    static const char * get() { return "Decimal32"; }
};
template <>
struct TypeName<Decimal64>
{
    static const char * get() { return "Decimal64"; }
};
template <>
struct TypeName<Decimal128>
{
    static const char * get() { return "Decimal128"; }
};
template <>
struct TypeName<Decimal256>
{
    static const char * get() { return "Decimal256"; }
};

template <>
struct TypeId<Decimal32>
{
    static constexpr const TypeIndex value = TypeIndex::Decimal32;
};
template <>
struct TypeId<Decimal64>
{
    static constexpr const TypeIndex value = TypeIndex::Decimal64;
};
template <>
struct TypeId<Decimal128>
{
    static constexpr const TypeIndex value = TypeIndex::Decimal128;
};
template <>
struct TypeId<Decimal256>
{
    static constexpr const TypeIndex value = TypeIndex::Decimal256;
};

template <typename T, typename U>
std::enable_if_t<std::is_integral_v<T>, U> ToDecimal(T value, ScaleType scale)
{
    using UType = typename U::NativeType;
    UType scale_mul = getScaleMultiplier<U>(scale);
    U result = static_cast<UType>(value) * scale_mul;
    return result;
}

template <typename T, typename U>
std::enable_if_t<std::is_floating_point_v<T>, U> ToDecimal(T value, ScaleType scale)
{
    bool neg = false;
    if (value < 0)
    {
        neg = true;
        value = -value;
    }
    for (ScaleType i = 0; i < scale; i++)
    {
        value *= 10;
    }
    if (std::abs(value) > static_cast<T>(DecimalMaxValue::get(decimal_max_prec)))
    {
        throw TiFlashException("Decimal value overflow", Errors::Decimal::Overflow);
    }
    // rounding
    T ten_times_value = value * 10;
    using UType = typename U::NativeType;
    UType v(value);
    if (static_cast<Int256>(ten_times_value) % 10 >= 5)
    {
        v++;
    }
    if (neg)
    {
        v = -v;
    }
    return v;
}

template <typename T, typename U>
std::enable_if_t<IsDecimal<T>, U> ToDecimal(T /*value*/, ScaleType /*scale*/)
{
    throw Exception("Should not call here", ErrorCodes::LOGICAL_ERROR);
}

template <typename T, typename U>
std::enable_if_t<IsDecimal<T>, U> ToDecimal(const T & v, ScaleType v_scale, ScaleType scale)
{
    auto value = Int256(v.value);
    if (v_scale <= scale)
    {
        for (ScaleType i = v_scale; i < scale; i++)
            value *= 10;
    }
    else
    {
        bool need_to_round = false;
        for (ScaleType i = scale; i < v_scale; i++)
        {
            need_to_round = (value < 0 ? -value : value) % 10 >= 5;
            value /= 10;
        }
        if (need_to_round)
        {
            if (value < 0)
                value--;
            else
                value++;
        }
    }
    return static_cast<typename U::NativeType>(value);
}

template <typename T, typename U>
std::enable_if_t<!IsDecimal<T>, U> ToDecimal(const T & /*v*/, ScaleType /*v_scale*/, ScaleType /*scale*/)
{
    throw Exception("Should not call here", ErrorCodes::LOGICAL_ERROR);
}

} // namespace DB
