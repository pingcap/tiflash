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

#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <common/strong_typedef.h>

#include <algorithm>
#include <functional>
#include <type_traits>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_TYPE_OF_FIELD;
extern const int BAD_GET;
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int DECIMAL_OVERFLOW_ERROR;
} // namespace ErrorCodes

class Field;
using Array = std::vector<Field>;
using TupleBackend = std::vector<Field>;
STRONG_TYPEDEF(TupleBackend, Tuple); /// Array and Tuple are different types with equal representation inside Field.

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
#define DBMS_MIN_FIELD_SIZE 32

template <typename T>
bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);

#pragma GCC diagnostic push
#if defined(__clang__)
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#endif
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized" // this one should be a false positive
template <typename T>
class DecimalField
{
    static_assert(IsDecimal<T>);

public:
    using DecimalType = T;
    using NativeType = typename T::NativeType;

    DecimalField(T value, UInt32 scale_)
        : dec(value)
        , scale(scale_)
    {}

    operator T() const { return dec; } // NOLINT(google-explicit-constructor)

    template <typename U, std::enable_if_t<std::is_floating_point_v<U>> * = nullptr>
    operator U() const // NOLINT(google-explicit-constructor)
    {
        // clang-format off
        static const double ScaleMultiplierArray[] = {
            1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
            1e20, 1e21, 1e22
        };
        // clang-format on

        // Use double divide algorithm when both the dividend and the divisor can be precisely represented by double
        // 1e0,1e1,...1e22 can be precisely represented by double
        // Note: ensure that if precise_dividend is true, then the dividend must be precisely represented;
        //  if precise_dividend is false, the dividend still could have chance to be precisely represented. It doesn't affect
        // the correctness.
        auto v = static_cast<Float64>(dec.value);
        auto nearest_v = v > 0 ? v - 1 : v + 1;
        bool precise_dividend
            = ((dec.value <= (1LL << 53) && (dec.value >= -(1LL << 53)))
               || (static_cast<NativeType>(v) == dec.value && static_cast<NativeType>(nearest_v) != dec.value));
        // Note: even if scale = 0, we should still check precise_dividend here, because cast<double>(int256) doesn't
        // satisfy IEEE754 perfectly
        if likely (precise_dividend && scale <= 22)
        {
            v = v / ScaleMultiplierArray[scale];
            return v;
        }
        return atof(toString().c_str());
    }

    template <
        typename U,
        std::enable_if_t<std::is_integral_v<U> || std::is_same_v<U, Int128> || std::is_same_v<U, Int256>> * = nullptr>
    operator U() const // NOLINT(google-explicit-constructor)
    {
        Int256 v = dec.value;
        for (ScaleType i = 0; i < scale; i++)
        {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (v > std::numeric_limits<U>::max() || v < std::numeric_limits<U>::min())
        {
            throw TiFlashException("Decimal value overflow", Errors::Decimal::Overflow);
        }
        return static_cast<U>(v);
    }

    T getValue() const { return dec; }
    UInt32 getScale() const { return scale; }
    template <typename U>
    bool operator<(const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLess<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    UInt32 getPrec() const
    {
        UInt32 cnt = 0;
        auto x = dec.value;
        while (x != 0)
        {
            x /= 10;
            cnt++;
        }
        if (cnt == 0)
            cnt = 1;
        return std::max(cnt, scale);
    }

    /// In TiFlash there are 4 subtype of decimal:
    /// Decimal32, Decimal64, Decimal128 and Decimal256
    /// they are not compatible with each other. So a DecimalField<Decimal32>
    /// can not be inserted into a decimal column with DecimalType<Decimal64>
    /// getPrecWithCurrentDecimalType will return the prec that fit
    /// current decimal type, that is to say, current DecimalField can be
    /// inserted into a decimal column with type `Decimal(getPrecWithCurrentDecimalType, getScale)`
    UInt32 getPrecWithCurrentDecimalType() const
    {
        auto raw_prec = getPrec();
        return std::max(raw_prec, minDecimalPrecision<T>());
    }

    template <typename U>
    bool operator<=(const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLessOrEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator==(const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator>(const DecimalField<U> & r) const
    {
        return r < *this;
    }
    template <typename U>
    bool operator>=(const DecimalField<U> & r) const
    {
        return r <= *this;
    }
    template <typename U>
    bool operator!=(const DecimalField<U> & r) const
    {
        return !(*this == r);
    }

    String toString() const { return dec.toString(scale); }

    const DecimalField<T> & operator+=(const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception("Add different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec += r.getValue();
        return *this;
    }

    const DecimalField<T> & operator-=(const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception("Sub different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec -= r.getValue();
        return *this;
    }


private:
    T dec{};
    UInt32 scale{};
};
#pragma GCC diagnostic pop
/** Discriminated union of several types.
  * Made for replacement of `boost::variant`
  *  is not generalized,
  *  but somewhat more efficient, and simpler.
  *
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See Column.h
  */
class Field
{
public:
    struct Types
    {
        /// Type tag.
        enum Which
        {
            Null = 0,
            UInt64 = 1,
            Int64 = 2,
            Float64 = 3,
            UInt128 = 4,
            Int128 = 5,
            Int256 = 6,

            /// Non-POD types.

            String = 16,
            Array = 17,
            Tuple = 18,
            Decimal32 = 19,
            Decimal64 = 20,
            Decimal128 = 21,
            Decimal256 = 22,
        };

        static const int MIN_NON_POD = 16;

        static const char * toString(Which which)
        {
            switch (which)
            {
            case Null:
                return "Null";
            case UInt64:
                return "UInt64";
            case Int64:
                return "Int64";
            case Float64:
                return "Float64";
            case UInt128:
                return "UInt128";
            case Int128:
                return "Int128";
            case Int256:
                return "Int256";

            case String:
                return "String";
            case Array:
                return "Array";
            case Tuple:
                return "Tuple";
            case Decimal32:
                return "Decimal32";
            case Decimal64:
                return "Decimal64";
            case Decimal128:
                return "Decimal128";
            case Decimal256:
                return "Decimal256";

            default:
                throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
            }
        }
    };


    /// Returns an identifier for the type or vice versa.
    template <typename T>
    struct TypeToEnum;
    template <Types::Which which>
    struct EnumToType;


    Field()
        : which(Types::Null)
    {}

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field & rhs) { create(rhs); }

    Field(Field && rhs) { create(std::move(rhs)); }

    template <typename T>
    Field(
        T && rhs,
        std::integral_constant<
            int,
            Field::TypeToEnum<std::decay_t<T>>::value> * = nullptr) // NOLINT(google-explicit-constructor)
    {
        createConcrete(std::forward<T>(rhs));
    }

    /// Create a string inplace.
    Field(const char * data, size_t size) { create(data, size); }

    Field(const unsigned char * data, size_t size) { create(data, size); }

    /// NOTE In case when field already has string type, more direct assign is possible.
    void assignString(const char * data, size_t size)
    {
        destroy();
        create(data, size);
    }

    void assignString(const unsigned char * data, size_t size)
    {
        destroy();
        create(data, size);
    }

    Field & operator=(const Field & rhs)
    {
        if (this != &rhs)
        {
            if (which != rhs.which)
            {
                destroy();
                create(rhs);
            }
            else
                assign(rhs); /// This assigns string or vector without deallocation of existing buffer.
        }
        return *this;
    }

    template <class T>
    Field & operator=(T && rhs) // NOLINT(misc-unconventional-assign-operator) there is still a false-positive here
    {
        if constexpr (std::is_same_v<std::decay_t<T>, Field>)
        {
            if (this != &rhs)
            {
                if (which != rhs.which)
                {
                    destroy();
                    create(std::forward<T>(rhs));
                }
                else
                    assign(std::forward<T>(rhs));
            }
        }
        else
        {
            if (which != TypeToEnum<std::decay_t<T>>::value)
            {
                destroy();
                createConcrete(std::forward<T>(rhs));
            }
            else
                assignConcrete(std::forward<T>(rhs));
        }

        return *this;
    }

    ~Field() { destroy(); }


    Types::Which getType() const { return which; }
    const char * getTypeName() const { return Types::toString(which); }
    String toString() const;

    bool isNull() const { return which == Types::Null; }


    template <typename T>
    T & get()
    {
        using TWithoutRef = std::remove_reference_t<T>;
        auto * MAY_ALIAS ptr = reinterpret_cast<TWithoutRef *>(&storage);
        return *ptr;
    };

    template <typename T>
    const T & get() const
    {
        using TWithoutRef = std::remove_reference_t<T>;
        const auto * MAY_ALIAS ptr = reinterpret_cast<const TWithoutRef *>(&storage);
        return *ptr;
    };

    template <typename T>
    bool tryGet(T & result)
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T>
    bool tryGet(T & result) const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T>
    T & safeGet()
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception(
                "Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)),
                ErrorCodes::BAD_GET);
        return get<T>();
    }

    template <typename T>
    const T & safeGet() const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception(
                "Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)),
                ErrorCodes::BAD_GET);
        return get<T>();
    }


    bool operator<(const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
        case Types::Null:
            return false;
        case Types::UInt64:
            return get<UInt64>() < rhs.get<UInt64>();
        case Types::UInt128:
            return get<UInt128>() < rhs.get<UInt128>();
        case Types::Int64:
            return get<Int64>() < rhs.get<Int64>();
        case Types::Float64:
            return get<Float64>() < rhs.get<Float64>();
        case Types::String:
            return get<String>() < rhs.get<String>();
        case Types::Array:
            return get<Array>() < rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() < rhs.get<Tuple>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() < rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() < rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256:
            return get<DecimalField<Decimal256>>() < rhs.get<DecimalField<Decimal256>>();

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    }

    bool operator>(const Field & rhs) const { return rhs < *this; }

    bool operator<=(const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
        case Types::Null:
            return true;
        case Types::UInt64:
            return get<UInt64>() <= rhs.get<UInt64>();
        case Types::UInt128:
            return get<UInt128>() <= rhs.get<UInt128>();
        case Types::Int64:
            return get<Int64>() <= rhs.get<Int64>();
        case Types::Float64:
            return get<Float64>() <= rhs.get<Float64>();
        case Types::String:
            return get<String>() <= rhs.get<String>();
        case Types::Array:
            return get<Array>() <= rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() <= rhs.get<Tuple>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() <= rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() <= rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256:
            return get<DecimalField<Decimal256>>() <= rhs.get<DecimalField<Decimal256>>();


        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    }

    bool operator>=(const Field & rhs) const { return rhs <= *this; }

    bool operator==(const Field & rhs) const
    {
        if (which != rhs.which)
            return false;

        switch (which)
        {
        case Types::Null:
            return true;
        case Types::UInt64:
        case Types::Int64:
        case Types::Float64:
            return get<UInt64>() == rhs.get<UInt64>();
        case Types::String:
            return get<String>() == rhs.get<String>();
        case Types::Array:
            return get<Array>() == rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() == rhs.get<Tuple>();
        case Types::UInt128:
            return get<UInt128>() == rhs.get<UInt128>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() == rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() == rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256:
            return get<DecimalField<Decimal256>>() == rhs.get<DecimalField<Decimal256>>();

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    }

    bool operator!=(const Field & rhs) const { return !(*this == rhs); }

private:
    std::aligned_union_t<
        DBMS_MIN_FIELD_SIZE - sizeof(Types::Which),
        Null,
        UInt64,
        UInt128,
        Int64,
        Float64,
        String,
        Array,
        Tuple,
        DecimalField<Decimal32>,
        DecimalField<Decimal64>,
        DecimalField<Decimal128>,
        DecimalField<Decimal256>>
        storage;

    Types::Which which;


    /// Assuming there was no allocated state or it was deallocated (see destroy).
#pragma GCC diagnostic push
#if defined(__clang__)
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#endif
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    template <typename T>
    void createConcrete(T && x)
    {
        using JustT = std::decay_t<T>;
        auto * MAY_ALIAS ptr = reinterpret_cast<JustT *>(&storage);
        new (ptr) JustT(std::forward<T>(x));
        which = TypeToEnum<JustT>::value;
    }

    /// Assuming same types.
    template <typename T>
    void assignConcrete(T && x)
    {
        using JustT = std::decay_t<T>;
        auto * MAY_ALIAS ptr = reinterpret_cast<JustT *>(&storage);
        *ptr = std::forward<T>(x);
    }


    template <typename F, typename Field> /// Field template parameter may be const or non-const Field.
    static void dispatch(F && f, Field & field)
    {
        switch (field.which)
        {
        case Types::Null:
            f(field.template get<Null>());
            return;
        case Types::UInt64:
            f(field.template get<UInt64>());
            return;
        case Types::UInt128:
            f(field.template get<UInt128>());
            return;
        case Types::Int64:
            f(field.template get<Int64>());
            return;
        case Types::Float64:
            f(field.template get<Float64>());
            return;
        case Types::String:
            f(field.template get<String>());
            return;
        case Types::Array:
            f(field.template get<Array>());
            return;
        case Types::Tuple:
            f(field.template get<Tuple>());
            return;
        case Types::Decimal32:
            f(field.template get<DecimalField<Decimal32>>());
            return;
        case Types::Decimal64:
            f(field.template get<DecimalField<Decimal64>>());
            return;
        case Types::Decimal128:
            f(field.template get<DecimalField<Decimal128>>());
            return;
        case Types::Decimal256:
            f(field.template get<DecimalField<Decimal256>>());
            return;

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    }

#pragma GCC diagnostic pop

    void create(const Field & x)
    {
        dispatch([this](auto & value) { createConcrete(value); }, x);
    }

    void create(Field && x)
    {
        dispatch([this](auto & value) { createConcrete(std::move(value)); }, x);
    }

    void assign(const Field & x)
    {
        dispatch([this](auto & value) { assignConcrete(value); }, x);
    }

    void assign(Field && x)
    {
        dispatch([this](auto & value) { assignConcrete(std::move(value)); }, x);
    }


    void create(const char * data, size_t size)
    {
        auto * MAY_ALIAS ptr = reinterpret_cast<String *>(&storage);
        new (ptr) String(data, size);
        which = Types::String;
    }

    void create(const unsigned char * data, size_t size) { create(reinterpret_cast<const char *>(data), size); }

    ALWAYS_INLINE void destroy()
    {
        if (which < Types::MIN_NON_POD)
            return;

        switch (which)
        {
        case Types::String:
            destroy<String>();
            break;
        case Types::Array:
            destroy<Array>();
            break;
        case Types::Tuple:
            destroy<Tuple>();
            break;
        default:
            break;
        }

        which = Types::Null; /// for exception safety in subsequent calls to destroy and create, when create fails.
    }

    template <typename T>
    void destroy()
    {
        T * MAY_ALIAS ptr = reinterpret_cast<T *>(&storage);
        ptr->~T();
    }
};

#undef DBMS_MIN_FIELD_SIZE


template <>
struct Field::TypeToEnum<Null>
{
    static const Types::Which value = Types::Null;
};
template <>
struct Field::TypeToEnum<UInt64>
{
    static const Types::Which value = Types::UInt64;
};
template <>
struct Field::TypeToEnum<UInt128>
{
    static const Types::Which value = Types::UInt128;
};
template <>
struct Field::TypeToEnum<Int128>
{
    static const Types::Which value = Types::Int128;
};
template <>
struct Field::TypeToEnum<Int256>
{
    static const Types::Which value = Types::Int256;
};
template <>
struct Field::TypeToEnum<Int64>
{
    static const Types::Which value = Types::Int64;
};
template <>
struct Field::TypeToEnum<Float64>
{
    static const Types::Which value = Types::Float64;
};
template <>
struct Field::TypeToEnum<String>
{
    static const Types::Which value = Types::String;
};
template <>
struct Field::TypeToEnum<Array>
{
    static const Types::Which value = Types::Array;
};
template <>
struct Field::TypeToEnum<Tuple>
{
    static const Types::Which value = Types::Tuple;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal32>>
{
    static const Types::Which value = Types::Decimal32;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal64>>
{
    static const Types::Which value = Types::Decimal64;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal128>>
{
    static const Types::Which value = Types::Decimal128;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal256>>
{
    static const Types::Which value = Types::Decimal256;
};

template <>
struct Field::EnumToType<Field::Types::Null>
{
    using Type = Null;
};
template <>
struct Field::EnumToType<Field::Types::UInt64>
{
    using Type = UInt64;
};
template <>
struct Field::EnumToType<Field::Types::UInt128>
{
    using Type = UInt128;
};
template <>
struct Field::EnumToType<Field::Types::Int64>
{
    using Type = Int64;
};
template <>
struct Field::EnumToType<Field::Types::Int128>
{
    using Type = Int128;
};
template <>
struct Field::EnumToType<Field::Types::Int256>
{
    using Type = Int256;
};
template <>
struct Field::EnumToType<Field::Types::Float64>
{
    using Type = Float64;
};
template <>
struct Field::EnumToType<Field::Types::String>
{
    using Type = String;
};
template <>
struct Field::EnumToType<Field::Types::Array>
{
    using Type = Array;
};
template <>
struct Field::EnumToType<Field::Types::Tuple>
{
    using Type = Tuple;
};
template <>
struct Field::EnumToType<Field::Types::Decimal32>
{
    using Type = DecimalField<Decimal32>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal64>
{
    using Type = DecimalField<Decimal64>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal128>
{
    using Type = DecimalField<Decimal128>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal256>
{
    using Type = DecimalField<Decimal256>;
};

template <typename T>
T get(const Field & field)
{
    return field.template get<T>();
}

template <typename T>
T get(Field & field)
{
    return field.template get<T>();
}

template <typename T>
T safeGet(const Field & field)
{
    return field.template safeGet<T>();
}

template <typename T>
T safeGet(Field & field)
{
    return field.template safeGet<T>();
}


template <>
struct TypeName<Array>
{
    static std::string get() { return "Array"; }
};
template <>
struct TypeName<Tuple>
{
    static std::string get() { return "Tuple"; }
};


template <typename T>
struct NearestFieldType;

template <>
struct NearestFieldType<UInt8>
{
    using Type = UInt64;
};
template <>
struct NearestFieldType<UInt16>
{
    using Type = UInt64;
};
template <>
struct NearestFieldType<UInt32>
{
    using Type = UInt64;
};
template <>
struct NearestFieldType<UInt64>
{
    using Type = UInt64;
};
template <>
struct NearestFieldType<UInt128>
{
    using Type = UInt128;
};
template <>
struct NearestFieldType<Int8>
{
    using Type = Int64;
};
template <>
struct NearestFieldType<Int16>
{
    using Type = Int64;
};
template <>
struct NearestFieldType<Int32>
{
    using Type = Int64;
};
template <>
struct NearestFieldType<Int64>
{
    using Type = Int64;
};
template <>
struct NearestFieldType<Float32>
{
    using Type = Float64;
};
template <>
struct NearestFieldType<Float64>
{
    using Type = Float64;
};
template <>
struct NearestFieldType<String>
{
    using Type = String;
};
template <>
struct NearestFieldType<Array>
{
    using Type = Array;
};
template <>
struct NearestFieldType<Tuple>
{
    using Type = Tuple;
};
template <>
struct NearestFieldType<bool>
{
    using Type = UInt64;
};
template <>
struct NearestFieldType<Null>
{
    using Type = Null;
};
template <>
struct NearestFieldType<DecimalField<Decimal32>>
{
    using Type = DecimalField<Decimal32>;
};
template <>
struct NearestFieldType<DecimalField<Decimal64>>
{
    using Type = DecimalField<Decimal64>;
};
template <>
struct NearestFieldType<DecimalField<Decimal128>>
{
    using Type = DecimalField<Decimal128>;
};
template <>
struct NearestFieldType<DecimalField<Decimal256>>
{
    using Type = DecimalField<Decimal256>;
};
template <>
struct NearestFieldType<Decimal32>
{
    using Type = DecimalField<Decimal32>;
};
template <>
struct NearestFieldType<Decimal64>
{
    using Type = DecimalField<Decimal64>;
};
template <>
struct NearestFieldType<Decimal128>
{
    using Type = DecimalField<Decimal128>;
};
template <>
struct NearestFieldType<Decimal256>
{
    using Type = DecimalField<Decimal256>;
};
template <>
struct NearestFieldType<Int128>
{
    using Type = Int128;
};
template <>
struct NearestFieldType<Int256>
{
    using Type = Int256;
};


template <typename T>
typename NearestFieldType<T>::Type nearestFieldType(const T & x)
{
    return typename NearestFieldType<T>::Type(x);
}


class ReadBuffer;
class WriteBuffer;

/// It is assumed that all elements of the array have the same type.
void readBinary(Array & x, ReadBuffer & buf);

inline void readText(Array &, ReadBuffer &)
{
    throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED);
}
inline void readQuoted(Array &, ReadBuffer &)
{
    throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED);
}

/// It is assumed that all elements of the array have the same type.
void writeBinary(const Array & x, WriteBuffer & buf);

void writeText(const Array & x, WriteBuffer & buf);

inline void writeQuoted(const Array &, WriteBuffer &)
{
    throw Exception("Cannot write Array quoted.", ErrorCodes::NOT_IMPLEMENTED);
}

void readBinary(Tuple & x, ReadBuffer & buf);

inline void readText(Tuple &, ReadBuffer &)
{
    throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED);
}
inline void readQuoted(Tuple &, ReadBuffer &)
{
    throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED);
}

void writeBinary(const Tuple & x, WriteBuffer & buf);

void writeText(const Tuple & x, WriteBuffer & buf);

inline void writeQuoted(const Tuple &, WriteBuffer &)
{
    throw Exception("Cannot write Tuple quoted.", ErrorCodes::NOT_IMPLEMENTED);
}
} // namespace DB
