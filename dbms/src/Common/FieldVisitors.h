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

#include <Core/AccurateComparison.h>
#include <Core/Field.h>
#include <common/DateLUT.h>
#include <common/demangle.h>


class SipHash;


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CONVERT_TYPE;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


/** StaticVisitor (and its descendants) - class with overloaded operator() for all types of fields.
  * You could call visitor for field using function 'applyVisitor'.
  * Also "binary visitor" is supported - its operator() takes two arguments.
  */
template <typename R = void>
struct StaticVisitor
{
    using ResultType = R;
};


/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
typename std::decay_t<Visitor>::ResultType applyVisitor(Visitor && visitor, F && field)
{
    switch (field.getType())
    {
    case Field::Types::Null:
        return visitor(field.template get<Null>());
    case Field::Types::UInt64:
        return visitor(field.template get<UInt64>());
    case Field::Types::Int64:
        return visitor(field.template get<Int64>());
    case Field::Types::Float64:
        return visitor(field.template get<Float64>());
    case Field::Types::String:
        return visitor(field.template get<String>());
    case Field::Types::Array:
        return visitor(field.template get<Array>());
    case Field::Types::Tuple:
        return visitor(field.template get<Tuple>());
    case Field::Types::Decimal32:
        return visitor(field.template get<DecimalField<Decimal32>>());
    case Field::Types::Decimal64:
        return visitor(field.template get<DecimalField<Decimal64>>());
    case Field::Types::Decimal128:
        return visitor(field.template get<DecimalField<Decimal128>>());
    case Field::Types::Decimal256:
        return visitor(field.template get<DecimalField<Decimal256>>());

    default:
        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


template <typename Visitor, typename F1, typename F2>
static typename std::decay_t<Visitor>::ResultType applyBinaryVisitorImpl(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field2.getType())
    {
    case Field::Types::Null:
        return visitor(field1, field2.template get<Null>());
    case Field::Types::UInt64:
        return visitor(field1, field2.template get<UInt64>());
    case Field::Types::Int64:
        return visitor(field1, field2.template get<Int64>());
    case Field::Types::Float64:
        return visitor(field1, field2.template get<Float64>());
    case Field::Types::String:
        return visitor(field1, field2.template get<String>());
    case Field::Types::Array:
        return visitor(field1, field2.template get<Array>());
    case Field::Types::Tuple:
        return visitor(field1, field2.template get<Tuple>());
    case Field::Types::Decimal32:
        return visitor(field1, field2.template get<DecimalField<Decimal32>>());
    case Field::Types::Decimal64:
        return visitor(field1, field2.template get<DecimalField<Decimal64>>());
    case Field::Types::Decimal128:
        return visitor(field1, field2.template get<DecimalField<Decimal128>>());
    case Field::Types::Decimal256:
        return visitor(field1, field2.template get<DecimalField<Decimal256>>());

    default:
        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}

template <typename Visitor, typename F1, typename F2>
typename std::decay_t<Visitor>::ResultType applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field1.getType())
    {
    case Field::Types::Null:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<Null>(),
            std::forward<F2>(field2));
    case Field::Types::UInt64:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<UInt64>(),
            std::forward<F2>(field2));
    case Field::Types::Int64:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<Int64>(),
            std::forward<F2>(field2));
    case Field::Types::Float64:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<Float64>(),
            std::forward<F2>(field2));
    case Field::Types::String:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<String>(),
            std::forward<F2>(field2));
    case Field::Types::Array:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<Array>(),
            std::forward<F2>(field2));
    case Field::Types::Tuple:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<Tuple>(),
            std::forward<F2>(field2));
    case Field::Types::Decimal32:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<DecimalField<Decimal32>>(),
            std::forward<F2>(field2));
    case Field::Types::Decimal64:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<DecimalField<Decimal64>>(),
            std::forward<F2>(field2));
    case Field::Types::Decimal128:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<DecimalField<Decimal128>>(),
            std::forward<F2>(field2));
    case Field::Types::Decimal256:
        return applyBinaryVisitorImpl(
            std::forward<Visitor>(visitor),
            field1.template get<DecimalField<Decimal256>>(),
            std::forward<F2>(field2));

    default:
        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


/** Prints Field as literal in SQL query */
class FieldVisitorToString : public StaticVisitor<String>
{
private:
    bool isDecimalWithQuoted;

public:
    FieldVisitorToString()
        : isDecimalWithQuoted(true){};

    FieldVisitorToString(bool val)
        : isDecimalWithQuoted(val){};

    String operator()(const Null & x) const;
    String operator()(const UInt64 & x) const;
    String operator()(const Int64 & x) const;
    String operator()(const Float64 & x) const;
    String operator()(const String & x) const;
    String operator()(const Array & x) const;
    String operator()(const Tuple & x) const;
    String operator()(const DecimalField<Decimal32> & x) const;
    String operator()(const DecimalField<Decimal64> & x) const;
    String operator()(const DecimalField<Decimal128> & x) const;
    String operator()(const DecimalField<Decimal256> & x) const;
};

/** Prints Field as literal in debug logging. The value will be converted to '?' if redact-log is on */
class FieldVisitorToDebugString : public StaticVisitor<String>
{
public:
    String operator()(const Null & x) const;
    String operator()(const UInt64 & x) const;
    String operator()(const Int64 & x) const;
    String operator()(const Float64 & x) const;
    String operator()(const String & x) const;
    String operator()(const Array & x) const;
    String operator()(const Tuple & x) const;
    String operator()(const DecimalField<Decimal32> & x) const;
    String operator()(const DecimalField<Decimal64> & x) const;
    String operator()(const DecimalField<Decimal128> & x) const;
    String operator()(const DecimalField<Decimal256> & x) const;
};


/** Print readable and unique text dump of field type and value. */
class FieldVisitorDump : public StaticVisitor<String>
{
public:
    String operator()(const Null & x) const;
    String operator()(const UInt64 & x) const;
    String operator()(const Int64 & x) const;
    String operator()(const Float64 & x) const;
    String operator()(const String & x) const;
    String operator()(const Array & x) const;
    String operator()(const Tuple & x) const;
    String operator()(const DecimalField<Decimal32> & x) const;
    String operator()(const DecimalField<Decimal64> & x) const;
    String operator()(const DecimalField<Decimal128> & x) const;
    String operator()(const DecimalField<Decimal256> & x) const;
};


/** Converts numberic value of any type to specified type. */
template <typename T>
class FieldVisitorConvertToNumber : public StaticVisitor<T>
{
public:
    T operator()(const Null &) const
    {
        throw Exception("Cannot convert NULL to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator()(const String &) const
    {
        throw Exception("Cannot convert String to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator()(const Array &) const
    {
        throw Exception("Cannot convert Array to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator()(const Tuple &) const
    {
        throw Exception("Cannot convert Tuple to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator()(const UInt64 & x) const { return x; }
    T operator()(const Int64 & x) const { return x; }
    T operator()(const Float64 & x) const { return x; }
    template <typename U>
    T operator()(const DecimalField<U> & x) const
    {
        return static_cast<T>(x.getValue().value);
    }
};


/** Updates SipHash by type and value of Field */
class FieldVisitorHash : public StaticVisitor<>
{
private:
    SipHash & hash;

public:
    explicit FieldVisitorHash(SipHash & hash);

    void operator()(const Null & x) const;
    void operator()(const UInt64 & x) const;
    void operator()(const Int64 & x) const;
    void operator()(const Float64 & x) const;
    void operator()(const String & x) const;
    void operator()(const Array & x) const;
    void operator()(const DecimalField<Decimal32> & x) const;
    void operator()(const DecimalField<Decimal64> & x) const;
    void operator()(const DecimalField<Decimal128> & x) const;
    void operator()(const DecimalField<Decimal256> & x) const;
};

template <typename T>
constexpr bool isDecimalField()
{
    return false;
}
template <>
constexpr bool isDecimalField<DecimalField<Decimal32>>()
{
    return true;
}
template <>
constexpr bool isDecimalField<DecimalField<Decimal64>>()
{
    return true;
}
template <>
constexpr bool isDecimalField<DecimalField<Decimal128>>()
{
    return true;
}
template <>
constexpr bool isDecimalField<DecimalField<Decimal256>>()
{
    return true;
}

/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    bool operator()(const Null &, const Null &) const { return true; }
    bool operator()(const Null &, const UInt64 &) const { return false; }
    bool operator()(const Null &, const Int64 &) const { return false; }
    bool operator()(const Null &, const Float64 &) const { return false; }
    bool operator()(const Null &, const String &) const { return false; }
    bool operator()(const Null &, const Array &) const { return false; }
    bool operator()(const Null &, const Tuple &) const { return false; }

    bool operator()(const UInt64 &, const Null &) const { return false; }
    bool operator()(const UInt64 & l, const UInt64 & r) const { return l == r; }
    bool operator()(const UInt64 & l, const Int64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const UInt64 & l, const Float64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const UInt64 &, const String &) const { return false; }
    bool operator()(const UInt64 &, const Array &) const { return false; }
    bool operator()(const UInt64 &, const Tuple &) const { return false; }

    bool operator()(const Int64 &, const Null &) const { return false; }
    bool operator()(const Int64 & l, const UInt64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const Int64 & l, const Int64 & r) const { return l == r; }
    bool operator()(const Int64 & l, const Float64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const Int64 &, const String &) const { return false; }
    bool operator()(const Int64 &, const Array &) const { return false; }
    bool operator()(const Int64 &, const Tuple &) const { return false; }

    bool operator()(const Float64 &, const Null &) const { return false; }
    bool operator()(const Float64 & l, const UInt64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const Float64 & l, const Int64 & r) const { return accurate::equalsOp(l, r); }
    bool operator()(const Float64 & l, const Float64 & r) const { return l == r; }
    bool operator()(const Float64 &, const String &) const { return false; }
    bool operator()(const Float64 &, const Array &) const { return false; }
    bool operator()(const Float64 &, const Tuple &) const { return false; }

    bool operator()(const String &, const Null &) const { return false; }
    bool operator()(const String &, const UInt64 &) const { return false; }
    bool operator()(const String &, const Int64 &) const { return false; }
    bool operator()(const String &, const Float64 &) const { return false; }
    bool operator()(const String & l, const String & r) const { return l == r; }
    bool operator()(const String &, const Array &) const { return false; }
    bool operator()(const String &, const Tuple &) const { return false; }

    bool operator()(const Array &, const Null &) const { return false; }
    bool operator()(const Array &, const UInt64 &) const { return false; }
    bool operator()(const Array &, const Int64 &) const { return false; }
    bool operator()(const Array &, const Float64 &) const { return false; }
    bool operator()(const Array &, const String &) const { return false; }
    bool operator()(const Array & l, const Array & r) const { return l == r; }
    bool operator()(const Array &, const Tuple &) const { return false; }

    bool operator()(const Tuple &, const Null &) const { return false; }
    bool operator()(const Tuple &, const UInt64 &) const { return false; }
    bool operator()(const Tuple &, const Int64 &) const { return false; }
    bool operator()(const Tuple &, const Float64 &) const { return false; }
    bool operator()(const Tuple &, const String &) const { return false; }
    bool operator()(const Tuple &, const Array &) const { return false; }
    bool operator()(const Tuple & l, const Tuple & r) const { return l == r; }

    template <typename T>
    bool operator()(const Null &, const T &) const
    {
        return std::is_same_v<T, Null>;
    }

    template <typename T>
    bool operator()(const String & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, String>)
            return l == r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const UInt128 & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, UInt128>)
            return l == r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const Array & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Array>)
            return l == r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const Tuple & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Tuple>)
            return l == r;
        return cantCompare(l, r);
    }

    template <typename T, typename U>
    bool operator()(const DecimalField<T> & l, const U & r) const
    {
        if constexpr (isDecimalField<U>())
            return l == r;
        if constexpr (std::is_same_v<U, Int64> || std::is_same_v<U, UInt64>)
            return l == DecimalField<Decimal256>(static_cast<Int256>(r), 0);
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const UInt64 & l, const DecimalField<T> & r) const
    {
        return DecimalField<Decimal256>(static_cast<Int256>(l), 0) == r;
    }
    template <typename T>
    bool operator()(const Int64 & l, const DecimalField<T> & r) const
    {
        return DecimalField<Decimal256>(static_cast<Int256>(l), 0) == r;
    }
    template <typename T>
    bool operator()(const Float64 & l, const DecimalField<T> & r) const
    {
        return cantCompare(l, r);
    }

private:
    template <typename T, typename U>
    bool cantCompare(const T &, const U &) const
    {
        if constexpr (std::is_same_v<U, Null>)
            return false;
        throw Exception(
            "Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    bool operator()(const Null &, const Null &) const { return false; }
    bool operator()(const Null &, const UInt64 &) const { return true; }
    bool operator()(const Null &, const Int64 &) const { return true; }
    bool operator()(const Null &, const Float64 &) const { return true; }
    bool operator()(const Null &, const String &) const { return true; }
    bool operator()(const Null &, const Array &) const { return true; }
    bool operator()(const Null &, const Tuple &) const { return true; }

    bool operator()(const UInt64 &, const Null &) const { return false; }
    bool operator()(const UInt64 & l, const UInt64 & r) const { return l < r; }
    bool operator()(const UInt64 & l, const Int64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const UInt64 & l, const Float64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const UInt64 &, const String &) const { return true; }
    bool operator()(const UInt64 &, const Array &) const { return true; }
    bool operator()(const UInt64 &, const Tuple &) const { return true; }

    bool operator()(const Int64 &, const Null &) const { return false; }
    bool operator()(const Int64 & l, const UInt64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const Int64 & l, const Int64 & r) const { return l < r; }
    bool operator()(const Int64 & l, const Float64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const Int64 &, const String &) const { return true; }
    bool operator()(const Int64 &, const Array &) const { return true; }
    bool operator()(const Int64 &, const Tuple &) const { return true; }

    bool operator()(const Float64 &, const Null &) const { return false; }
    bool operator()(const Float64 & l, const UInt64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const Float64 & l, const Int64 & r) const { return accurate::lessOp(l, r); }
    bool operator()(const Float64 & l, const Float64 & r) const { return l < r; }
    bool operator()(const Float64 &, const String &) const { return true; }
    bool operator()(const Float64 &, const Array &) const { return true; }
    bool operator()(const Float64 &, const Tuple &) const { return true; }

    bool operator()(const String &, const Null &) const { return false; }
    bool operator()(const String &, const UInt64 &) const { return false; }
    bool operator()(const String &, const Int64 &) const { return false; }
    bool operator()(const String &, const Float64 &) const { return false; }
    bool operator()(const String & l, const String & r) const { return l < r; }
    bool operator()(const String &, const Array &) const { return true; }
    bool operator()(const String &, const Tuple &) const { return true; }

    bool operator()(const Array &, const Null &) const { return false; }
    bool operator()(const Array &, const UInt64 &) const { return false; }
    bool operator()(const Array &, const Int64 &) const { return false; }
    bool operator()(const Array &, const Float64 &) const { return false; }
    bool operator()(const Array &, const String &) const { return false; }
    bool operator()(const Array & l, const Array & r) const { return l < r; }
    bool operator()(const Array &, const Tuple &) const { return false; }

    bool operator()(const Tuple &, const Null &) const { return false; }
    bool operator()(const Tuple &, const UInt64 &) const { return false; }
    bool operator()(const Tuple &, const Int64 &) const { return false; }
    bool operator()(const Tuple &, const Float64 &) const { return false; }
    bool operator()(const Tuple &, const String &) const { return false; }
    bool operator()(const Tuple &, const Array &) const { return false; }
    bool operator()(const Tuple & l, const Tuple & r) const { return l < r; }

    template <typename T>
    bool operator()(const Null &, const T &) const
    {
        return !std::is_same_v<T, Null>;
    }

    template <typename T>
    bool operator()(const String & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, String>)
            return l < r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const UInt128 & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, UInt128>)
            return l < r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const Array & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Array>)
            return l < r;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const Tuple & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Tuple>)
            return l < r;
        return cantCompare(l, r);
    }

    template <typename T, typename U>
    bool operator()(const DecimalField<T> & l, const U & r) const
    {
        if constexpr (isDecimalField<U>())
            return l < r;
        if constexpr (std::is_same_v<U, Int64> || std::is_same_v<U, UInt64>)
            return l < DecimalField<Decimal256>(static_cast<Int256>(r), 0);
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator()(const UInt64 & l, const DecimalField<T> & r) const
    {
        return DecimalField<Decimal256>(static_cast<Int256>(l), 0) < r;
    }
    template <typename T>
    bool operator()(const Int64 & l, const DecimalField<T> & r) const
    {
        return DecimalField<Decimal256>(static_cast<Int256>(l), 0) < r;
    }
    template <typename T>
    bool operator()(const Float64 & l, const DecimalField<T> & r) const
    {
        return cantCompare(l, r);
    }

private:
    template <typename T, typename U>
    bool cantCompare(const T &, const U &) const
    {
        throw Exception(
            "Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};

/** Implements `+=` operation.
 *  Returns false if the result is zero.
 */
class FieldVisitorSum : public StaticVisitor<bool>
{
private:
    const Field & rhs;

public:
    explicit FieldVisitorSum(const Field & rhs_)
        : rhs(rhs_)
    {}

    bool operator()(UInt64 & x) const
    {
        x += get<UInt64>(rhs);
        return x != 0;
    }
    bool operator()(Int64 & x) const
    {
        x += get<Int64>(rhs);
        return x != 0;
    }
    bool operator()(Float64 & x) const
    {
        x += get<Float64>(rhs);
        return x != 0;
    }
    template <typename T>
    bool operator()(DecimalField<T> & x) const
    {
        x += get<DecimalField<T>>(rhs);
        return x.getValue().value != 0;
    }

    bool operator()(Null &) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator()(String &) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
    bool operator()(Array &) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
};

} // namespace DB
