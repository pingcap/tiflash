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

#include <Columns/ColumnDecimal.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <fmt/core.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
}

// Implements Decimal(P, S), where P is precision (significant digits), and S is scale (digits following the decimal point).
// For example, Decimal(5, 2) can represent numbers from -999.99 to 999.99
// Maximum precisions for underlying types are:
// Int32 9
// Int64 18
// Int128 38
// Int256 65

template <typename T>
class DataTypeDecimal final : public IDataType
{
    static_assert(IsDecimal<T>);

private:
    PrecType precision;
    ScaleType scale;

public:
    using FieldType = T;

    using ColumnType = ColumnDecimal<T>;

    static constexpr bool is_parametric = true;

    static constexpr bool is_Decimal256 = std::is_same_v<T, Decimal256>;

    static constexpr size_t maxPrecision() { return maxDecimalPrecision<T>(); }

    // default values
    DataTypeDecimal()
        : precision(10)
        , scale(0)
    {}

    DataTypeDecimal(size_t precision_, size_t scale_)
        : precision(precision_)
        , scale(scale_)
    {
        if (precision > decimal_max_prec || scale > precision || scale > decimal_max_scale)
        {
            std::string msg = getName() + "is out of bound";
            if (precision > decimal_max_prec)
            {
                msg = fmt::format(
                    "{}, precision {} is greater than maximum value {}",
                    msg,
                    precision,
                    decimal_max_prec);
            }
            else if (scale > precision)
            {
                msg = fmt::format("{}, scale {} is greater than precision {}", msg, scale, precision);
            }
            else
            {
                msg = fmt::format("{}, scale {} is greater than maximum value {}", msg, scale, decimal_max_scale);
            }
            throw Exception(msg, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
    }

    T getScaleMultiplier(UInt32 scale_) const;

    template <typename U1, typename U2>
    std::tuple<T, T, T> getScales(const U1 & left, const U2 & right, bool is_mul, bool is_div) const
    {
        ScaleType left_scale = 0, right_scale = 0;
        if constexpr (IsDecimal<typename U1::FieldType>)
        {
            left_scale = left.getScale();
        }
        if constexpr (IsDecimal<typename U2::FieldType>)
        {
            right_scale = right.getScale();
        }
        if (is_mul)
        {
            ScaleType result_scale = left_scale + right_scale - scale;
            return std::make_tuple(T(1), T(1), getScaleMultiplier(result_scale));
        }
        else if (is_div)
        {
            ScaleType left_result_scale = right_scale - left_scale + scale;
            return std::make_tuple(getScaleMultiplier(left_result_scale), T(1), T(1));
        }
        else
        {
            ScaleType left_result_scale = scale - left_scale;
            ScaleType right_result_scale = scale - right_scale;
            return std::make_tuple(getScaleMultiplier(left_result_scale), getScaleMultiplier(right_result_scale), T(1));
        }
    }

    std::string getName() const override;

    const char * getFamilyName() const override { return "Decimal"; }

    TypeIndex getTypeId() const override { return TypeId<T>::value; }

    PrecType getPrec() const { return precision; }

    ScaleType getScale() const { return scale; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint)
        const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &)
        const override;

    void readText(T & x, ReadBuffer & istr) const;

    MutableColumnPtr createColumn() const override;

    T parseFromString(const String & str) const;

    Field getDefault() const override { return DecimalField(T(0), scale); }

    template <typename U>
    typename T::NativeType scaleFactorFor(const DataTypeDecimal<U> & x) const
    {
        if (getScale() < x.getScale())
        {
            return 1;
        }
        ScaleType delta = getScale() - x.getScale();
        return getScaleMultiplier(delta);
    }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; };
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override { return scale == 0; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return !is_Decimal256; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(T); }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool isDecimal() const override { return true; }
};

using DataTypeDecimal32 = DataTypeDecimal<Decimal32>;
using DataTypeDecimal64 = DataTypeDecimal<Decimal64>;
using DataTypeDecimal128 = DataTypeDecimal<Decimal128>;
using DataTypeDecimal256 = DataTypeDecimal<Decimal256>;

inline DataTypePtr createDecimal(UInt64 prec, UInt64 scale)
{
    if (prec < minDecimalPrecision<Decimal32>() || prec > maxDecimalPrecision<Decimal256>())
        throw Exception("Wrong precision:" + DB::toString(prec), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(scale) > prec)
        throw Exception(
            "Negative scales and scales larger than precision are not supported. precision:" + DB::toString(prec)
                + ", scale:" + DB::toString(scale),
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (prec <= maxDecimalPrecision<Decimal32>())
    {
        return (std::make_shared<DataTypeDecimal32>(prec, scale));
    }
    if (prec <= maxDecimalPrecision<Decimal64>())
    {
        return (std::make_shared<DataTypeDecimal64>(prec, scale));
    }
    if (prec <= maxDecimalPrecision<Decimal128>())
    {
        return (std::make_shared<DataTypeDecimal128>(prec, scale));
    }
    return std::make_shared<DataTypeDecimal256>(prec, scale);
}

template <typename T>
inline const DataTypeDecimal<T> * checkDecimal(const IDataType & data_type)
{
    return typeid_cast<const DataTypeDecimal<T> *>(&data_type);
}

inline bool IsDecimalDataType(const DataTypePtr & type)
{
    return checkDecimal<Decimal32>(*type) || checkDecimal<Decimal64>(*type) || checkDecimal<Decimal128>(*type)
        || checkDecimal<Decimal256>(*type);
}
template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), const DataTypeDecimal<T>> decimalResultType(
    const DataTypeDecimal<T> & tx,
    const DataTypeDecimal<U> & ty)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    return DataTypeDecimal<T>(maxDecimalPrecision<T>(), scale);
}

template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DataTypeDecimal<U>> decimalResultType(
    const DataTypeDecimal<T> & tx,
    const DataTypeDecimal<U> & ty)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    return DataTypeDecimal<U>(maxDecimalPrecision<U>(), scale);
}

inline PrecType getDecimalPrecision(const IDataType & data_type, PrecType default_value)
{
    if (const auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getPrec();
    if (const auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getPrec();
    if (const auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getPrec();
    if (const auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getPrec();
    return default_value;
}

inline ScaleType getDecimalScale(const IDataType & data_type, ScaleType default_value)
{
    if (const auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getScale();
    return default_value;
}

inline UInt32 leastDecimalPrecisionFor(TypeIndex int_type)
{
    switch (int_type)
    {
    case TypeIndex::Int8:
        [[fallthrough]];
    case TypeIndex::UInt8:
        return 3;
    case TypeIndex::Int16:
        [[fallthrough]];
    case TypeIndex::UInt16:
        return 5;
    case TypeIndex::Int32:
        [[fallthrough]];
    case TypeIndex::UInt32:
        return 10;
    case TypeIndex::Int64:
        return 19;
    case TypeIndex::UInt64:
        return 20;
    default:
        break;
    }
    return 0;
}

} // namespace DB
