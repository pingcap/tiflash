// Copyright 2026 PingCAP, Inc.
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

#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDecimal.h>

namespace DB::DM
{

template <typename T, bool scale_column>
struct PreparedDecimalComparison
{
    T value;
    T column_multiplier;
    int constant_overflow_sign;

    template <template <typename, typename> class Op, typename Right>
    ALWAYS_INLINE bool compare(const Right & right) const
    {
        // A constant that overflowed during alignment is outside the range of every column value.
        if (unlikely(constant_overflow_sign != 0))
            return Op<int, int>::apply(constant_overflow_sign, 0);

        T column_value = static_cast<T>(right);
        if constexpr (scale_column)
        {
            const int sign = boost::math::sign(column_value);
            if (unlikely(common::mulOverflow(column_value, column_multiplier, column_value)))
                return Op<int, int>::apply(0, sign);
        }
        return Op<T, T>::apply(value, column_value);
    }
};

namespace details
{
template <typename Left, typename Right, typename Func>
auto prepareDecimalComparison(const DecimalField<Left> & value, ScaleType column_scale, const Func & func)
{
    const auto value_scale = value.getScale();
    if (value_scale > maxDecimalPrecision<Decimal256>() || column_scale > maxDecimalPrecision<Decimal256>())
        throw Exception("Bad scale of decimal field", ErrorCodes::DECIMAL_OVERFLOW);

    using CompareInt = typename DecCompareInt<Left, Right>::Type;
    CompareInt constant = static_cast<CompareInt>(value.getValue());
    if (value_scale > column_scale)
    {
        const auto multiplier = getScaleMultiplier<CompareInt>(value_scale - column_scale);
        return func(PreparedDecimalComparison<CompareInt, true>{constant, multiplier, 0});
    }

    int constant_overflow_sign = 0;
    if (value_scale < column_scale)
    {
        const auto multiplier = getScaleMultiplier<CompareInt>(column_scale - value_scale);
        const int sign = boost::math::sign(constant);
        if (common::mulOverflow(constant, multiplier, constant))
            constant_overflow_sign = sign;
    }
    return func(PreparedDecimalComparison<CompareInt, false>{constant, 1, constant_overflow_sign});
}

// Dispatch the constant and prepare scale alignment once, before entering the pack loop.
// Each callback instantiation has a fixed integer type and a compile-time scaling decision.
template <typename Right, typename Func>
auto withPreparedDecimalComparison(const Field & value, const DataTypePtr & type, const Func & func)
{
    if constexpr (IsDecimal<Right>)
    {
        const auto * decimal_type = checkDecimal<Right>(*type);
        if (decimal_type)
        {
            const auto column_scale = decimal_type->getScale();
            switch (value.getType())
            {
            case Field::Types::Which::Decimal32:
                return prepareDecimalComparison<Decimal32, Right>(
                    value.safeGet<DecimalField<Decimal32>>(),
                    column_scale,
                    func);
            case Field::Types::Which::Decimal64:
                return prepareDecimalComparison<Decimal64, Right>(
                    value.safeGet<DecimalField<Decimal64>>(),
                    column_scale,
                    func);
            case Field::Types::Which::Decimal128:
                return prepareDecimalComparison<Decimal128, Right>(
                    value.safeGet<DecimalField<Decimal128>>(),
                    column_scale,
                    func);
            case Field::Types::Which::Decimal256:
                return prepareDecimalComparison<Decimal256, Right>(
                    value.safeGet<DecimalField<Decimal256>>(),
                    column_scale,
                    func);
            default:
                break;
            }
        }
    }
    // Preserve conservative handling of unsupported comparisons and the non-decimal path.
    return func(value);
}
} // namespace details
} // namespace DB::DM
