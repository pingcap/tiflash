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

// A comparison between a decimal constant and column min/max values, prepared once.
// Decimals store an unscaled integer and a scale: e.g. -4.001 at scale 3 is stored as -4001.
// Align operands by scaling up the one with the smaller scale. The pack loop only scales
// column values when needed; constant scaling and integer dispatch have already been resolved.
//
// Keep scale guards, alignment direction and overflow ordering consistent with
// DecimalComparison::compare/apply in Core/DecimalComparison.h. A mismatch could return
// None incorrectly and skip a pack containing matching rows.
template <typename T, bool scale_column>
struct PreparedDecimalComparison
{
    // The constant's integer, aligned to the column's scale when scale_column is false.
    // If that alignment overflowed, use constant_overflow_sign instead.
    T value;
    // 10^(constant scale - column scale), used only when scale_column is true.
    T column_multiplier;
    // The constant's sign if alignment overflowed, or zero otherwise.
    int constant_overflow_sign;

    template <template <typename, typename> class Op, typename Right>
    ALWAYS_INLINE bool compare(const Right & right) const
    {
        // An overflowing constant lies outside every column value's range. Compare its sign
        // against zero to preserve ordering, equivalent to Op(0, -sign) in DecimalComparison::apply.
        if (unlikely(constant_overflow_sign != 0))
            return Op<int, int>::apply(constant_overflow_sign, 0);

        T column_value = static_cast<T>(right);
        if constexpr (scale_column)
        {
            // Align this column value to the constant's scale. On overflow, its sign determines
            // the ordering relative to the constant. Read the sign from the original right operand,
            // since mulOverflow overwrites column_value.
            if (unlikely(common::mulOverflow(column_value, column_multiplier, column_value)))
                return Op<int, int>::apply(0, boost::math::sign(static_cast<T>(right)));
        }
        return Op<T, T>::apply(value, column_value);
    }
};

namespace details
{
// Prepare scale alignment once, then invoke func with the chosen integer type and
// scale_column specialization. The callback lets the pack loop specialize both at compile time.
//
// Match DecimalComparison's rule: scale up the operand with the smaller scale.
// If the constant has the larger scale, prepare a multiplier for column min/max values.
// Otherwise align the single constant here, leaving the pack loop free of scaling multiplications.
template <typename Left, typename Right, typename Func>
auto prepareDecimalComparison(const DecimalField<Left> & value, ScaleType column_scale, const Func & func)
{
    const auto value_scale = value.getScale();
    // Use the same scale limit as DecimalComparison::compare.
    if (value_scale > maxDecimalPrecision<Decimal256>() || column_scale > maxDecimalPrecision<Decimal256>())
        throw Exception("Bad scale of decimal field", ErrorCodes::DECIMAL_OVERFLOW);

    // Use the wider operand's integer type so the original operands convert without truncation.
    // Scaling can still overflow this type and must retain the overflow handling below.
    using CompareInt = typename DecCompareInt<Left, Right>::Type;
    // The literal's unscaled integer at its original scale.
    CompareInt constant = static_cast<CompareInt>(value.getValue());
    if (value_scale > column_scale)
    {
        // Keep the constant at its own scale and align each column value inside compare().
        const auto multiplier = getScaleMultiplier<CompareInt>(value_scale - column_scale);
        return func(PreparedDecimalComparison<CompareInt, true>{constant, multiplier, 0});
    }

    // Align the single constant now so compare() does not multiply column values.
    int constant_overflow_sign = 0;
    if (value_scale < column_scale)
    {
        const auto multiplier = getScaleMultiplier<CompareInt>(column_scale - value_scale);
        // mulOverflow overwrites constant; recover the original sign only on overflow.
        // An overflowing constant is beyond every column value, so its sign preserves ordering.
        if (common::mulOverflow(constant, multiplier, constant))
            constant_overflow_sign = boost::math::sign(static_cast<CompareInt>(value.getValue()));
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
