// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Decimal.h>
#include <Core/Field.h>

namespace DB
{
template <typename T>
String Decimal<T>::toString(ScaleType scale) const
{
    PrecType precision = maxDecimalPrecision<Decimal<T>>();
    char str[decimal_max_prec + 5];
    size_t len = precision;
    if (value < 0)
    { // extra space for sign
        len++;
    }
    if (scale > 0)
    { // for factional point
        len++;
    }
    if (scale == precision)
    { // for leading zero
        len++;
    }
    size_t end_point = len;
    Int256 cur_v = value;
    if (value < 0)
    {
        cur_v = -cur_v;
    }
    if (scale > 0)
    {
        for (size_t i = 0; i < scale; i++)
        {
            int d = static_cast<int>(cur_v % 10);
            cur_v = cur_v / 10;
            str[--len] = d + '0';
        }
        str[--len] = '.';
    }
    do
    {
        int d = static_cast<int>(cur_v % 10);
        cur_v = cur_v / 10;
        str[--len] = d + '0';
    } while (cur_v > 0);
    if (value < 0)
    {
        str[--len] = '-';
    }
    return std::string(str + len, end_point - len);
}

inline std::optional<std::tuple<Int256, PrecType, ScaleType>> parseDecimal(const char * str, size_t len, bool negative)
{
    PrecType prec = 0;
    ScaleType scale = 0;
    Int256 value = 0; // Int256 is ok for 65 digits number at most.
    bool frac = false;

    for (size_t i = 0; i < len; ++i)
    {
        char c = str[i];
        if (c == '.')
        {
            if (frac || i == 0)
                return std::nullopt;
            frac = true;
        }
        else if (c <= '9' && c >= '0')
        {
            value = value * 10 + int(c - '0');
            if (frac)
                ++scale;
            if (frac || value > 0)
                ++prec;
            if (prec > decimal_max_prec || scale > decimal_max_scale)
                return std::nullopt;
        }
        else
            return std::nullopt;
    }

    if (prec == 0)
        ++prec;
    if (negative)
        value = -value;

    return std::make_tuple(value, prec, scale);
}

std::optional<std::tuple<Int256, PrecType, ScaleType>> parseDecimal(const char * str, size_t len)
{
    bool negative = false;

    // note: we only check the first character, so "+-" and "-+" are invalid.
    if (len > 0)
    {
        if (*str == '-')
        {
            negative = true;
            ++str;
            --len;
        }
        else if (*str == '+')
        {
            //  ignore plus sign. e.g. "+10000" = "10000".
            ++str;
            --len;
        }
    }

    return parseDecimal(str, len, negative);
}

bool parseDecimal(const char * str, size_t len, bool negative, Field & field)
{
    auto parsed_result = parseDecimal(str, len, negative);
    if (!parsed_result.has_value())
        return false;

    auto [value, prec, scale] = parsed_result.value();

    if (prec <= maxDecimalPrecision<Decimal32>())
        field = DecimalField<Decimal32>(static_cast<Int32>(value), scale);
    else if (prec <= maxDecimalPrecision<Decimal64>())
        field = DecimalField<Decimal64>(static_cast<Int64>(value), scale);
    else if (prec <= maxDecimalPrecision<Decimal128>())
        field = DecimalField<Decimal128>(static_cast<Int128>(value), scale);
    else if (prec <= maxDecimalPrecision<Decimal256>())
        field = DecimalField<Decimal256>(value, scale);
    else
    {
        // This branch expect to be dead code. Cause if prec > decimal_max_prec,
        // it will return false in for-loop
        throw TiFlashException("Decimal Overflow", Errors::Decimal::Overflow);
    }

    return true;
}

template struct Decimal<Int32>;
template struct Decimal<Int64>;
template struct Decimal<Int128>;
template struct Decimal<Int256>;


std::tuple<PrecType, ScaleType> PlusDecimalInferer::infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale)
{
    ScaleType result_scale = std::max(left_scale, right_scale);
    PrecType result_int = std::max(left_prec - left_scale, right_prec - right_scale);
    PrecType result_prec = std::min(result_scale + result_int + 1, decimal_max_prec);
    return {result_prec, result_scale};
}
std::tuple<PrecType, ScaleType> MulDecimalInferer::infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale)
{
    return {std::min(left_prec + right_prec, decimal_max_prec), std::min(left_scale + right_scale, decimal_max_scale)};
}

std::tuple<PrecType, ScaleType> DivDecimalInferer::infer(PrecType left_prec, ScaleType left_scale, PrecType /* right_prec is not used */, ScaleType right_scale)
{
    return {
        std::min(left_prec + right_scale + div_precincrement, decimal_max_prec),
        std::min(left_scale + div_precincrement, decimal_max_scale)};
}

std::tuple<PrecType, ScaleType> SumDecimalInferer::infer(PrecType prec, ScaleType scale)
{
    return {std::min(prec + decimal_longlong_digits, decimal_max_prec), scale};
}

std::tuple<PrecType, ScaleType> AvgDecimalInferer::infer(PrecType left_prec, ScaleType left_scale)
{
    return {std::min(left_prec + div_precincrement, decimal_max_prec), std::min(left_scale + div_precincrement, decimal_max_scale)};
}

std::tuple<PrecType, ScaleType> ModDecimalInferer::infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale)
{
    return {std::max(left_prec, right_prec), std::max(left_scale, right_scale)};
}

std::tuple<PrecType, ScaleType> OtherInferer::infer(PrecType, ScaleType, PrecType, ScaleType)
{
    return {};
}

Int256 DecimalMaxValue::get(PrecType idx)
{
    // In case DecimalMaxValue::get(IntPrec<Int256>::prec), where IntPrec<Int256>::prec > 65.
    assert(idx <= decimal_max_prec);
    return instance().getInternal(idx);
}

Int256 DecimalMaxValue::maxValue()
{
    return get(maxDecimalPrecision<Decimal256>());
}

DecimalMaxValue::DecimalMaxValue()
{
    for (PrecType i = 1; i <= decimal_max_prec; i++)
    {
        number[i] = number[i - 1] * 10 + 9;
    }
}

Int256 DecimalMaxValue::getInternal(PrecType idx) const
{
    return number[idx];
}

} // namespace DB
