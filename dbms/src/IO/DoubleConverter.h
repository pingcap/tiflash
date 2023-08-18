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

#include <Common/nocopyable.h>
#include <double-conversion/double-conversion.h>

namespace DB
{
template <bool emit_decimal_point>
struct DoubleToStringConverterFlags
{
    static constexpr auto flags = double_conversion::DoubleToStringConverter::NO_FLAGS;
};

template <>
struct DoubleToStringConverterFlags<true>
{
    static constexpr auto flags = double_conversion::DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT;
};

template <bool emit_decimal_point>
class DoubleConverter
{
    DoubleConverter() = default;

public:
    /** @todo Add commentary on how this constant is deduced.
     *    e.g. it's minus sign, integral zero, decimal point, up to 5 leading zeros and kBase10MaximalLength digits. */
    static constexpr auto MAX_REPRESENTATION_LENGTH = 26;
    using BufferType = char[MAX_REPRESENTATION_LENGTH];

    static const auto & instance()
    {
        // clang-format off
        static const double_conversion::DoubleToStringConverter instance{
            DoubleToStringConverterFlags<emit_decimal_point>::flags, "inf", "nan", 'e', -6, 21, 6, 1
        };
        // clang-format on

        return instance;
    }

    DISALLOW_COPY(DoubleConverter);
};

} // namespace DB
