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

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(DecimalType_test, Parse)
{
    Field field;
    const float f_true_value = 1234.56789f;
    const double d_true_value = 1234.56789;
    const std::string str_to_parse = "1234.56789";
    bool parse_result = false;
    parse_result = parseDecimal(str_to_parse.c_str(), str_to_parse.size(), false, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal32);
    Decimal32 d32 = field.get<Decimal32>();
    std::string str_decimal = d32.toString(5);
    ASSERT_EQ(str_decimal, "1234.56789");
    ASSERT_FLOAT_EQ(d32.toFloat<float>(5), f_true_value);
    ASSERT_DOUBLE_EQ(d32.toFloat<double>(5), d_true_value);

    parse_result = parseDecimal(str_to_parse.c_str(), str_to_parse.size(), true, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal32);
    Decimal32 md32 = field.get<Decimal32>();
    str_decimal = md32.toString(5);
    ASSERT_EQ(str_decimal, "-1234.56789");
    ASSERT_FLOAT_EQ(md32.toFloat<float>(5), -f_true_value);
    ASSERT_DOUBLE_EQ(md32.toFloat<double>(5), -d_true_value);

    // The maximum number of digits (the precision) has a range of 1 to 65.
    const std::string max_value = "99999999999999999999999999999999999999999999999999999999999999999";
    parse_result = parseDecimal(max_value.c_str(), max_value.size(), false, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal256);
    Decimal256 d256 = field.get<Decimal256>();
    str_decimal = d256.toString(0);
    ASSERT_EQ(str_decimal, max_value);

    const std::string overflow_max_value = "100000000000000000000000000000000000000000000000000000000000000000";
    parse_result = parseDecimal(overflow_max_value.c_str(), overflow_max_value.size(), false, field);
    ASSERT_FALSE(parse_result);
}

TEST(DecimalType_test, ParseZero)
{
    Field field;
    const std::string str_to_parse = "0";
    bool parse_result = false;
    parse_result = parseDecimal(str_to_parse.c_str(), str_to_parse.size(), false, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal32);
    Decimal32 d32 = field.get<Decimal32>();
    std::string str_decimal = d32.toString(0);
    ASSERT_EQ(str_decimal, "0");

    parse_result = parseDecimal(str_to_parse.c_str(), str_to_parse.size(), true, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal32);
    d32 = field.get<Decimal32>();
    str_decimal = d32.toString(0);
    ASSERT_EQ(str_decimal, "0");

    parse_result = parseDecimal("", 0, true, field);
    ASSERT_TRUE(parse_result);
    ASSERT_EQ(field.getType(), Field::Types::Which::Decimal32);
    d32 = field.get<Decimal32>();
    str_decimal = d32.toString(0);
    ASSERT_EQ(str_decimal, "0");
}

TEST(DecimalType_test, InvalidInputs)
{
    Field field;
    std::vector<std::string> inputs = {
        "1.1.1",
        ".1314",
        "0x2",
        "0o2",
    };
    for (const auto & str : inputs)
    {
        bool parse_result = parseDecimal(str.c_str(), str.size(), false, field);
        ASSERT_FALSE(parse_result);
    }
}

TEST(DecimalType_test, ConvertToDecimal)
{
    uint64_t u64_value = 12345678910UL;
    Decimal64 d64 = ToDecimal<uint64_t, Decimal64>(u64_value, 4);
    ASSERT_EQ(d64.toString(4), "12345678910.0000");

    double f_value = 1234.56789;
    Decimal32 d32_5 = ToDecimal<double, Decimal32>(f_value, 5);
    ASSERT_EQ(d32_5.toString(5), "1234.56789");
    // rounding
    Decimal32 d32_2 = ToDecimal<double, Decimal32>(f_value, 2);
    ASSERT_EQ(d32_2.toString(2), "1234.57");
}

} // namespace tests
} // namespace DB
