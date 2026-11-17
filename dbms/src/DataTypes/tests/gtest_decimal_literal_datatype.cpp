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

#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <limits>

namespace DB
{
namespace tests
{
template <typename T>
class DecimalScaleMultiplierTest : public ::testing::Test
{
};

using DecimalScaleTypes = ::testing::Types<Decimal32, Decimal64, Decimal128, Decimal256>;
TYPED_TEST_CASE(DecimalScaleMultiplierTest, DecimalScaleTypes);

TYPED_TEST(DecimalScaleMultiplierTest, Bounds)
{
    constexpr auto precision = maxDecimalPrecision<TypeParam>();
    const DataTypeDecimal<TypeParam> type(precision, 0);
    EXPECT_EQ(type.getScaleMultiplier(0).value, 1);
    EXPECT_EQ(type.getScaleMultiplier(precision).value, getScaleMultiplier<TypeParam>(precision));
    for (const ScaleType scale : {precision + 1, std::numeric_limits<ScaleType>::max()})
    {
        SCOPED_TRACE(scale);
        EXPECT_THROW(type.getScaleMultiplier(scale), TiFlashException);
    }
}

TEST(DecimalLiteralDataTypeTest, DivisionScaleUnderflow)
{
    const DataTypeDecimal64 left(10, 2);
    const DataTypeDecimal64 right(10, 0);
    const DataTypeDecimal32 invalid_result(9, 1);
    EXPECT_THROW(invalid_result.getScales(left, right, false, true), TiFlashException);

    // A negative intermediate difference is valid when the result scale compensates for it.
    const DataTypeDecimal64 valid_result(14, 6);
    const auto [left_multiplier, right_multiplier, result_multiplier]
        = valid_result.getScales(left, right, false, true);
    EXPECT_EQ(left_multiplier.value, 10000);
    EXPECT_EQ(right_multiplier.value, 1);
    EXPECT_EQ(result_multiplier.value, 1);

    // Clamping the result scale to 30 can still require a multiplier with an exponent above 30.
    const DataTypeDecimal32 integer_left(1, 0);
    const DataTypeDecimal256 fractional_right(65, 30);
    const DataTypeDecimal256 clamped_result(61, 30);
    const auto clamped_scales = clamped_result.getScales(integer_left, fractional_right, false, true);
    EXPECT_EQ(std::get<0>(clamped_scales).value, getScaleMultiplier<Decimal256>(60));
}

TEST(DecimalLiteralDataTypeTest, getPrec)
try
{
    /// Decimal32
    ASSERT_TRUE(DecimalField<Decimal32>(0, 0).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal32>(0, 1).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal32>(0, 2).getPrec() == 2);
    ASSERT_TRUE(DecimalField<Decimal32>(123, 0).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal32>(123, 2).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal32>(123, 4).getPrec() == 4);

    /// Decimal64
    ASSERT_TRUE(DecimalField<Decimal64>(0, 0).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal64>(0, 1).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal64>(0, 2).getPrec() == 2);
    ASSERT_TRUE(DecimalField<Decimal64>(123, 0).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal64>(123, 2).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal64>(123, 4).getPrec() == 4);
    ASSERT_TRUE(DecimalField<Decimal64>(1234567891011ll, 4).getPrec() == 13);

    /// Decimal128
    ASSERT_TRUE(DecimalField<Decimal128>(0, 0).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal128>(0, 1).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal128>(0, 2).getPrec() == 2);
    ASSERT_TRUE(DecimalField<Decimal128>(123, 0).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal128>(123, 2).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal128>(123, 4).getPrec() == 4);
    ASSERT_TRUE(DecimalField<Decimal128>(Int128(123123123123123ll) * 1000000, 4).getPrec() == 21);

    /// Decimal256
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(0), 0).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(0), 1).getPrec() == 1);
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(0), 2).getPrec() == 2);
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(123), 0).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(123), 2).getPrec() == 3);
    ASSERT_TRUE(DecimalField<Decimal256>(Int256(123), 4).getPrec() == 4);
    ASSERT_TRUE(
        DecimalField<Decimal256>(Int256(123123123123123123ll) * Int256(1000000000ll) * Int256(100000000000000ll), 4)
            .getPrec()
        == 41);
}
CATCH

TEST(DecimalLiteralDataTypeTest, fieldToDataType)
try
{
    /// Decimal32
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(1,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(0, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(1,1)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(0, 1)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(2,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(0, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(3,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(123, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(3,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(123, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(4,4)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal32>(123, 4)))));

    /// Decimal64
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(10,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(0, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(10,1)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(0, 1)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(10,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(0, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(10,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(123, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(10,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(123, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(13,4)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal64>(1234567891011ll, 4)))));

    /// Decimal128
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(19,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal128>(0, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(19,1)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal128>(0, 1)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(19,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal128>(0, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(19,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal128>(123, 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(19,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal128>(123, 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(21,4)")
                    ->equals(*applyVisitor(
                        FieldToDataType(),
                        Field(DecimalField<Decimal128>(Int128(123123123123123ll) * 1000000, 4)))));

    /// Decimal256
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(39,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal256>(Int256(0), 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(39,1)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal256>(Int256(0), 1)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(39,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal256>(Int256(0), 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(39,0)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal256>(Int256(123), 0)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(39,2)")
                    ->equals(*applyVisitor(FieldToDataType(), Field(DecimalField<Decimal256>(Int256(123), 2)))));
    ASSERT_TRUE(DataTypeFactory::instance()
                    .get("Decimal(41,4)")
                    ->equals(*applyVisitor(
                        FieldToDataType(),
                        Field(DecimalField<Decimal256>(
                            Int256(123123123123123123ll) * Int256(1000000000ll) * Int256(100000000000000ll),
                            4)))));
}
CATCH
} // namespace tests
} // namespace DB
