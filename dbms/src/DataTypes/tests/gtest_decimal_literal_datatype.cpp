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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
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
