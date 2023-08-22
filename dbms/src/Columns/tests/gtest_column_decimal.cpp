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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestColumnDecimal : public ::testing::Test
{
};

template <typename Decimal>
void testCloneResized(int precision)
{
    using Native = typename Decimal::NativeType;
    using FieldType = DecimalField<Decimal>;

    auto column_ptr = createColumn<Decimal>(
                          std::make_tuple(precision, 4),
                          {FieldType(static_cast<Native>(1), 4),
                           FieldType(static_cast<Native>(2), 4),
                           FieldType(static_cast<Native>(3), 4),
                           FieldType(static_cast<Native>(4), 4)})
                          .column;
    auto clone_column_ptr = column_ptr->cloneResized(column_ptr->size() + 1);

    for (size_t i = 0; i != column_ptr->size(); ++i)
    {
        Field origin_field;
        column_ptr->get(i, origin_field);
        auto & origin_value = origin_field.template get<Decimal>();

        Field clone_field;
        clone_column_ptr->get(i, clone_field);
        auto & clone_value = clone_field.template get<Decimal>();

        ASSERT_TRUE(origin_value == clone_value);

        Decimal zero{};
        origin_value = zero;
        ASSERT_TRUE(origin_value != clone_value);
    }
    Field last_field;
    clone_column_ptr->get(column_ptr->size(), last_field);
    auto last_value = last_field.template get<Decimal>();
    ASSERT_TRUE(last_value.value == 0);
}

TEST_F(TestColumnDecimal, CloneResized)
try
{
    testCloneResized<Decimal32>(9);
    testCloneResized<Decimal64>(18);
    testCloneResized<Decimal128>(38);
    testCloneResized<Decimal256>(65);
}
CATCH

} // namespace tests
} // namespace DB