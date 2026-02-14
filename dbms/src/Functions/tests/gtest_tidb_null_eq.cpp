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
#include <TiDB/Collation/Collator.h>

namespace DB::tests
{
class TestTiDBNullEQ : public DB::tests::FunctionTest
{
};

TEST_F(TestTiDBNullEQ, Basic)
try
{
    auto a = createColumn<Int64>({1, 2, 2});
    auto b = createColumn<Int64>({1, 3, 2});
    auto res = executeFunction("tidbNullEQ", a, b);
    ASSERT_EQ(res.type->getName(), "UInt8");
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 0, 1}), res);
}
CATCH

TEST_F(TestTiDBNullEQ, NullableInputs)
try
{
    auto a = createColumn<Nullable<Int64>>({1, std::nullopt, std::nullopt, 2});
    auto b = createColumn<Nullable<Int64>>({1, std::nullopt, 3, std::nullopt});
    auto res = executeFunction("tidbNullEQ", a, b);
    ASSERT_EQ(res.type->getName(), "UInt8");
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 1, 0, 0}), res);
}
CATCH

TEST_F(TestTiDBNullEQ, OnlyNullColumns)
try
{
    auto a = createOnlyNullColumn(5);
    auto b = createOnlyNullColumn(5);
    auto res = executeFunction("tidbNullEQ", a, b);
    ASSERT_EQ(res.type->getName(), "UInt8");
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 1, 1, 1, 1}), res);
}
CATCH

TEST_F(TestTiDBNullEQ, OneSideOnlyNull)
try
{
    auto a = createOnlyNullColumn(3);
    auto b = createColumn<Nullable<Int64>>({1, std::nullopt, 3});
    auto res = executeFunction("tidbNullEQ", a, b);
    ASSERT_EQ(res.type->getName(), "UInt8");
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 0}), res);
}
CATCH

TEST_F(TestTiDBNullEQ, ConstOnlyNull)
try
{
    auto a = createOnlyNullColumnConst(4);
    auto b = createConstColumn<Nullable<Int64>>(4, 1);
    auto res = executeFunction("tidbNullEQ", a, b);
    ASSERT_EQ(res.type->getName(), "UInt8");
    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(4, 0), res);
}
CATCH

TEST_F(TestTiDBNullEQ, CollatorIsForwardedToEquals)
try
{
    auto a = createColumn<Nullable<String>>({"a", "A", std::nullopt});
    auto b = createColumn<Nullable<String>>({"A", "a", std::nullopt});

    auto ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 1, 1}), executeFunction("tidbNullEQ", {a, b}, ci_collator));

    auto bin_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 1}), executeFunction("tidbNullEQ", {a, b}, bin_collator));
}
CATCH

} // namespace DB::tests
