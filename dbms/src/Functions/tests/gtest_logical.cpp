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

namespace DB::tests
{
class Logical : public DB::tests::FunctionTest
{
};

TEST_F(Logical, andTest)
try
{
    const String & func_name = "binary_and";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1, 0, 0, {}, 0}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 0}),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(2, 1), createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(1, 1),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(1, 1), createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, 0}),
        executeFunction(func_name, createOnlyNullColumnConst(2), createColumn<Nullable<UInt8>>({1, 0})));
    // issue 6127
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 1, 0, 0}),
        executeFunction(func_name, createColumn<Int64>({0, 123, 0, 41}), createColumn<UInt8>({0, 11, 221, 0})));
    // issue 6127, position of UInt8 column may affect the result
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 1, 0, 0}),
        executeFunction(func_name, createColumn<UInt8>({0, 123, 0, 41}), createColumn<Int64>({0, 11, 221, 0})));
}
CATCH

TEST_F(Logical, orTest)
try
{
    const String & func_name = "binary_or";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1, 1, 1, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 1}),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(2, 1), createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(1, 1),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(1, 1), createConstColumn<Nullable<UInt8>>(1, 0)));
    // only null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, {}}),
        executeFunction(func_name, createOnlyNullColumnConst(2), createColumn<Nullable<UInt8>>({1, 0})));
    // issue 5849
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 1, 1, 1}),
        executeFunction(func_name, createColumn<UInt8>({0, 123, 0, 41}), createColumn<Int64>({0, 11, 221, 0})));
}
CATCH

TEST_F(Logical, xorTest)
try
{
    const String & func_name = "binary_xor";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 0, 1, 1, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1}),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(2, 1), createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(1, 0),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(1, 1), createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(func_name, createOnlyNullColumnConst(2), createColumn<Nullable<UInt8>>({1, 0})));
}
CATCH

TEST_F(Logical, notTest)
try
{
    const String & func_name = "not";

    // column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 0, {}}),
        executeFunction(func_name, createColumn<Nullable<UInt8>>({0, 1, {}})));
    // const
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(1, 0),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(1), executeFunction(func_name, createOnlyNullColumnConst(1)));
}
CATCH

} // namespace DB::tests
