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

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

namespace DB::tests
{
class Logical : public DB::tests::FunctionTest
{
protected:
    void test(
        const String & func_name,
        const ColumnWithTypeAndName & result,
        const ColumnWithTypeAndName & col1,
        const ColumnWithTypeAndName & col2)
    {
        ASSERT_COLUMN_EQ(result, executeFunction(func_name, col1, col2));
    }
    ColumnWithTypeAndName not_null_false_column = createColumn<UInt8>({0, 0});
    ColumnWithTypeAndName not_null_true_column = createColumn<UInt8>({1, 1});
    ColumnWithTypeAndName nullable_false_column = createColumn<Nullable<UInt8>>({0, 0});
    ColumnWithTypeAndName nullable_true_column = createColumn<Nullable<UInt8>>({1, 1});
    ColumnWithTypeAndName nullable_null_column = createColumn<Nullable<UInt8>>({{}, {}});
    ColumnWithTypeAndName not_null_false_const = createConstColumn<UInt8>(2, 0);
    ColumnWithTypeAndName not_null_true_const = createConstColumn<UInt8>(2, 1);
    ColumnWithTypeAndName nullable_false_const = createConstColumn<Nullable<UInt8>>(2, 0);
    ColumnWithTypeAndName nullable_true_const = createConstColumn<Nullable<UInt8>>(2, 1);
    ColumnWithTypeAndName nullable_null_const = createConstColumn<Nullable<UInt8>>(2, {});
};

TEST_F(Logical, andTest)
try
{
    const String & name = "binary_and";
    // basic tests
    // false && false
    test(name, not_null_false_column, not_null_false_column, not_null_false_column);
    test(name, not_null_false_const, not_null_false_column, not_null_false_const);
    test(name, nullable_false_column, nullable_false_column, not_null_false_column);
    test(name, nullable_false_const, nullable_false_column, not_null_false_const);
    test(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    test(name, not_null_false_const, not_null_false_column, nullable_false_const);
    test(name, nullable_false_column, nullable_false_column, nullable_false_column);
    test(name, nullable_false_const, nullable_false_column, nullable_false_const);
    // false && true
    test(name, not_null_false_column, not_null_false_column, not_null_true_column);
    test(name, not_null_false_column, not_null_false_column, not_null_true_const);
    test(name, nullable_false_column, nullable_false_column, not_null_true_column);
    test(name, nullable_false_column, nullable_false_column, not_null_true_const);
    test(name, nullable_false_column, not_null_false_column, nullable_true_column);
    test(name, not_null_false_column, not_null_false_column, nullable_true_const);
    test(name, nullable_false_column, nullable_false_column, nullable_true_column);
    test(name, nullable_false_column, nullable_false_column, nullable_true_const);
    // false && null
    test(name, nullable_false_column, not_null_false_column, nullable_null_column);
    test(name, nullable_false_column, not_null_false_column, nullable_null_const);
    test(name, nullable_false_column, nullable_false_column, nullable_null_column);
    test(name, nullable_false_column, nullable_false_column, nullable_null_const);
    // true && false
    test(name, not_null_false_column, not_null_true_column, not_null_false_column);
    test(name, not_null_false_const, not_null_true_column, not_null_false_const);
    test(name, nullable_false_column, nullable_true_column, not_null_false_column);
    test(name, nullable_false_const, nullable_true_column, not_null_false_const);
    test(name, nullable_false_column, not_null_true_column, nullable_false_column);
    test(name, not_null_false_const, not_null_true_column, nullable_false_const);
    test(name, nullable_false_column, nullable_true_column, nullable_false_column);
    test(name, nullable_false_const, nullable_true_column, nullable_false_const);
    // true && true
    test(name, not_null_true_column, not_null_true_column, not_null_true_column);
    test(name, not_null_true_column, not_null_true_column, not_null_true_const);
    test(name, nullable_true_column, nullable_true_column, not_null_true_column);
    test(name, nullable_true_column, nullable_true_column, not_null_true_const);
    test(name, nullable_true_column, not_null_true_column, nullable_true_column);
    test(name, not_null_true_column, not_null_true_column, nullable_true_const);
    test(name, nullable_true_column, nullable_true_column, nullable_true_column);
    test(name, nullable_true_column, nullable_true_column, nullable_true_const);
    // true && null
    test(name, nullable_null_column, not_null_true_column, nullable_null_column);
    test(name, nullable_null_column, not_null_true_column, nullable_null_const);
    test(name, nullable_null_column, nullable_true_column, nullable_null_column);
    test(name, nullable_null_column, nullable_true_column, nullable_null_const);
    // null && true
    test(name, nullable_null_column, nullable_null_column, not_null_true_column);
    test(name, nullable_null_column, nullable_null_column, not_null_true_const);
    test(name, nullable_null_column, nullable_null_column, nullable_true_column);
    test(name, nullable_null_column, nullable_null_column, nullable_true_const);
    // null && false
    test(name, nullable_false_column, nullable_null_column, not_null_false_column);
    test(name, nullable_false_const, nullable_null_column, not_null_false_const);
    test(name, nullable_false_column, nullable_null_column, nullable_false_column);
    test(name, nullable_false_const, nullable_null_column, nullable_false_const);
    // null && null
    test(name, nullable_null_column, nullable_null_column, nullable_null_column);
    test(name, nullable_null_column, nullable_null_column, nullable_null_const);

    // column, column
    test(
        name,
        createColumn<Nullable<UInt8>>({0, 1, 0, 0, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    test(
        name,
        createColumn<Nullable<UInt8>>({1, 0}),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    test(
        name,
        createConstColumn<Nullable<UInt8>>(2, 0),
        createConstColumn<Nullable<UInt8>>(2, 0),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    test(
        name,
        createConstColumn<UInt8>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1));
    // only null
    test(
        name,
        createColumn<Nullable<UInt8>>({{}, 0}),
        createOnlyNullColumnConst(2),
        createColumn<Nullable<UInt8>>({1, 0}));
    // issue 6127
    test(
        name,
        createColumn<UInt8>({0, 1, 0, 0}),
        createColumn<Int64>({0, 123, 0, 41}),
        createColumn<UInt8>({0, 11, 221, 0}));
    // issue 6127, position of UInt8 column may affect the result
    test(
        name,
        createColumn<UInt8>({0, 1, 0, 0}),
        createColumn<UInt8>({0, 123, 0, 41}),
        createColumn<Int64>({0, 11, 221, 0}));
}
CATCH

TEST_F(Logical, orTest)
try
{
    const String & name = "binary_or";
    // basic tests
    // false || false
    test(name, not_null_false_column, not_null_false_column, not_null_false_column);
    test(name, not_null_false_column, not_null_false_column, not_null_false_const);
    test(name, nullable_false_column, nullable_false_column, not_null_false_column);
    test(name, nullable_false_column, nullable_false_column, not_null_false_const);
    test(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    test(name, not_null_false_column, not_null_false_column, nullable_false_const);
    test(name, nullable_false_column, nullable_false_column, nullable_false_column);
    test(name, nullable_false_column, nullable_false_column, nullable_false_const);
    // false || true
    test(name, not_null_true_column, not_null_false_column, not_null_true_column);
    test(name, not_null_true_const, not_null_false_column, not_null_true_const);
    test(name, nullable_true_column, nullable_false_column, not_null_true_column);
    test(name, nullable_true_const, nullable_false_column, not_null_true_const);
    test(name, nullable_true_column, not_null_false_column, nullable_true_column);
    test(name, not_null_true_const, not_null_false_column, nullable_true_const);
    test(name, nullable_true_column, nullable_false_column, nullable_true_column);
    test(name, nullable_true_const, nullable_false_column, nullable_true_const);
    // false || null
    test(name, nullable_null_column, not_null_false_column, nullable_null_column);
    test(name, nullable_null_column, not_null_false_column, nullable_null_const);
    test(name, nullable_null_column, nullable_false_column, nullable_null_column);
    test(name, nullable_null_column, nullable_false_column, nullable_null_const);
    // true || false
    test(name, not_null_true_column, not_null_true_column, not_null_false_column);
    test(name, not_null_true_column, not_null_true_column, not_null_false_const);
    test(name, nullable_true_column, nullable_true_column, not_null_false_column);
    test(name, nullable_true_column, nullable_true_column, not_null_false_const);
    test(name, nullable_true_column, not_null_true_column, nullable_false_column);
    test(name, not_null_true_column, not_null_true_column, nullable_false_const);
    test(name, nullable_true_column, nullable_true_column, nullable_false_column);
    test(name, nullable_true_column, nullable_true_column, nullable_false_const);
    // true || true
    test(name, not_null_true_column, not_null_true_column, not_null_true_column);
    test(name, not_null_true_const, not_null_true_column, not_null_true_const);
    test(name, nullable_true_column, nullable_true_column, not_null_true_column);
    test(name, nullable_true_const, nullable_true_column, not_null_true_const);
    test(name, nullable_true_column, not_null_true_column, nullable_true_column);
    test(name, not_null_true_const, not_null_true_column, nullable_true_const);
    test(name, nullable_true_column, nullable_true_column, nullable_true_column);
    test(name, nullable_true_const, nullable_true_column, nullable_true_const);
    // true || null
    test(name, nullable_true_column, not_null_true_column, nullable_null_column);
    test(name, nullable_true_column, not_null_true_column, nullable_null_const);
    test(name, nullable_true_column, nullable_true_column, nullable_null_column);
    test(name, nullable_true_column, nullable_true_column, nullable_null_const);
    // null || true
    test(name, nullable_true_column, nullable_null_column, not_null_true_column);
    test(name, nullable_true_const, nullable_null_column, not_null_true_const);
    test(name, nullable_true_column, nullable_null_column, nullable_true_column);
    test(name, nullable_true_const, nullable_null_column, nullable_true_const);
    // null || false
    test(name, nullable_null_column, nullable_null_column, not_null_false_column);
    test(name, nullable_null_column, nullable_null_column, not_null_false_const);
    test(name, nullable_null_column, nullable_null_column, nullable_false_column);
    test(name, nullable_null_column, nullable_null_column, nullable_false_const);
    // null || null
    test(name, nullable_null_column, nullable_null_column, nullable_null_column);
    test(name, nullable_null_column, nullable_null_column, nullable_null_const);

    // column, column
    test(
        name,
        createColumn<Nullable<UInt8>>({0, 1, 1, 1, 1, {}}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    test(
        name,
        createConstColumn<Nullable<UInt8>>(2, 1),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    test(
        name,
        createConstColumn<UInt8>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 0));
    // only null
    test(
        name,
        createColumn<Nullable<UInt8>>({1, {}}),
        createOnlyNullColumnConst(2),
        createColumn<Nullable<UInt8>>({1, 0}));
    // issue 5849
    test(
        name,
        createColumn<UInt8>({0, 1, 1, 1}),
        createColumn<UInt8>({0, 123, 0, 41}),
        createColumn<Int64>({0, 11, 221, 0}));
}
CATCH

TEST_F(Logical, xorTest)
try
{
    const String & name = "binary_xor";
    // basic tests
    // false xor false
    test(name, not_null_false_column, not_null_false_column, not_null_false_column);
    test(name, not_null_false_column, not_null_false_column, not_null_false_const);
    test(name, nullable_false_column, nullable_false_column, not_null_false_column);
    test(name, nullable_false_column, nullable_false_column, not_null_false_const);
    test(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    test(name, not_null_false_column, not_null_false_column, nullable_false_const);
    test(name, nullable_false_column, nullable_false_column, nullable_false_column);
    test(name, nullable_false_column, nullable_false_column, nullable_false_const);
    // false xor true
    test(name, not_null_true_column, not_null_false_column, not_null_true_column);
    test(name, not_null_true_column, not_null_false_column, not_null_true_const);
    test(name, nullable_true_column, nullable_false_column, not_null_true_column);
    test(name, nullable_true_column, nullable_false_column, not_null_true_const);
    test(name, nullable_true_column, not_null_false_column, nullable_true_column);
    test(name, not_null_true_column, not_null_false_column, nullable_true_const);
    test(name, nullable_true_column, nullable_false_column, nullable_true_column);
    test(name, nullable_true_column, nullable_false_column, nullable_true_const);
    // false xor null
    test(name, nullable_null_column, not_null_false_column, nullable_null_column);
    test(name, nullable_null_const, not_null_false_column, nullable_null_const);
    test(name, nullable_null_column, nullable_false_column, nullable_null_column);
    test(name, nullable_null_const, nullable_false_column, nullable_null_const);
    // true xor false
    test(name, not_null_true_column, not_null_true_column, not_null_false_column);
    test(name, not_null_true_column, not_null_true_column, not_null_false_const);
    test(name, nullable_true_column, nullable_true_column, not_null_false_column);
    test(name, nullable_true_column, nullable_true_column, not_null_false_const);
    test(name, nullable_true_column, not_null_true_column, nullable_false_column);
    test(name, not_null_true_column, not_null_true_column, nullable_false_const);
    test(name, nullable_true_column, nullable_true_column, nullable_false_column);
    test(name, nullable_true_column, nullable_true_column, nullable_false_const);
    // true xor true
    test(name, not_null_false_column, not_null_true_column, not_null_true_column);
    test(name, not_null_false_column, not_null_true_column, not_null_true_const);
    test(name, nullable_false_column, nullable_true_column, not_null_true_column);
    test(name, nullable_false_column, nullable_true_column, not_null_true_const);
    test(name, nullable_false_column, not_null_true_column, nullable_true_column);
    test(name, not_null_false_column, not_null_true_column, nullable_true_const);
    test(name, nullable_false_column, nullable_true_column, nullable_true_column);
    test(name, nullable_false_column, nullable_true_column, nullable_true_const);
    // true xor null
    test(name, nullable_null_column, not_null_true_column, nullable_null_column);
    test(name, nullable_null_const, not_null_true_column, nullable_null_const);
    test(name, nullable_null_column, nullable_true_column, nullable_null_column);
    test(name, nullable_null_const, nullable_true_column, nullable_null_const);
    // null xor true
    test(name, nullable_null_column, nullable_null_column, not_null_true_column);
    test(name, nullable_null_column, nullable_null_column, not_null_true_const);
    test(name, nullable_null_column, nullable_null_column, nullable_true_column);
    test(name, nullable_null_column, nullable_null_column, nullable_true_const);
    // null xor false
    test(name, nullable_null_column, nullable_null_column, not_null_false_column);
    test(name, nullable_null_column, nullable_null_column, not_null_false_const);
    test(name, nullable_null_column, nullable_null_column, nullable_false_column);
    test(name, nullable_null_column, nullable_null_column, nullable_false_const);
    // null xor null
    test(name, nullable_null_column, nullable_null_column, nullable_null_column);
    test(name, nullable_null_const, nullable_null_column, nullable_null_const);

    // column, column
    test(
        name,
        createColumn<Nullable<UInt8>>({0, 0, 1, 1, {}, {}}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    test(
        name,
        createColumn<Nullable<UInt8>>({0, 1}),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    test(
        name,
        createConstColumn<UInt8>(1, 0),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1));
    // only null
    test(name, createOnlyNullColumnConst(2), createOnlyNullColumnConst(2), createColumn<Nullable<UInt8>>({1, 0}));
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
