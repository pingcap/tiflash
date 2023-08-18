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

#include <ext/range.h>

namespace DB
{
namespace tests
{
class TestFunctionElt : public DB::tests::FunctionTest
{
};

#define ASSERT_ELT(expected, t1, t2, ...) ASSERT_COLUMN_EQ(expected, executeFunction("elt", t1, t2, __VA_ARGS__))

TEST_F(TestFunctionElt, BoundaryIdx)
try
{
    constexpr size_t ndummy_col = 6;
    const ColumnWithTypeAndName dummy_col0 = createColumn<Nullable<String>>({"abc", "123", "", {}, "", ""});
    const ColumnWithTypeAndName dummy_col1 = createConstColumn<Nullable<String>>(ndummy_col, "def");

    constexpr size_t nboundary_test = 6;
    const int boundary_values[nboundary_test] = {0, -1, -2, -100, 3, 100};

    /// elt returns null if the first argument is less than 1, greater than the number of string arguments, or NULL

    /// const idx
    for (const auto i : ext::range(0, nboundary_test))
    {
        ASSERT_ELT(
            createConstColumn<Nullable<String>>(ndummy_col, {}),
            createConstColumn<Nullable<Int64>>(ndummy_col, boundary_values[i]),
            dummy_col0,
            dummy_col1);

        ASSERT_ELT(
            createConstColumn<Nullable<String>>(ndummy_col, {}),
            createConstColumn<Int64>(ndummy_col, boundary_values[i]),
            dummy_col0,
            dummy_col1);
    }

    /// vector idx
    ASSERT_ELT(
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}, {}}),
        createColumn<Nullable<Int64>>({0, -1, -2, -100, 3, 100}),
        dummy_col0,
        dummy_col1);
}
CATCH

TEST_F(TestFunctionElt, NullArgument)
try
{
    constexpr size_t nrow = 5;

    /// const null idx
    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, {}),
        createConstColumn<Nullable<Int64>>(nrow, {}),
        createColumn<Nullable<String>>({"abc", "123", "", {}, ""}),
        createConstColumn<Nullable<String>>(nrow, "def"));

    /// vector null idx
    ASSERT_ELT(
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<Int64>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<String>>({"abc", "123", "", {}, ""}),
        createConstColumn<Nullable<String>>(nrow, "def"));

    /// const non-null idx x const null arg
    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, {}),
        createConstColumn<Nullable<Int64>>(nrow, 1),
        createConstColumn<Nullable<String>>(nrow, {}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));

    /// vector non-null idx x const null arg
    ASSERT_ELT(
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<Int64>>({1, 1, 1, 1, 1}),
        createConstColumn<Nullable<String>>(nrow, {}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));

    /// const non-null idx x null vector arg
    ASSERT_ELT(
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createConstColumn<Nullable<Int64>>(nrow, 1),
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));

    /// vector non-null idx x null vector arg
    ASSERT_ELT(
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<Int64>>({1, 1, 1, 1, 1}),
        createColumn<Nullable<String>>({{}, {}, {}, {}, {}}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));

    /// const non-null idx x nullable arg
    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "123", "", {}, ""}),
        createConstColumn<Nullable<Int64>>(nrow, 1),
        createColumn<Nullable<String>>({"abc", "123", "", {}, ""}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));

    /// vector idx x nullable arg
    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", {}, {}, {}}),
        createColumn<Nullable<Int64>>({1, 2, 2, 1, {}}),
        createColumn<Nullable<String>>({"abc", "123", "", {}, ""}),
        createColumn<Nullable<String>>({"def", "321", {}, "", ""}));
}
CATCH

TEST_F(TestFunctionElt, AllTypeIdx)
try
{
    constexpr size_t nrow = 8;

    /// const idx x vector
    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createConstColumn<Nullable<Int8>>(nrow, 1),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<UInt8>>(nrow, 2),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createConstColumn<Nullable<Int16>>(nrow, 1),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<UInt16>>(nrow, 2),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createConstColumn<Nullable<Int32>>(nrow, 1),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<UInt32>>(nrow, 2),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createConstColumn<Nullable<Int64>>(nrow, 1),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<UInt64>>(nrow, 2),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}));

    /// const idx x const
    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<Int8>>(nrow, 1),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "def"),
        createConstColumn<Nullable<UInt8>>(nrow, 2),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<Int16>>(nrow, 1),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "def"),
        createConstColumn<Nullable<UInt16>>(nrow, 2),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<Int32>>(nrow, 1),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "def"),
        createConstColumn<Nullable<UInt32>>(nrow, 2),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<Int64>>(nrow, 1),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    ASSERT_ELT(
        createConstColumn<Nullable<String>>(nrow, "def"),
        createConstColumn<Nullable<UInt64>>(nrow, 2),
        createConstColumn<Nullable<String>>(nrow, "abc"),
        createConstColumn<Nullable<String>>(nrow, "def"));

    /// vector idx x vector/const
    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "", {}, "xxx", "", {}, {}}),
        createColumn<Nullable<Int8>>({1, 2, 1, 100, 3, 1, 0, -2}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "", {}, "xxx", "", {}, "c"}),
        createColumn<Nullable<UInt8>>({1, 2, 1, 100, 3, 1, 0, 1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "xxx", {}, "xxx", "", "a", {}}),
        createColumn<Nullable<Int16>>({1, 2, 3, 100, 3, 2, 1, -1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "", {}, "xxx", "", {}, "c"}),
        createColumn<Nullable<UInt16>>({1, 2, 1, 100, 3, 1, 0, 1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "xxx", {}, "xxx", "", "a", {}}),
        createColumn<Nullable<Int32>>({1, 2, 3, 100, 3, 2, 1, -1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "", {}, "xxx", "", {}, "c"}),
        createColumn<Nullable<UInt32>>({1, 2, 1, 100, 3, 1, 0, 1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "xxx", {}, "xxx", "", "a", {}}),
        createColumn<Nullable<Int64>>({1, 2, 3, 100, 3, 2, 1, -1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));

    ASSERT_ELT(
        createColumn<Nullable<String>>({"abc", "321", "", {}, "xxx", "", {}, "c"}),
        createColumn<Nullable<UInt64>>({1, 2, 1, 100, 3, 1, 0, 1}),
        createColumn<Nullable<String>>({"abc", "123", "", "", {}, "", "a", "c"}),
        createColumn<Nullable<String>>({"def", "321", {}, "", "", "", "b", "d"}),
        createConstColumn<Nullable<String>>(nrow, "xxx"));
}
CATCH

} // namespace tests
} // namespace DB
