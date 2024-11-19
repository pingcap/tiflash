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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class StringPosition : public DB::tests::FunctionTest
{
};

TEST_F(StringPosition, position)
{
    const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    {
        // const const
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(0, 0),
            executeFunction(
                "position",
                {createConstColumn<String>(0, ""), createConstColumn<String>(0, "")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(3, 1),
            executeFunction(
                "position",
                {createConstColumn<String>(3, ""), createConstColumn<String>(3, "123")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 7),
            executeFunction(
                "position",
                {createConstColumn<String>(1, ".*"), createConstColumn<String>(1, "123啊.*f啊")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 7),
            executeFunction(
                "position",
                {createConstColumn<String>(1, "aBc"), createConstColumn<String>(1, "1啊23aBc啊")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(2, 0),
            executeFunction(
                "position",
                {createConstColumn<String>(2, "aBc"), createConstColumn<String>(2, "1啊23abc啊")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(10, 0),
            executeFunction(
                "position",
                {createConstColumn<String>(10, "人"), createConstColumn<String>(10, "3啊*f啊")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(6, 0),
            executeFunction(
                "position",
                {createConstColumn<String>(6, "123"), createConstColumn<String>(6, "")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(6, 1),
            executeFunction(
                "position",
                {createConstColumn<String>(6, ""), createConstColumn<String>(6, "")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(2, 3),
            executeFunction(
                "position",
                {createConstColumn<String>(2, "123 "), createConstColumn<String>(2, "ce123 fe")},
                collator));

        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(2, 0),
            executeFunction(
                "position",
                {createConstColumn<String>(2, "123 "), createConstColumn<String>(2, "ce123fe ")},
                collator));
    }

    {
        // const vector
        ASSERT_COLUMN_EQ(
            createColumn<Int64>({}),
            executeFunction("position", {createConstColumn<String>(0, ""), createColumn<String>({})}, collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({1, 1}),
            executeFunction(
                "position",
                {createConstColumn<String>(2, ""), createColumn<String>({"", "12A哇"})},
                collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({0, 7, 0}),
            executeFunction(
                "position",
                {createConstColumn<String>(3, "a啊B.*"),
                 createColumn<String>({"", "2aF啊a啊B.*fe# ", "2aF啊A啊b.*fe "})},
                collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({0, 0, 7}),
            executeFunction(
                "position",
                {createConstColumn<String>(3, "123 "), createColumn<String>({"", "2aF啊123.*fe# ", "2aF啊123 .*fe "})},
                collator));
    }

    {
        // vector const
        ASSERT_COLUMN_EQ(
            createColumn<Int64>({}),
            executeFunction("position", {createColumn<String>({}), createConstColumn<String>(0, "")}, collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({1, 5, 0, 0, 19}),
            executeFunction(
                "position",
                {createColumn<String>({"", "A啊q", "g4GFE4g", "a啊q", ".*"}),
                 createConstColumn<String>(5, "f$*eA啊q飞F#f。.*&")},
                collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({1, 0, 0, 0}),
            executeFunction(
                "position",
                {createColumn<String>({"", "A啊q", "a啊q", ".*"}), createConstColumn<String>(4, "")},
                collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({1, 7, 0, 0}),
            executeFunction(
                "position",
                {createColumn<String>({"", "A啊q ", "a啊q ", ".*"}), createConstColumn<String>(4, "cew爬A啊q 3   ")},
                collator));
    }

    {
        // vector vector
        ASSERT_COLUMN_EQ(
            createColumn<Int64>({}),
            executeFunction("position", {createColumn<String>({}), createColumn<String>({})}, collator));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({1, 1, 0, 8, 0, 1, 0, 7}),
            executeFunction(
                "position",
                {createColumn<String>({"", "", "123", "G$啊3", "G$啊w3", "啊3法4fd", "aBc ", "aBc "}),
                 createColumn<String>(
                     {"", "123", "", "da嗯w$G$啊3gf4", "w好g$啊w33", "啊3法4fd", "啊3aBc法4fd ", "few人aBc "})},
                collator));
    }
}

TEST_F(StringPosition, positionUTF8)
{
    std::vector<TiDB::TiDBCollatorPtr> ci_collators{
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI)};

    // const const
    {
        // case sensitive(BinCollator)
        {
            const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(0, 0),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(0, ""), createConstColumn<String>(0, "")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(3, 1),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(3, ""), createConstColumn<String>(3, "123")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(1, 5),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(1, ".*"), createConstColumn<String>(1, "123啊.*f啊")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(1, 5),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(1, "aBc"), createConstColumn<String>(1, "1啊23aBc啊")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(2, 0),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(2, "aBc"), createConstColumn<String>(2, "1啊23abc啊")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(10, 0),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(10, "人"), createConstColumn<String>(10, "3啊*f啊")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(6, 0),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(6, "123"), createConstColumn<String>(6, "")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(6, 1),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(6, ""), createConstColumn<String>(6, "")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(2, 3),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(2, "123 "), createConstColumn<String>(2, "ce123 fe")},
                    collator));

            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(2, 0),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(2, "123 "), createConstColumn<String>(2, "ce123fe ")},
                    collator));
        }

        // case insensitive
        {
            for (const auto * collator : ci_collators)
            {
                ASSERT_COLUMN_EQ(
                    createConstColumn<Int64>(1, 5),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(1, "aBc"), createConstColumn<String>(1, "1啊23aBc啊")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createConstColumn<Int64>(2, 5),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(2, "aBc"), createConstColumn<String>(2, "1啊23abc啊")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createConstColumn<Int64>(2, 3),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(2, "abc "), createConstColumn<String>(2, "ceaBc fe")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createConstColumn<Int64>(2, 0),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(2, "abc "), createConstColumn<String>(2, "ceaBcfe ")},
                        collator));
            }
        }
    }

    // const vector
    {
        // case sensitive
        {
            const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
            ASSERT_COLUMN_EQ(
                createColumn<Int64>({}),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(0, ""), createColumn<String>({})},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({1, 1}),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(2, ""), createColumn<String>({"", "12A哇"})},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({0, 5, 0}),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(3, "a啊B.*"),
                     createColumn<String>({"", "2aF啊a啊B.*fe# ", "2aF啊A啊b.*fe "})},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({0, 0, 5}),
                executeFunction(
                    "positionUTF8",
                    {createConstColumn<String>(3, "123 "),
                     createColumn<String>({"", "2aF啊123.*fe# ", "2aF啊123 .*fe "})},
                    collator));
        }

        // case insensitive
        {
            for (const auto * collator : ci_collators)
            {
                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({}),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(0, ""), createColumn<String>({})},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({1, 1}),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(2, ""), createColumn<String>({"", "12A哇"})},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({0, 5, 5}),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(3, "a啊B.*"),
                         createColumn<String>({"", "2.F啊a啊B.*fe#", "2a.啊A啊b.*fe"})},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({0, 0, 5}),
                    executeFunction(
                        "positionUTF8",
                        {createConstColumn<String>(3, "abc "),
                         createColumn<String>({"", "2aF啊aBc.*fe# ", "2aF啊abC .*fe "})},
                        collator));
            }
        }
    }

    // vector const
    {
        // case sensitive
        {
            const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
            ASSERT_COLUMN_EQ(
                createColumn<Int64>({}),
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({}), createConstColumn<String>(0, "")},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({1, 5, 0, 0, 13}),
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "A啊q", "g4GFE4g", "a啊q", ".*"}),
                     createConstColumn<String>(5, "f$*eA啊q飞F#f。.*&")},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({1, 0, 0, 0}),
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "A啊q", "a啊q", ".*"}), createConstColumn<String>(4, "")},
                    collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({1, 5, 0, 0}),
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "A啊q ", "a啊q ", ".*"}),
                     createConstColumn<String>(4, "cew爬A啊q 3   ")},
                    collator));
        }

        // case insensitive
        {
            for (const auto * collator : ci_collators)
            {
                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({}),
                    executeFunction(
                        "positionUTF8",
                        {createColumn<String>({}), createConstColumn<String>(0, "")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({1, 5, 0, 5, 13}),
                    executeFunction(
                        "positionUTF8",
                        {createColumn<String>({"", "A啊q", "g4GFE4g", "a啊Q", ".*"}),
                         createConstColumn<String>(5, "f$*eA啊q飞F#f。.*&")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({1, 0, 0, 0}),
                    executeFunction(
                        "positionUTF8",
                        {createColumn<String>({"", "A啊q", "a啊q", ".*"}), createConstColumn<String>(4, "")},
                        collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({1, 5, 5, 0}),
                    executeFunction(
                        "positionUTF8",
                        {createColumn<String>({"", "A啊q ", "a啊q ", ".*"}),
                         createConstColumn<String>(4, "cew爬A啊q 3   ")},
                        collator));
            }
        }

        // no collator
        {
            ASSERT_THROW(
                executeFunction("positionUTF8", {createColumn<String>({}), createConstColumn<String>(0, "")}, nullptr),
                Exception);

            ASSERT_THROW(
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "A啊q", "g4GFE4g", "a啊q", ".*"}),
                     createConstColumn<String>(5, "f$*eA啊q飞F#f。.*&")},
                    nullptr),
                Exception);

            ASSERT_THROW(
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "A啊q", "a啊q", ".*"}), createConstColumn<String>(4, "")},
                    nullptr),
                Exception);
        }
    }

    // vector vector
    {
        // case sensitive
        {
            const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
            ASSERT_COLUMN_EQ(
                createColumn<Int64>({}),
                executeFunction("positionUTF8", {createColumn<String>({}), createColumn<String>({})}, collator));

            ASSERT_COLUMN_EQ(
                createColumn<Int64>({1, 1, 0, 6, 0, 1, 0, 5}),
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "", "123", "G$啊3", "G$啊w3", "啊3法4fd", "aBc ", "aBc "}),
                     createColumn<String>(
                         {"", "123", "", "da嗯w$G$啊3gf4", "w好g$啊w33", "啊3法4fd", "啊3aBc法4fd ", "few人aBc "})},
                    collator));
        }

        // case insensitive
        {
            for (const auto * collator : ci_collators)
            {
                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({}),
                    executeFunction("positionUTF8", {createColumn<String>({}), createColumn<String>({})}, collator));

                ASSERT_COLUMN_EQ(
                    createColumn<Int64>({1, 1, 0, 6, 3, 1, 0, 5}),
                    executeFunction(
                        "positionUTF8",
                        {createColumn<String>({"", "", "123", "G$啊3", "G$啊w3", "啊3法4fD", "aBc ", "aBc "}),
                         createColumn<String>(
                             {"", "123", "", "da嗯w$G$啊3gf4", "w好g$啊w33", "啊3法4fd", "啊3aBC法4fd ", "few人ABc "})},
                        collator));
            }
        }

        // no collator
        {
            ASSERT_THROW(
                executeFunction("positionUTF8", {createColumn<String>({}), createColumn<String>({})}, nullptr),
                Exception);

            ASSERT_THROW(
                executeFunction(
                    "positionUTF8",
                    {createColumn<String>({"", "", "123", "G$啊3", "G$啊w3", "啊3法4fd"}),
                     createColumn<String>({"", "123", "", "da嗯w$G$啊3gf4", "w好g$啊w33", "啊3法4fd"})},
                    nullptr),
                Exception);
        }
    }

    // nullable const
    {
        const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({}, std::vector<Int32>{}),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({}, std::vector<Int32>{}), createConstColumn<String>(0, "")},
                collator));

        std::vector<Int32> null_map{0, 1, 0, 0, 0, 1};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({1, 0, 0, 0, 0, 0}, null_map),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({"", "", "ad", "aB啊c", ".*", ""}, null_map),
                 createConstColumn<String>(6, "")},
                collator));

        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({1, 0, 5, 0, 0, 0}, null_map),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({"", "", "A啊q", "g4GFE4g", "a啊q", ""}, null_map),
                 createConstColumn<String>(6, "f$*eA啊q飞F#f。.*&")},
                collator));
    }

    // nullable vector
    {
        const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({}, std::vector<Int32>{}),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({}, std::vector<Int32>{}), createColumn<String>({})},
                collator));

        std::vector<Int32> null_map{0, 1, 0, 0, 0, 1, 0};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({1, 0, 1, 0, 0, 0, 3}, null_map),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({"", "", "ad", "aB啊c", ".*", "", "c"}, null_map),
                 createColumn<String>({"", "", "ad", "ab啊c", "123", "", "ACc"})},
                collator));
    }

    // nullable nullable
    {
        const auto * collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({}, std::vector<Int32>{}),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({}, std::vector<Int32>{}),
                 createNullableColumn<String>({}, std::vector<Int32>{})},
                collator));

        std::vector<Int32> col0_null_map{0, 0, 0, 1, 0, 0, 1, 0};
        std::vector<Int32> col1_null_map{1, 0, 0, 0, 1, 0, 1, 0};
        std::vector<Int32> res_null_map{1, 0, 0, 1, 1, 0, 1, 0};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({0, 0, 1, 0, 0, 1, 0, 3}, res_null_map),
            executeFunction(
                "positionUTF8",
                {createNullableColumn<String>({"", "aB啊c", "ad", "", "", "", "", "c"}, col0_null_map),
                 createNullableColumn<String>({"", "ab啊c", "ad", "", "", "", "", "ACc"}, col1_null_map)},
                collator));
    }
}

} // namespace tests
} // namespace DB
