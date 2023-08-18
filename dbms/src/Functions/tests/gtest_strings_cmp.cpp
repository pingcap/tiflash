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

namespace DB
{
namespace tests
{
class Strcmp : public DB::tests::FunctionTest
{
};

TEST_F(Strcmp, Strcmp)
try
{
    // without collation
    {
        // column with column
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1, 1, 0, 0}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})}));

        // column with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1, 1, -1, std::nullopt, std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<Nullable<String>>({"1", "123", "123", "123", std::nullopt}),
                 createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})}));

        // nullable with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1, 1, 0, std::nullopt, std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<Nullable<String>>({"", "123", "", "", std::nullopt}),
                 createColumn<Nullable<String>>({"123", "", "", std::nullopt, ""})}));

        // column with constant
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1, 0, 1, std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<Nullable<String>>({"a", "b", "c", std::nullopt}),
                 createConstColumn<Nullable<String>>(4, "b")}));

        // constant with column
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(4, "b"),
                 createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})}));

        // constant with constant
        ASSERT_COLUMN_EQ(
            createConstColumn<Int8>(1, -1),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "b")}));

        // constant with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1}),
            executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})}));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({std::nullopt}),
            executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})}));
    }

    // with collation
    {
        // column with column
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1, 1, 0, 0}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1, 1, 0, 0}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)));

        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1, 1, 0, 0}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"B", "A", "A", ""})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // column with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1, 1, -1, std::nullopt, std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<Nullable<String>>({"1", "123", "123", "123", std::nullopt}),
                 createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // nullable with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1, 1, 0, std::nullopt, std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<Nullable<String>>({"", "123", "", "", std::nullopt}),
                 createColumn<Nullable<String>>({"123", "", "", std::nullopt, ""})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // column with constant
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a"}), createConstColumn<Nullable<String>>(1, "b")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A"}), createConstColumn<Nullable<String>>(1, "b")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A"}), createConstColumn<Nullable<String>>(1, "B")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Int8>({-1, 0, 1, 1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A", "B", "C", "D"}), createConstColumn<Nullable<String>>(4, "B")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        {
            std::string src(35, char(2));
            std::string tar1 = src;
            std::string tar2 = tar1;
            std::string tar3 = tar1;
            tar2.back() = char(1);
            tar3.back() = char(3);
            ASSERT_COLUMN_EQ(
                createColumn<Int8>({-1, 0, 1}),
                executeFunction(
                    "strcmp",
                    {createColumn<String>({tar2, tar1, tar3}), createConstColumn<Nullable<String>>(3, src)},
                    TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)));
        }

        // constant with column
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(4, "b"),
                 createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(4, "B"),
                 createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(4, "b"),
                 createColumn<Nullable<String>>({"A", "B", "C", std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // constant with constant
        ASSERT_COLUMN_EQ(
            createConstColumn<Int8>(1, -1),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "b")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int8>(1, -1),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(1, "A"), createConstColumn<Nullable<String>>(1, "b")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int8>(1, -1),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "B")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int8>(1, -1),
            executeFunction(
                "strcmp",
                {createConstColumn<Nullable<String>>(1, "A"), createConstColumn<Nullable<String>>(1, "B")},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // constant with nullable
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A"}), createColumn<Nullable<String>>({"b"})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a"}), createColumn<Nullable<String>>({"B"})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({-1}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A"}), createColumn<Nullable<String>>({"B"})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int8>>({std::nullopt}),
            executeFunction(
                "strcmp",
                {createColumn<String>({"A"}), createColumn<Nullable<String>>({std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
    }
}
CATCH

TEST_F(Strcmp, StrEq)
try
{
    const std::string equals_fn_name = "equals";
    for (const auto col_id : {
             TiDB::ITiDBCollator::UTF8MB4_BIN,
             TiDB::ITiDBCollator::BINARY,
         })
    {
        const auto * collator = TiDB::ITiDBCollator::getCollator(col_id);

        // with collation
        {
            // column : column
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>({0, 0, 1, 1, 0}),
                executeFunction(
                    equals_fn_name,
                    {createColumn<String>({"a", "b", "a", "", "00"}), createColumn<String>({"b", "a", "a", "", "0"})},
                    collator));
            // column : constant
            // constant : column
            {
                for (size_t size = 1; size < 35; ++size)
                {
                    std::string a(size, '0');
                    std::string b(size, '0');
                    b.back() = '1';
                    ASSERT_COLUMN_EQ(
                        createColumn<UInt8>({1, 0}),
                        executeFunction(
                            equals_fn_name,
                            {createConstColumn<String>(2, a), createColumn<String>({a, b})},
                            collator));
                    ASSERT_COLUMN_EQ(
                        createColumn<UInt8>({1, 0}),
                        executeFunction(
                            equals_fn_name,
                            {createColumn<String>({a, b}), createConstColumn<String>(2, a)},
                            collator));
                    {
                        auto c = a;
                        a += " ";
                        b += "    ";
                        c += "  ";
                        if (!collator->isBinary())
                        {
                            ASSERT_COLUMN_EQ(
                                createColumn<UInt8>({1, 0}),
                                executeFunction(
                                    equals_fn_name,
                                    {createConstColumn<String>(2, c), createColumn<String>({a, b})},
                                    collator));
                            ASSERT_COLUMN_EQ(
                                createColumn<UInt8>({1, 0}),
                                executeFunction(
                                    equals_fn_name,
                                    {createColumn<String>({a, b}), createConstColumn<String>(2, c)},
                                    collator));
                        }
                        else
                        {
                            ASSERT_COLUMN_EQ(
                                createColumn<UInt8>({0, 0, 1}),
                                executeFunction(
                                    equals_fn_name,
                                    {createConstColumn<String>(3, c), createColumn<String>({a, b, c})},
                                    collator));
                            ASSERT_COLUMN_EQ(
                                createColumn<UInt8>({0, 0, 1}),
                                executeFunction(
                                    equals_fn_name,
                                    {createColumn<String>({a, b, c}), createConstColumn<String>(3, c)},
                                    collator));
                        }
                    }
                }
            }

            // column : nullable
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>({0, 0, 1, std::nullopt, std::nullopt}),
                executeFunction(
                    equals_fn_name,
                    {createColumn<Nullable<String>>({"1", "123", "45", "123", std::nullopt}),
                     createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})},
                    collator));

            // column with constant
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>({0, 1, std::nullopt}),
                executeFunction(
                    equals_fn_name,
                    {createColumn<Nullable<String>>({"aa", "bb", std::nullopt}),
                     createConstColumn<Nullable<String>>(3, "bb")},
                    collator));

            // constant with column
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>({0, 1, std::nullopt}),
                executeFunction(
                    equals_fn_name,
                    {createConstColumn<Nullable<String>>(3, "bb"),
                     createColumn<Nullable<String>>({"aa", "bb", std::nullopt})},
                    collator));

            // constant with nullable
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>({0}),
                executeFunction(
                    equals_fn_name,
                    {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})},
                    collator));
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>({std::nullopt}),
                executeFunction(
                    equals_fn_name,
                    {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})},
                    collator));
        }
    }
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<UInt8>>({0, 1, std::nullopt}),
            executeFunction(
                equals_fn_name,
                {createConstColumn<String>(3, ""), createColumn<Nullable<String>>({"123", "", std::nullopt})},
                TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN)));
    }
}
CATCH
} // namespace tests
} // namespace DB
