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
        ASSERT_COLUMN_EQ(createColumn<Int8>({-1, 1, 0, 0}), executeFunction("strcmp", {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})}));

        // column with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, -1, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"1", "123", "123", "123", std::nullopt}), createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})}));

        // nullable with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, 0, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"", "123", "", "", std::nullopt}), createColumn<Nullable<String>>({"123", "", "", std::nullopt, ""})}));

        // column with constant
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 0, 1, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"a", "b", "c", std::nullopt}), createConstColumn<Nullable<String>>(4, "b")}));

        // constant with column
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}), executeFunction("strcmp", {createConstColumn<Nullable<String>>(4, "b"), createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})}));

        // constant with constant
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "b")}));

        // constant with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})}));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({std::nullopt}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})}));
    }

    // with collation
    {
        // column with column
        ASSERT_COLUMN_EQ(createColumn<Int8>({-1, 1, 0, 0}), executeFunction("strcmp", {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Int8>({-1, 1, 0, 0}), executeFunction("strcmp", {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)));

        ASSERT_COLUMN_EQ(createColumn<Int8>({-1, 1, 0, 0}), executeFunction("strcmp", {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"B", "A", "A", ""})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // column with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, -1, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"1", "123", "123", "123", std::nullopt}), createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // nullable with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, 0, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"", "123", "", "", std::nullopt}), createColumn<Nullable<String>>({"123", "", "", std::nullopt, ""})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // column with constant
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createConstColumn<Nullable<String>>(1, "b")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"A"}), createConstColumn<Nullable<String>>(1, "b")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"A"}), createConstColumn<Nullable<String>>(1, "B")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 0, 1, 1}), executeFunction("strcmp", {createColumn<String>({"A", "B", "C", "D"}), createConstColumn<Nullable<String>>(4, "B")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // constant with column
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}), executeFunction("strcmp", {createConstColumn<Nullable<String>>(4, "b"), createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}), executeFunction("strcmp", {createConstColumn<Nullable<String>>(4, "B"), createColumn<Nullable<String>>({"a", "b", "c", std::nullopt})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({1, 0, -1, std::nullopt}), executeFunction("strcmp", {createConstColumn<Nullable<String>>(4, "b"), createColumn<Nullable<String>>({"A", "B", "C", std::nullopt})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // constant with constant
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "b")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "A"), createConstColumn<Nullable<String>>(1, "b")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "B")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "A"), createConstColumn<Nullable<String>>(1, "B")}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        // constant with nullable
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"A"}), createColumn<Nullable<String>>({"b"})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({"B"})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"A"}), createColumn<Nullable<String>>({"B"})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));

        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({std::nullopt}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
        ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({std::nullopt}), executeFunction("strcmp", {createColumn<String>({"A"}), createColumn<Nullable<String>>({std::nullopt})}, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI)));
    }
}
CATCH
} // namespace tests
} // namespace DB
