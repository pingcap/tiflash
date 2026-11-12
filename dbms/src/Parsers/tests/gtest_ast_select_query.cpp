// Copyright 2026 PingCAP, Inc.
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

#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
ASTPtr parseSelect(const String & sql)
{
    ParserQuery parser(sql.data() + sql.size());
    const ASTPtr ast = parseQuery(parser, sql, "select format test", DEFAULT_MAX_QUERY_SIZE);
    const auto * select_union = typeid_cast<const ASTSelectWithUnionQuery *>(ast.get());
    EXPECT_NE(select_union, nullptr);
    if (!select_union || select_union->list_of_selects->children.size() != 1)
        return nullptr;
    return select_union->list_of_selects->children[0];
}
} // namespace

// PARTITION and SEGMENT are parsed into different members. Formatting a query that has only
// PARTITION used to guard the SEGMENT block with partition_expression_list and then dereference
// a null segment_expression_list.
TEST(ASTSelectQueryFormatTest, FormatsPartitionWithoutSegment)
{
    const ASTPtr select_ast = parseSelect("SELECT 1 FROM t PARTITION 1");
    const auto * select = typeid_cast<const ASTSelectQuery *>(select_ast.get());
    ASSERT_NE(select, nullptr);
    ASSERT_TRUE(select->partition_expression_list);
    ASSERT_FALSE(select->segment_expression_list);

    const String formatted = queryToString(*select);
    EXPECT_NE(formatted.find("PARTITION"), String::npos) << formatted;
    EXPECT_EQ(formatted.find("SEGMENT"), String::npos) << formatted;
}

TEST(ASTSelectQueryFormatTest, FormatsSegmentWithoutPartition)
{
    const ASTPtr select_ast = parseSelect("SELECT 1 FROM t SEGMENT 2");
    const auto * select = typeid_cast<const ASTSelectQuery *>(select_ast.get());
    ASSERT_NE(select, nullptr);
    ASSERT_FALSE(select->partition_expression_list);
    ASSERT_TRUE(select->segment_expression_list);

    const String formatted = queryToString(*select);
    EXPECT_EQ(formatted.find("PARTITION"), String::npos) << formatted;
    EXPECT_NE(formatted.find("SEGMENT"), String::npos) << formatted;
}

TEST(ASTSelectQueryFormatTest, FormatsPartitionAndSegment)
{
    const ASTPtr select_ast = parseSelect("SELECT 1 FROM t PARTITION 1 SEGMENT 2");
    const auto * select = typeid_cast<const ASTSelectQuery *>(select_ast.get());
    ASSERT_NE(select, nullptr);
    ASSERT_TRUE(select->partition_expression_list);
    ASSERT_TRUE(select->segment_expression_list);

    const String formatted = queryToString(*select);
    const auto partition_pos = formatted.find("PARTITION");
    const auto segment_pos = formatted.find("SEGMENT");
    EXPECT_NE(partition_pos, String::npos) << formatted;
    EXPECT_NE(segment_pos, String::npos) << formatted;
    EXPECT_LT(partition_pos, segment_pos) << formatted;
}

// Alias-collision diagnostics format the user AST, including a subquery that carries PARTITION.
TEST(ASTSelectQueryFormatTest, FormatsSubqueryPartitionWithoutSegment)
{
    const String sql = "SELECT (SELECT 1 FROM system.one PARTITION (1)) AS a, 2 AS a";
    ParserQuery parser(sql.data() + sql.size());
    const ASTPtr ast = parseQuery(parser, sql, "select format test", DEFAULT_MAX_QUERY_SIZE);
    const String formatted = queryToString(ast);
    EXPECT_NE(formatted.find("PARTITION"), String::npos) << formatted;
    EXPECT_EQ(formatted.find("SEGMENT"), String::npos) << formatted;
}
} // namespace DB::tests
