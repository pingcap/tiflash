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

#include <Common/Exception.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int TOO_DEEP_RECURSION;
}

namespace DB::tests
{
namespace
{
ASTPtr parseSQL(const String & sql)
{
    ParserQuery parser(sql.data() + sql.size());
    return parseQuery(parser, sql, "parser depth test", DEFAULT_MAX_QUERY_SIZE);
}

template <typename F>
void expectTooDeep(F && parse)
{
    try
    {
        parse();
        FAIL() << "Expected the parser depth limit to reject the input";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_DEEP_RECURSION) << e.message();
    }
}

// Recreate the parser and copy its position on every call, as the SQL grammar does.
class RecursiveParser : public IParserBase
{
public:
    explicit RecursiveParser(size_t remaining_, bool succeed_ = true)
        : remaining(remaining_)
        , succeed(succeed_)
    {}

private:
    const char * getName() const override { return "recursive parser for depth tests"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        if (remaining == 1)
            return succeed;

        RecursiveParser nested(remaining - 1, succeed);
        auto nested_pos = pos;
        return nested.parse(nested_pos, node, expected);
    }

    size_t remaining;
    bool succeed;
};
} // namespace

TEST(ParserDepthTest, RejectsDeepQueries)
{
    constexpr size_t nesting = 10000;
    const std::vector<String> queries{
        "SELECT " + String(nesting, '(') + "1" + String(nesting, ')'),
        "SELECT " + String(nesting, '('),
        String(nesting, '(') + "SELECT 1" + String(nesting, ')'),
        "SELECT " + String(nesting, '[') + "1" + String(nesting, ']'),
    };
    for (const auto & sql : queries)
        expectTooDeep([&] { parseSQL(sql); });

    String functions = "SELECT ";
    for (size_t i = 0; i < nesting; ++i)
        functions += "f(";
    functions += "1" + String(nesting, ')');
    expectTooDeep([&] { parseSQL(functions); });
}

TEST(ParserDepthTest, ParsesNormalQueries)
{
    const std::vector<String> queries{
        "SELECT 1",
        "SELECT (((1 + 2) * 3))",
        "SELECT f(g(h(1))), [[1, 2], [3, 4]]",
        "SELECT * FROM (SELECT * FROM (SELECT 1))",
        "SELECT CASE WHEN 1 THEN f(2) ELSE g(3) END",
        "SELECT * FROM system.dt_local_indexes WHERE tidb_database IN ('db') AND tidb_table IN ('t') LIMIT 0, 1024",
        "CREATE TABLE t (a Nullable(Int64), b Array(Nullable(String))) ENGINE = Memory",
    };
    for (const auto & sql : queries)
    {
        SCOPED_TRACE(sql);
        ASSERT_NO_THROW(parseSQL(sql));
    }
}

TEST(ParserDepthTest, DoesNotAccumulateSiblingCalls)
{
    String sql = "SELECT 1";
    for (size_t i = 0; i < IParserBase::maxParserDepth * 2; ++i)
        sql += ", (1)";
    ASSERT_NO_THROW(parseSQL(sql));
}

TEST(ParserDepthTest, RestoresDepthAfterSuccessFailureAndException)
{
    const String sql = "1";
    Tokens tokens(sql.data(), sql.data() + sql.size());
    TokenIterator pos(tokens);
    const auto begin = pos;
    ASTPtr node;
    Expected expected;

    RecursiveParser at_limit(IParserBase::maxParserDepth);
    ASSERT_TRUE(at_limit.parse(pos, node, expected));
    ASSERT_TRUE(at_limit.parse(pos, node, expected));

    RecursiveParser failure(IParserBase::maxParserDepth, false);
    ASSERT_FALSE(failure.parse(pos, node, expected));
    ASSERT_TRUE(pos == begin);
    ASSERT_TRUE(at_limit.parse(pos, node, expected));

    RecursiveParser over_limit(IParserBase::maxParserDepth + 1);
    expectTooDeep([&] { over_limit.parse(pos, node, expected); });
    ASSERT_TRUE(at_limit.parse(pos, node, expected));
}
} // namespace DB::tests
