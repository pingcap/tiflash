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

#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDBGInvokeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int READONLY;
extern const int BAD_ARGUMENTS;
} // namespace DB::ErrorCodes

namespace DB::tests
{
namespace
{
ASTPtr parseDBGInvoke(const String & sql)
{
    ParserQuery parser(sql.data() + sql.size());
    return parseQuery(parser, sql, "gtest DBGInvoke", 0);
}
} // namespace

TEST(InterpreterDBGInvokeQueryTest, ReadonlyRejectsBeforeInvoke)
{
    auto ctx = TiFlashTestEnv::getContext();
    ctx->setSetting("readonly", "1");
    const ASTPtr ast = parseDBGInvoke("DBGInvoke foo()");
    try
    {
        InterpreterDBGInvokeQuery(ast, *ctx).execute();
        FAIL() << "DBGInvoke should be rejected when readonly = 1";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::READONLY) << e.message();
    }
}

TEST(InterpreterDBGInvokeQueryTest, WritableReachesInvoker)
{
    auto ctx = TiFlashTestEnv::getContext();
    ASSERT_EQ(ctx->getSettingsRef().readonly, 0);
    const ASTPtr ast = parseDBGInvoke("DBGInvoke foo()");
    try
    {
        InterpreterDBGInvokeQuery(ast, *ctx).execute();
        FAIL() << "unknown DBG function should still fail";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << e.message();
    }
}
} // namespace DB::tests
