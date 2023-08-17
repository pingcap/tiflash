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

#pragma once

#include <Debug/MockExecutor/AstToPB.h>
#include <Parsers/IAST.h>
#include <tipb/executor.pb.h>

namespace DB::mock
{
class MockRuntimeFilter
{
public:
    MockRuntimeFilter(
        int id_,
        ASTPtr source_expr_,
        ASTPtr target_expr_,
        const std::string & source_executor_id_,
        const std::string & target_executor_id_)
        : id(id_)
        , source_expr(source_expr_)
        , target_expr(target_expr_)
        , source_executor_id(source_executor_id_)
        , target_executor_id(target_executor_id_)
    {}
    void toPB(
        const DAGSchema & source_schema,
        const DAGSchema & target_schema,
        int32_t collator_id,
        const Context & context,
        tipb::RuntimeFilter * rf);

private:
    int id;
    ASTPtr source_expr;
    ASTPtr target_expr;
    std::string source_executor_id;
    std::string target_executor_id;
};
} // namespace DB::mock
