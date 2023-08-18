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

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{
class InterpreterSelectQuery;


/** Interprets one or multiple SELECT queries inside UNION ALL chain.
  */
class InterpreterSelectWithUnionQuery : public IInterpreter
{
public:
    InterpreterSelectWithUnionQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const Names & required_result_column_names = Names{},
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0);

    ~InterpreterSelectWithUnionQuery();

    BlockIO execute() override;

    /// Execute the query without union of streams.
    BlockInputStreams executeWithMultipleStreams();

    Block getSampleBlock();

    static Block getSampleBlock(const ASTPtr & query_ptr_, const Context & context_);

private:
    ASTPtr query_ptr;
    Context context;
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;

    std::vector<std::unique_ptr<InterpreterSelectQuery>> nested_interpreters;

    Block result_header;
};

} // namespace DB
