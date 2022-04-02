// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/IQuerySource.h>

namespace DB
{
/// DAGQuerySource is an adaptor between DAG and CH's executeQuery.
/// TODO: consider to directly use DAGContext instead.
class DAGQuerySource : public IQuerySource
{
public:
    explicit DAGQuerySource(Context & context_);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    std::shared_ptr<DAGQueryBlock> getRootQueryBlock() const { return root_query_block; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

private:
    Context & context;
    std::shared_ptr<DAGQueryBlock> root_query_block;
};

} // namespace DB
