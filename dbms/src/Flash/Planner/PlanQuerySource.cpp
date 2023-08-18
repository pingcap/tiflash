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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Planner/PlanQuerySource.h>
#include <Flash/Planner/Planner.h>
#include <Interpreters/Context.h>
#include <Parsers/makeDummyQuery.h>

namespace DB
{
PlanQuerySource::PlanQuerySource(Context & context_)
    : context(context_)
{}

std::tuple<std::string, ASTPtr> PlanQuerySource::parse(size_t)
{
    // this is a way to avoid NPE when the MergeTreeDataSelectExecutor trying
    // to extract key range of the query.
    // todo find a way to enable key range extraction for dag query
    return {getDAGContext().dummy_query_string, getDAGContext().dummy_ast};
}

String PlanQuerySource::str(size_t)
{
    return getDAGContext().dummy_query_string;
}

std::unique_ptr<IInterpreter> PlanQuerySource::interpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<Planner>(context, *this);
}

const tipb::DAGRequest & PlanQuerySource::getDAGRequest() const
{
    return *getDAGContext().dag_request;
}

DAGContext & PlanQuerySource::getDAGContext() const
{
    return *context.getDAGContext();
}

} // namespace DB
