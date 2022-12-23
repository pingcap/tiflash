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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{
class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;
class DAGContext;

/** build ch plan from dag request: dag executors -> ch plan
  */
class InterpreterDAG : public IInterpreter
{
public:
    InterpreterDAG(Context & context_, const DAGQuerySource & dag_);

    ~InterpreterDAG() = default;

    BlockIO execute() override;

private:
    BlockInputStreams executeQueryBlock(DAGQueryBlock & query_block);

    DAGContext & dagContext() const;

    Context & context;
    const DAGQuerySource & dag;
    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
};
} // namespace DB
