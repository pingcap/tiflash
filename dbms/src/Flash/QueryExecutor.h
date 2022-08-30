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

#include <Core/Block.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Pipeline/DAGScheduler.h>

#include <functional>
#include <memory>
#include <utility>

namespace DB
{
using ResultHandler = std::function<void(const Block &)>;

class QueryExecutor
{
public:
    const static ResultHandler default_result_handler;

    QueryExecutor() = default;
    virtual ~QueryExecutor() = default;

    // is_success, err_msg
    virtual std::pair<bool, String> execute(ResultHandler) = 0;

    std::pair<bool, String> execute()
    {
        return execute(default_result_handler);
    }
};

using QueryExecutorPtr = std::shared_ptr<QueryExecutor>;

class DataStreamExecutor : public QueryExecutor
{
public:
    explicit DataStreamExecutor(const BlockIO & block_io)
        : QueryExecutor()
        , data_stream(block_io.in)
    {}

    std::pair<bool, String> execute(ResultHandler result_handler) override;

    BlockInputStreamPtr dataStream() const;

private:
    BlockInputStreamPtr data_stream;
};

class PipelineExecutor : public QueryExecutor
{
public:
    explicit PipelineExecutor(
        Context & context,
        const PhysicalPlanNodePtr & plan_node_,
        size_t max_streams,
        const String & req_id)
        : QueryExecutor()
        , dag_scheduler(context, max_streams, req_id)
        , plan_node(plan_node_)
    {}

    std::pair<bool, String> execute(ResultHandler) override;

private:
    DAGScheduler dag_scheduler;

    PhysicalPlanNodePtr plan_node;
};
}
