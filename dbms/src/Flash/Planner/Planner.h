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

#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>

namespace DB
{
class Context;
class DAGContext;

class Planner
{
public:
    Planner(
        Context & context_,
        const std::vector<BlockInputStreams> & input_streams_vec_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_);

    ~Planner() = default;

    BlockInputStreams execute();

    static bool isSupported(const DAGQueryBlock & query_block);

private:
    DAGContext & dagContext() const;

    void restorePipelineConcurrency(DAGPipeline & pipeline);

    void executeImpl(DAGPipeline & pipeline);

private:
    Context & context;

    std::vector<BlockInputStreams> input_streams_vec;

    const DAGQueryBlock & query_block;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    LoggerPtr log;
};
} // namespace DB