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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>
#include <tipb/executor.pb.h>

namespace DB
{
/**
 * A physical plan node that generates MockExchangeReceiverInputStream.
 * Used in gtest to test execution logic.
 * Only available with `context.isExecutorTest() == true || context.isInterpreterTest() == true`.
 */
class PhysicalMockExchangeReceiver : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::ExchangeReceiver & exchange_receiver,
        const FineGrainedShuffle & fine_grained_shuffle);

    PhysicalMockExchangeReceiver(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const Block & sample_block_,
        const BlockInputStreams & mock_streams,
        size_t source_num_);

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    size_t getSourceNum() const { return source_num; };

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    Block sample_block;

    BlockInputStreams mock_streams;
    size_t source_num = 0;
};
} // namespace DB
