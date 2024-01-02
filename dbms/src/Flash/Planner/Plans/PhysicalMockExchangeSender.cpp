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

#include <Common/Logger.h>
#include <DataStreams/MockExchangeSenderInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/Plans/PhysicalMockExchangeSender.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalMockExchangeSender::build(
    const String & executor_id,
    const LoggerPtr & log,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    auto physical_mock_exchange_sender = std::make_shared<PhysicalMockExchangeSender>(
        executor_id,
        child->getSchema(),
        FineGrainedShuffle{},
        log->identifier(),
        child);
    // executeUnion will be call after sender.transform, so don't need to restore concurrency.
    physical_mock_exchange_sender->disableRestoreConcurrency();
    return physical_mock_exchange_sender;
}

void PhysicalMockExchangeSender::buildBlockInputStreamImpl(
    DAGPipeline & pipeline,
    Context & context,
    size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<MockExchangeSenderInputStream>(stream, log->identifier()); });
}

void PhysicalMockExchangeSender::finalizeImpl(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalMockExchangeSender::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
