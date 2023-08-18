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
#include <Flash/Planner/plans/PhysicalMockExchangeSender.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalMockExchangeSender::build(
    const String & executor_id,
    const LoggerPtr & log,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    auto physical_mock_exchange_sender = std::make_shared<PhysicalMockExchangeSender>(
        executor_id,
        child->getSchema(),
        log->identifier(),
        child);
    // executeUnion will be call after sender.transform, so don't need to restore concurrency.
    physical_mock_exchange_sender->disableRestoreConcurrency();
    return physical_mock_exchange_sender;
}

<<<<<<< HEAD:dbms/src/Flash/Planner/plans/PhysicalMockExchangeSender.cpp
void PhysicalMockExchangeSender::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
=======
void PhysicalMockExchangeSender::buildBlockInputStreamImpl(
    DAGPipeline & pipeline,
    Context & context,
    size_t max_streams)
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Planner/Plans/PhysicalMockExchangeSender.cpp
{
    child->transform(pipeline, context, max_streams);

    pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<MockExchangeSenderInputStream>(stream, log->identifier()); });
}

void PhysicalMockExchangeSender::finalize(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalMockExchangeSender::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
