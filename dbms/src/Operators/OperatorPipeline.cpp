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

#include <Flash/Pipeline/PipelineExecStatus.h>
#include <Operators/OperatorPipeline.h>

namespace DB
{
#define CHECK_IS_CANCELLED(exec_status)          \
    if (unlikely((exec_status).isCancelled()))   \
        return OperatorStatus::CANCELLED;

/**
 *     sink     transform     ...     transform    source
 *
 *  prepare───►fetchBlock───► ... ───►fetchBlock───►read────┐
 *                                                          │ block
 *    write◄────transform◄─── ... ◄───transform◄────────────┘
 */
OperatorStatus OperatorPipeline::execute(PipelineExecStatus & exec_status)
{
    Block block;
    size_t transform_index = 0;
    auto status = fetchBlock(block, transform_index, exec_status);
    if (status != OperatorStatus::PASS)
        return status;

    for (; transform_index < transforms.size(); ++transform_index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto status = transforms[transform_index]->transform(block);
        if (status != OperatorStatus::PASS)
            return pushSpiller(status, transforms[transform_index]);
    }
    CHECK_IS_CANCELLED(exec_status);
    return pushSpiller(sink->write(std::move(block)), sink);
}

OperatorStatus OperatorPipeline::fetchBlock(Block & block, size_t & transform_index, PipelineExecStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);
    auto status = sink->prepare();
    if (status != OperatorStatus::PASS)
        return pushSpiller(status, sink);
    for (int64_t index = transforms.size() - 1; index >= 0; --index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto status = transforms[index]->fetchBlock(block);
        if (status != OperatorStatus::NO_OUTPUT)
        {
            transform_index = index + 1;
            return pushSpiller(status, transforms[index]);
        }
    }
    CHECK_IS_CANCELLED(exec_status);
    transform_index = 0;
    return pushSpiller(source->read(block), source);
}

OperatorStatus OperatorPipeline::await(PipelineExecStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);

    auto status = sink->await();
    if (status != OperatorStatus::PASS)
        return status;
    for (auto it = transforms.rbegin(); it != transforms.rend(); ++it)
    {
        auto status = (*it)->await();
        if (status != OperatorStatus::SKIP)
            return status;
    }
    return source->await();
}

OperatorStatus OperatorPipeline::spill(PipelineExecStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);

    assert(spiller);
    assert(*spiller);
    auto status = (*spiller)->spill();
    if (status != OperatorStatus::SPILLING)
        spiller.reset();
    return status;
}

#undef CHECK_IS_CANCELLED

} // namespace DB
