// Copyright 2023 PingCAP, Ltd.
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

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalGetResultSink.h>
#include <Operators/GetResultSinkOp.h>
#include <Operators/SharedQueue.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalGetResultSink::build(
    ResultHandler && result_handler,
    const PhysicalPlanNodePtr & child)
{
    auto get_result_sink = std::make_shared<PhysicalGetResultSink>("get_result_sink", child->getSchema(), "", child, std::move(result_handler));
    get_result_sink->disableRestoreConcurrency();
    return get_result_sink;
}

void PhysicalGetResultSink::buildPipelineExecImpl(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t /*concurrency*/)
{
    auto cur_concurrency = group_builder.getCurrentConcurrency();
    assert(cur_concurrency >= 1);
    if (cur_concurrency == 1)
    {
        group_builder.transform([&](auto & builder) {
            builder.setSinkOp(std::make_unique<GetResultSinkOp>(std::move(result_handler)));
        });
    }
    else
    {
        auto shared_queue = SharedQueue::build(cur_concurrency, 1);
        group_builder.transform([&](auto & builder) {
            builder.setSinkOp(std::make_unique<SharedQueueSinkOp>(group_builder.exec_status, shared_queue));
        });
        auto cur_header = group_builder.getCurrentHeader();
        group_builder.addGroup(1);
        assert(1 == group_builder.getCurrentConcurrency());
        group_builder.transform([&](auto & builder) {
            builder.setSourceOp(std::make_unique<SharedQueueSourceOp>(group_builder.exec_status, cur_header, shared_queue));
            builder.setSinkOp(std::make_unique<GetResultSinkOp>(group_builder.exec_status, std::move(result_handler)));
        });
    }
}
} // namespace DB
