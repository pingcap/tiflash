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

#include <Flash/Pipeline/GetResultSink.h>
#include <Operators/OperatorBuilder.h>

namespace DB
{
OperatorStatus GetResultSink::write(Block && block)
{
    if (!block)
        return OperatorStatus::FINISHED;
    std::lock_guard lock(physical_sink.mu);
    physical_sink.result_handler(block);
    return OperatorStatus::PASS;
}

PhysicalPlanNodePtr PhysicalGetResultSink::build(
    ResultHandler result_handler,
    const PhysicalPlanNodePtr & child)
{
    return std::make_shared<PhysicalGetResultSink>("get_result_sink", child->getSchema(), "", child, result_handler);
}

void PhysicalGetResultSink::transform(OperatorsBuilder & op_builder, Context & /*context*/, size_t /*concurrency*/)
{
    op_builder.transform([&](auto & builder) {
        builder.setSink(std::make_unique<GetResultSink>(*this));
    });
}
} // namespace DB
