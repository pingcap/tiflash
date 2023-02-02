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

namespace DB
{
void PipelineExecBuilder::setSourceOp(SourceOpPtr && source_op_)
{
    assert(!source_op && source_op_);
    source_op = std::move(source_op_);
}
void PipelineExecBuilder::appendTransformOp(TransformOpPtr && transform_op)
{
    assert(source_op && transform_op);
    Block header = getCurrentHeader();
    transform_op->transformHeader(header);
    transform_ops.push_back(std::move(transform_op));
}
void PipelineExecBuilder::setSinkOp(SinkOpPtr && sink_op_)
{
    assert(!sink_op && sink_op_);
    Block header = getCurrentHeader();
    sink_op_->setHeader(header);
    sink_op = std::move(sink_op_);
}

PipelineExecPtr PipelineExecBuilder::build()
{
    assert(source_op && sink_op);
    return std::make_unique<PipelineExec>(
        std::move(source_op),
        std::move(transform_ops),
        std::move(sink_op));
}

Block PipelineExecBuilder::getCurrentHeader() const
{
    if (sink_op)
        return sink_op->getHeader();
    else if (!transform_ops.empty())
        return transform_ops.back()->getHeader();
    else
    {
        assert(source_op);
        return source_op->getHeader();
    }
}

void PipelineExecGroupBuilder::addGroup(size_t init_concurrency)
{
    assert(init_concurrency > 0);
    ++cur_group_index;
    assert(cur_group_index >= 0);
    BuilderGroup group;
    group.resize(init_concurrency);
    groups.push_back(std::move(group));
}


PipelineExecGroups PipelineExecGroupBuilder::build()
{
    assert(!groups.empty());
    PipelineExecGroups pipeline_exec_groups;
    for (auto & group : groups)
    {
        assert(!group.empty());
        PipelineExecGroup pipeline_exec_group;
        for (auto & builder : group)
            pipeline_exec_group.push_back(builder.build());
        pipeline_exec_groups.push_back(std::move(pipeline_exec_group));
    }
    return pipeline_exec_groups;
}

Block PipelineExecGroupBuilder::getCurrentHeader()
{
    assert(cur_group_index >= 0 && groups.size() > static_cast<size_t>(cur_group_index));
    const auto & group = groups[cur_group_index];
    assert(!group.empty());
    return group.back().getCurrentHeader();
}

size_t PipelineExecGroupBuilder::getCurrentConcurrency() const
{
    assert(cur_group_index >= 0 && groups.size() > static_cast<size_t>(cur_group_index));
    const auto & group = groups[cur_group_index];
    assert(!group.empty());
    return group.size();
}
} // namespace DB
