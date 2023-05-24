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
    RUNTIME_CHECK(!source_op && source_op_);
    source_op = std::move(source_op_);
}
void PipelineExecBuilder::appendTransformOp(TransformOpPtr && transform_op)
{
    RUNTIME_CHECK(source_op && transform_op);
    Block header = getCurrentHeader();
    transform_op->transformHeader(header);
    transform_ops.push_back(std::move(transform_op));
}
void PipelineExecBuilder::setSinkOp(SinkOpPtr && sink_op_)
{
    RUNTIME_CHECK(!sink_op && sink_op_);
    Block header = getCurrentHeader();
    sink_op_->setHeader(header);
    sink_op = std::move(sink_op_);
}

PipelineExecPtr PipelineExecBuilder::build()
{
    RUNTIME_CHECK(source_op && sink_op);
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
        RUNTIME_CHECK(source_op);
        return source_op->getHeader();
    }
}

void PipelineExecGroupBuilder::addConcurrency(SourceOpPtr && source)
{
    getCurGroup().emplace_back();
    getCurGroup().back().setSourceOp(std::move(source));
}

void PipelineExecGroupBuilder::reset()
{
    groups.clear();
}

void PipelineExecGroupBuilder::merge(PipelineExecGroupBuilder && other)
{
    RUNTIME_CHECK(groups.size() == other.groups.size());
    size_t group_num = groups.size();
    for (size_t i = 0; i < group_num; ++i)
        groups[i].insert(groups[i].end(), std::make_move_iterator(other.groups[i].begin()), std::make_move_iterator(other.groups[i].end()));
}

PipelineExecGroup PipelineExecGroupBuilder::build()
{
    RUNTIME_CHECK(!groups.empty());
    PipelineExecGroup pipeline_exec_group;
    for (auto & group : groups)
    {
        RUNTIME_CHECK(!group.empty());
        for (auto & builder : group)
            pipeline_exec_group.push_back(builder.build());
    }
    return pipeline_exec_group;
}

Block PipelineExecGroupBuilder::getCurrentHeader()
{
    RUNTIME_CHECK(!getCurGroup().empty());
    return getCurGroup().back().getCurrentHeader();
}
} // namespace DB
