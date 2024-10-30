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

PipelineExecPtr PipelineExecBuilder::build(bool internal_break_time, uint64_t minTSO_time_in_ms)
{
    RUNTIME_CHECK(source_op && sink_op);
    return std::make_unique<PipelineExec>(
        std::move(source_op),
        std::move(transform_ops),
        std::move(sink_op),
        internal_break_time,
        minTSO_time_in_ms);
}

OperatorProfileInfoPtr PipelineExecBuilder::getCurProfileInfo() const
{
    if (sink_op)
        return sink_op->getProfileInfo();
    else if (!transform_ops.empty())
        return transform_ops.back()->getProfileInfo();
    else
    {
        RUNTIME_CHECK(source_op);
        return source_op->getProfileInfo();
    }
}

IOProfileInfoPtr PipelineExecBuilder::getCurIOProfileInfo() const
{
    if (sink_op)
        return sink_op->getIOProfileInfo();
    else if (!transform_ops.empty())
        return transform_ops.back()->getIOProfileInfo();
    else
    {
        RUNTIME_CHECK(source_op);
        return source_op->getIOProfileInfo();
    }
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
    auto & cur_group = getCurGroup();
    cur_group.emplace_back();
    cur_group.back().setSourceOp(std::move(source));
}

void PipelineExecGroupBuilder::addConcurrency(PipelineExecBuilder && exec_builder)
{
    RUNTIME_CHECK(exec_builder.source_op);
    auto & cur_group = getCurGroup();
    cur_group.push_back(std::move(exec_builder));
}

void PipelineExecGroupBuilder::reset()
{
    groups.clear();
    // Re-add an empty group to ensure that the group builder after reset is available.
    groups.emplace_back();
}

void PipelineExecGroupBuilder::merge(PipelineExecGroupBuilder && other)
{
    RUNTIME_CHECK(groups.size() == other.groups.size());
    size_t group_num = groups.size();
    for (size_t i = 0; i < group_num; ++i)
        groups[i].insert(
            groups[i].end(),
            std::make_move_iterator(other.groups[i].begin()),
            std::make_move_iterator(other.groups[i].end()));
}

PipelineExecGroup PipelineExecGroupBuilder::build(bool internal_break_time, uint64_t minTSO_time_in_ms)
{
    RUNTIME_CHECK(!groups.empty());
    PipelineExecGroup pipeline_exec_group;
    for (auto & group : groups)
    {
        RUNTIME_CHECK(!group.empty());
        for (auto & builder : group)
            pipeline_exec_group.push_back(builder.build(internal_break_time, minTSO_time_in_ms));
    }
    return pipeline_exec_group;
}

Block PipelineExecGroupBuilder::getCurrentHeader()
{
    auto & cur_group = getCurGroup();
    RUNTIME_CHECK(!cur_group.empty());
    return cur_group.front().getCurrentHeader();
}

OperatorProfileInfos PipelineExecGroupBuilder::getCurProfileInfos() const
{
    OperatorProfileInfos ret;
    for (const auto & builder : getCurGroup())
        ret.push_back(builder.getCurProfileInfo());
    return ret;
}

IOProfileInfos PipelineExecGroupBuilder::getCurIOProfileInfos() const
{
    IOProfileInfos ret;
    for (const auto & builder : getCurGroup())
        ret.push_back(builder.getCurIOProfileInfo());
    return ret;
}
} // namespace DB
