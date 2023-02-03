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

void PipelineExecGroupBuilder::init(size_t init_concurrency)
{
    assert(concurrency == 0);
    assert(init_concurrency > 0);
    concurrency = init_concurrency;
    group.resize(concurrency);
}

PipelineExecGroup PipelineExecGroupBuilder::build()
{
    assert(concurrency > 0);
    PipelineExecGroup pipeline_exec_group;
    for (auto & builder : group)
        pipeline_exec_group.push_back(builder.build());
    return pipeline_exec_group;
}

Block PipelineExecGroupBuilder::getCurrentHeader()
{
    assert(!group.empty());
    return group.back().getCurrentHeader();
}
} // namespace DB
