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

#include <Operators/OperatorPipelineBuilder.h>

namespace DB
{
void OperatorPipelineBuilder::setSource(SourcePtr && source_)
{
    assert(!source && source_);
    source = std::move(source_);
    assert(!header);
    header = source->readHeader();
    assert(header);
}
void OperatorPipelineBuilder::appendTransform(TransformPtr && transform)
{
    assert(source && transform);
    transforms.push_back(std::move(transform));
    transforms.back()->transformHeader(header);
    assert(header);
}
void OperatorPipelineBuilder::setSink(SinkPtr && sink_)
{
    assert(header && !sink && sink_);
    sink = std::move(sink_);
}

OperatorPipelinePtr OperatorPipelineBuilder::build()
{
    assert(source && sink);
    return std::make_unique<OperatorPipeline>(
        std::move(source),
        std::move(transforms),
        std::move(sink));
}

void OperatorPipelineGroupBuilder::addGroup(size_t concurrency)
{
    assert(concurrency > 0);
    max_concurrency_in_groups = std::max(max_concurrency_in_groups, concurrency);
    BuilderGroup group;
    group.resize(concurrency);
    groups.push_back(std::move(group));
}

OperatorPipelineGroups OperatorPipelineGroupBuilder::build()
{
    assert(max_concurrency_in_groups > 0);
    OperatorPipelineGroups op_pipeline_groups;
    for (auto & group : groups)
    {
        OperatorPipelineGroup op_pipeline_group;
        for (auto & builder : group)
            op_pipeline_group.push_back(builder.build());
        op_pipeline_groups.push_back(std::move(op_pipeline_group));
    }
    return op_pipeline_groups;
}

Block OperatorPipelineGroupBuilder::getHeader()
{
    assert(max_concurrency_in_groups > 0);
    assert(groups.back().back().header);
    return groups.back().back().header;
}
} // namespace DB
