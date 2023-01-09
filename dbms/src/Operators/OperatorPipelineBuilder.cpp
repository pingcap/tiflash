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

void OperatorPipelineGroupBuilder::init(size_t init_concurrency)
{
    assert(concurrency == 0);
    assert(init_concurrency > 0);
    concurrency = init_concurrency;
    group.resize(concurrency);
}

OperatorPipelineGroup OperatorPipelineGroupBuilder::build()
{
    assert(concurrency > 0);
    OperatorPipelineGroup op_pipeline_group;
    for (auto & builder : group)
        op_pipeline_group.push_back(builder.build());
    return op_pipeline_group;
}

Block OperatorPipelineGroupBuilder::getHeader()
{
    assert(concurrency > 0);
    assert(group.back().header);
    return group.back().header;
}
} // namespace DB
