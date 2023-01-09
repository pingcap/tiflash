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

#pragma once

#include <Operators/OperatorPipeline.h>

namespace DB
{
struct OperatorPipelineBuilder
{
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    Block header;

    void setSource(SourcePtr && source_);
    void appendTransform(TransformPtr && transform);
    void setSink(SinkPtr && sink_);

    OperatorPipelinePtr build();
};

struct OperatorPipelineGroupBuilder
{
    // A Group generates a set of operator pipelines running in parallel.
    using BuilderGroup = std::vector<OperatorPipelineBuilder>;
    BuilderGroup group;

    size_t concurrency = 0;

    void init(size_t init_concurrency);

    /// ff: [](OperatorPipelineBuilder & builder) {}
    template <typename FF>
    void transform(FF && ff)
    {
        assert(concurrency > 0);
        for (auto & builder : group)
        {
            ff(builder);
        }
    }

    OperatorPipelineGroup build();

    Block getHeader();
};
} // namespace DB
