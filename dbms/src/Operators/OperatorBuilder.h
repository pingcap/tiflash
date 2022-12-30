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

#pragma once

#include <Operators/OperatorExecutor.h>

namespace DB
{
struct OperatorBuilder
{
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    Block header;

    void setSource(SourcePtr && source_);
    void appendTransform(TransformPtr && transform);
    void setSink(SinkPtr && sink_);

    OperatorExecutorPtr build();
};

struct OperatorGroupBuilder
{
    // A Group generates a set of operator executors running in parallel.
    using BuilderGroup = std::vector<OperatorBuilder>;
    // The relationship between different BuilderGroups is similar to
    // the relationship between `streams` and `streams_with_non_joined_data` in `DAGPipeline`.
    std::vector<BuilderGroup> groups;

    size_t max_concurrency_in_groups = 0;

    void addGroup(size_t concurrency);

    /// ff: [](OperatorBuilder & builder) {}
    template <typename FF>
    void transform(FF && ff)
    {
        assert(max_concurrency_in_groups > 0);
        for (auto & group : groups)
        {
            for (auto & builder : group)
            {
                ff(builder);
            }
        }
    }

    OperatorExecutorGroups build();

    Block getHeader();
};
} // namespace DB
