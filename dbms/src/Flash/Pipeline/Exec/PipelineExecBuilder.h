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

#include <Flash/Pipeline/Exec/PipelineExec.h>

namespace DB
{
struct PipelineExecBuilder
{
    SourceOpPtr source_op;
    TransformOps transform_ops;
    SinkOpPtr sink_op;

    void setSourceOp(SourceOpPtr && source_op_);
    void appendTransformOp(TransformOpPtr && transform_op);
    void setSinkOp(SinkOpPtr && sink_op_);

    Block getCurrentHeader() const;

    PipelineExecPtr build();
};

class PipelineExecGroupBuilder
{
public:
    PipelineExecGroupBuilder()
    {
        groups.emplace_back();
    }

    using BuilderGroup = std::vector<PipelineExecBuilder>;

    PipelineExecBuilder & getCurBuilder(size_t index)
    {
        RUNTIME_CHECK(!groups.empty());
        RUNTIME_CHECK(groups.back().size() > index);
        return groups.back()[index];
    }

    void addGroup() { groups.emplace_back(); }

    size_t concurrency() { return getCurGroup().size(); }

    bool empty() { return getCurGroup().empty(); }

    void addConcurrency(SourceOpPtr && source);

    void reset();

    void merge(PipelineExecGroupBuilder && other);

    /// ff: [](PipelineExecBuilder & builder) {}
    template <typename FF>
    void transform(FF && ff)
    {
        for (auto & builder : getCurGroup())
        {
            ff(builder);
        }
    }

    PipelineExecGroup build();

    Block getCurrentHeader();

private:
    BuilderGroup & getCurGroup()
    {
        RUNTIME_CHECK(!groups.empty());
        return groups.back();
    }

private:
    // groups generates a set of pipeline_execs running in parallel.
    //
    // group1  -->   group2  -->   cur_group
    // exec1         exec1         exec1
    // exec2         exec2         exec2
    // exec3                       exec3
    //                             exec4
    //
    // Only `cur_group` will be modified, other groups are already stable.
    // The concurrency level of different groups can be different. Usually, a `SharedQueue` is used to connect different groups.
    // This way, different operators with different levels of concurrency can be executed together in a pipeline.
    std::vector<BuilderGroup> groups;
};
} // namespace DB
