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

struct PipelineExecGroupBuilder
{
    // A Group generates a set of pipeline_execs running in parallel.
    using BuilderGroup = std::vector<PipelineExecBuilder>;
    BuilderGroup group;

    size_t concurrency = 0;

    void init(size_t init_concurrency);

    /// ff: [](PipelineExecBuilder & builder) {}
    template <typename FF>
    void transform(FF && ff)
    {
        assert(concurrency > 0);
        for (auto & builder : group)
        {
            ff(builder);
        }
    }

    PipelineExecGroup build();

    Block getCurrentHeader();
};
} // namespace DB
