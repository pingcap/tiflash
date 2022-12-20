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

#include <Operators/Operator.h>

#include <memory>

namespace DB
{
class OperatorExecutor
{
public:
    OperatorExecutor(
        SourcePtr && source_,
        std::vector<TransformPtr> && transforms_,
        SinkPtr && sink_)
        : source(std::move(source_))
        , transforms(std::move(transforms_))
        , sink(std::move(sink_))
    {}

    OperatorStatus execute();

    OperatorStatus await();

    OperatorStatus spill();

private:
    // status, next_transform_index
    std::tuple<OperatorStatus, size_t> fetchBlock(Block & block);

    template <typename Op>
    OperatorStatus pushSpiller(OperatorStatus status, const Op & op)
    {
        if (status == OperatorStatus::SPILLING)
        {
            assert(!spiller);
            spiller.emplace(op.get());
        }
        return status;
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    std::optional<Spiller *> spiller;
};
using OperatorExecutorPtr = std::unique_ptr<OperatorExecutor>;
using OperatorExecutorGroup = std::vector<OperatorExecutorPtr>;
using OperatorExecutorGroups = std::vector<OperatorExecutorGroup>;
} // namespace DB
