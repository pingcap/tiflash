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

#include <Operators/Operator.h>

namespace DB
{
class AggregateContext;
using AggregateContextPtr = std::shared_ptr<AggregateContext>;

class AggregateConvergentSourceOp : public SourceOp
{
public:
    AggregateConvergentSourceOp(
        PipelineExecutorStatus & exec_status_,
        const AggregateContextPtr & agg_context_,
        size_t index_,
        const String & req_id);

    String getName() const override
    {
        return "AggregateConvergentSourceOp";
    }

protected:
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

private:
    AggregateContextPtr agg_context;
    uint64_t total_rows{};
    const size_t index;
};
} // namespace DB
