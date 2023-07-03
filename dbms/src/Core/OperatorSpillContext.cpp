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

#include <Core/OperatorSpillContext.h>

namespace DB
{
HashJoinSpillContext::HashJoinSpillContext(size_t concurrency, UInt64 operator_spill_threshold_)
    : OperatorSpillContext(operator_spill_threshold_)
    , spill_statuses(concurrency)
    , revocable_memories(concurrency)
{
    for (size_t i = 0; i < concurrency; ++i)
    {
        spill_statuses[i] = SpillStatus::NOT_SPILL;
        revocable_memories[i] = 0;
    }
}

AggSpillContext::AggSpillContext(size_t concurrency, const SpillConfig & spill_config_, UInt64 operator_spill_threshold_)
    : OperatorSpillContext(operator_spill_threshold_)
    , per_thread_revocable_memories(concurrency)
    , spill_config(spill_config_)
{
    for (auto & memory : per_thread_revocable_memories)
        memory = 0;
}

void AggSpillContext::buildSpiller(size_t partition_num, const Block & input_schema, const LoggerPtr & log)
{
    /// for aggregation, the input block is sorted by bucket number
    /// so it can work with MergingAggregatedMemoryEfficientBlockInputStream
    spiller = std::make_unique<Spiller>(spill_config, true, partition_num, input_schema, log);
}

} // namespace DB
