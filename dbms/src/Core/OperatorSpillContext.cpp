// Copyright 2023 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Core/OperatorSpillContext.h>

namespace DB
{
bool OperatorSpillContext::isSpillEnabled() const
{
    return enable_spill && (auto_spill_mode || operator_spill_threshold > 0);
}

bool OperatorSpillContext::supportSpill() const
{
    return enable_spill && (supportAutoTriggerSpill() || operator_spill_threshold > 0);
}

Int64 OperatorSpillContext::getTotalRevocableMemory()
{
    assert(isSpillEnabled());
    if (supportFurtherSpill())
        return getTotalRevocableMemoryImpl();
    else
        return 0;
}

void OperatorSpillContext::markSpilled()
{
    bool init_value = false;
    if (is_spilled.compare_exchange_strong(init_value, true, std::memory_order_relaxed))
    {
        LOG_INFO(log, "Begin spill in {}", op_name);
    }
}

void OperatorSpillContext::finishSpillableStage()
{
    if (isSpillEnabled())
        LOG_INFO(log, "Operator finish spill stage");
    in_spillable_stage = false;
}

Int64 OperatorSpillContext::triggerSpill(Int64 expected_released_memories)
{
    assert(isSpillEnabled());
    RUNTIME_CHECK_MSG(operator_spill_threshold == 0, "The operator spill threshold should be 0 in auto spill mode");
    if unlikely (!supportFurtherSpill() || expected_released_memories <= 0)
        return expected_released_memories;
    if (getTotalRevocableMemory() >= MIN_SPILL_THRESHOLD)
        return triggerSpillImpl(expected_released_memories);
    return expected_released_memories;
}

void OperatorSpillContext::setAutoSpillMode()
{
    RUNTIME_CHECK_MSG(supportAutoTriggerSpill(), "Only operator that support auto spill can be set in auto spill mode");
    auto_spill_mode = true;
    /// once auto spill is enabled, operator_spill_threshold will be ignored
    operator_spill_threshold = 0;
}
} // namespace DB
