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

#include <Core/OperatorSpillContext.h>

namespace DB
{
class OperatorSpillContexts
{
public:
    Int64 triggerAutoSpill(Int64 expected_released_memories)
    {
        bool old_value = false;
        if (under_auto_spill_check.compare_exchange_strong(old_value, true, std::memory_order::relaxed))
        {
            for (auto & operator_spill_context : operator_spill_contexts)
            {
                assert(operator_spill_context->supportAutoTriggerSpill());
                expected_released_memories = operator_spill_context->triggerSpill(expected_released_memories);
                if (expected_released_memories <= 0)
                    break;
            }
            under_auto_spill_check = false;
            return expected_released_memories;
        }
        return expected_released_memories;
    }
    void registerOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context)
    {
        if (operator_spill_context->supportAutoTriggerSpill())
            operator_spill_contexts.push_back(operator_spill_context);
    }

private:
    std::vector<OperatorSpillContextPtr> operator_spill_contexts;
    std::atomic<bool> under_auto_spill_check{false};
};

} // namespace DB
