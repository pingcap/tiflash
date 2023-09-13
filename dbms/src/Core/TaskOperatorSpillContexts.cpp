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

#include <Core/TaskOperatorSpillContexts.h>

namespace DB
{
Int64 TaskOperatorSpillContexts::triggerAutoSpill(Int64 expected_released_memories)
{
    if (isFinished())
        return expected_released_memories;
    appendAdditionalOperatorSpillContexts();
    bool has_finished_operator_spill_contexts = false;
    for (auto & operator_spill_context : operator_spill_contexts)
    {
        assert(operator_spill_context->supportAutoTriggerSpill());
        if (!operator_spill_context->supportFurtherSpill())
        {
            has_finished_operator_spill_contexts = true;
            continue;
        }
        expected_released_memories = operator_spill_context->triggerSpill(expected_released_memories);
        if (expected_released_memories <= 0)
            break;
    }
    if (has_finished_operator_spill_contexts)
    {
        /// clean finished spill context
        operator_spill_contexts.erase(
            std::remove_if(
                operator_spill_contexts.begin(),
                operator_spill_contexts.end(),
                [](const auto & context) { return !context->supportFurtherSpill(); }),
            operator_spill_contexts.end());
    }
    return expected_released_memories;
}
void TaskOperatorSpillContexts::appendAdditionalOperatorSpillContexts()
{
    if (has_additional_operator_spill_contexts)
    {
        std::unique_lock lock(mutex);
        operator_spill_contexts.splice(operator_spill_contexts.end(), additional_operator_spill_contexts);
        has_additional_operator_spill_contexts = false;
        additional_operator_spill_contexts.clear();
    }
}
void TaskOperatorSpillContexts::registerOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context)
{
    if likely (operator_spill_context->supportSpill() && operator_spill_context->supportAutoTriggerSpill())
    {
        std::unique_lock lock(mutex);
        operator_spill_context->setAutoSpillMode();
        additional_operator_spill_contexts.push_back(operator_spill_context);
        has_additional_operator_spill_contexts = true;
    }
}
Int64 TaskOperatorSpillContexts::totalRevocableMemories()
{
    if unlikely (isFinished())
        return 0;
    appendAdditionalOperatorSpillContexts();
    Int64 ret = 0;
    for (const auto & operator_spill_context : operator_spill_contexts)
        ret += operator_spill_context->getTotalRevocableMemory();
    return ret;
}
} // namespace DB
