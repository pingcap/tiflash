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

#include <Core/FineGrainedOperatorSpillContext.h>

namespace DB
{
bool FineGrainedOperatorSpillContext::supportFurtherSpill() const
{
    for (const auto & operator_spill_context : operator_spill_contexts)
    {
        if (operator_spill_context->supportFurtherSpill())
            return true;
    }
    return false;
}

void FineGrainedOperatorSpillContext::addOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context)
{
    /// fine grained operator spill context only used in auto spill
    if likely (operator_spill_context->supportSpill() && operator_spill_context->supportAutoTriggerSpill())
    {
        operator_spill_contexts.push_back(operator_spill_context);
        operator_spill_context->setAutoSpillMode();
    }
}

Int64 FineGrainedOperatorSpillContext::getTotalRevocableMemoryImpl()
{
    Int64 ret = 0;
    for (auto & operator_spill_context : operator_spill_contexts)
        ret += operator_spill_context->getTotalRevocableMemory();
    return ret;
}

Int64 FineGrainedOperatorSpillContext::triggerSpillImpl(Int64 expected_released_memories)
{
    Int64 original_expected_released_memories = expected_released_memories;
    for (auto & operator_spill_context : operator_spill_contexts)
    {
        if (expected_released_memories <= 0)
            operator_spill_context->triggerSpill(original_expected_released_memories);
        else
            expected_released_memories = operator_spill_context->triggerSpill(expected_released_memories);
    }
    return expected_released_memories;
}
} // namespace DB