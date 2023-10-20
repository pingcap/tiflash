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

#pragma once

#include <Core/OperatorSpillContext.h>

namespace DB
{
class TaskOperatorSpillContexts
{
public:
    Int64 triggerAutoSpill(Int64 expected_released_memories);

    void registerOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context);
    /// for tests
    size_t operatorSpillContextCount()
    {
        appendAdditionalOperatorSpillContexts();
        return operator_spill_contexts.size();
    }
    /// for tests
    size_t additionalOperatorSpillContextCount() const
    {
        std::unique_lock lock(mutex);
        return additional_operator_spill_contexts.size();
    }

    Int64 totalRevocableMemories();

    bool isFinished() const { return is_task_finished; }

    void finish() { is_task_finished = true; }

private:
    void appendAdditionalOperatorSpillContexts();
    /// access to operator_spill_contexts is thread safe
    std::list<OperatorSpillContextPtr> operator_spill_contexts;
    mutable std::mutex mutex;
    /// access to additional_operator_spill_contexts need acquire lock first
    std::list<OperatorSpillContextPtr> additional_operator_spill_contexts;
    std::atomic<bool> has_additional_operator_spill_contexts{false};
    std::atomic<bool> is_task_finished{false};
};

} // namespace DB
