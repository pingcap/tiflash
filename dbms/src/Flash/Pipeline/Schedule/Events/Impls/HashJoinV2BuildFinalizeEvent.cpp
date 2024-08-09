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

#include <Flash/Pipeline/Schedule/Events/Impls/HashJoinV2BuildFinalizeEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/HashJoinV2BuildFinalizeTask.h>

namespace DB
{
void HashJoinV2BuildFinalizeEvent::scheduleImpl()
{
    size_t concurrency = join_ptr->getBuildConcurrency();
    for (size_t i = 0; i < concurrency; ++i)
        addTask(std::make_unique<HashJoinV2BuildFinalizeTask>(
            exec_context,
            log->identifier(),
            shared_from_this(),
            join_ptr,
            i));
}

} // namespace DB
