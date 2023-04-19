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

#include <Flash/Pipeline/Schedule/Events/BucketLoadEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/BucketLoadTask.h>
#include <Operators/SharedAggregateRestorer.h>

namespace DB
{
std::vector<TaskPtr> BucketLoadEvent::scheduleImpl()
{
    assert(loader);
    std::vector<TaskPtr> tasks;
    auto load_inputs = loader->getLoadInputs();
    tasks.reserve(load_inputs.size());
    for (const auto & input : load_inputs)
        tasks.push_back(std::make_unique<BucketLoadTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), *input));
    return tasks;
}

void BucketLoadEvent::finishImpl()
{
    assert(loader);
    loader->storeFromInputToBucketData();
    loader.reset();
}
} // namespace DB
