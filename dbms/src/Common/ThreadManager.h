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

#include <functional>
#include <memory>
#include <string>

namespace DB
{
class ThreadManager
{
public:
    using Job = std::function<void()>;
    virtual ~ThreadManager() = default;
    // only wait non-detached tasks
    virtual void wait() = 0;
    virtual void schedule(bool propagate_memory_tracker, std::string thread_name, Job job) = 0;
    virtual void scheduleThenDetach(bool propagate_memory_tracker, std::string thread_name, Job job) = 0;
};

std::shared_ptr<ThreadManager> newThreadManager();

class ThreadPoolManager
{
public:
    using Job = std::function<void()>;
    virtual ~ThreadPoolManager() = default;
    virtual void wait() = 0;
    virtual void schedule(bool propagate_memory_tracker, Job job) = 0;
};

std::shared_ptr<ThreadPoolManager> newThreadPoolManager(size_t capacity);

} // namespace DB
