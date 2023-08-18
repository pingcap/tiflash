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

#include <Poco/Util/TimerTask.h>

#include <functional>

namespace DB
{
class FunctionTimerTask : public Poco::Util::TimerTask
{
public:
    using Task = std::function<void()>;

    explicit FunctionTimerTask(Task task_)
        : task(task_)
    {}

    void run() override { task(); }

    static Poco::Util::TimerTask::Ptr create(Task task) { return new FunctionTimerTask(task); }

private:
    Task task;
};
} // namespace DB
