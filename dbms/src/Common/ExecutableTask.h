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

#include <Common/nocopyable.h>

#include <memory>

namespace DB
{
class IExecutableTask
{
public:
    IExecutableTask() = default;

    virtual ~IExecutableTask() = default;

    DISALLOW_COPY(IExecutableTask);
    IExecutableTask(IExecutableTask && other) = default;
    IExecutableTask & operator=(IExecutableTask && other) = default;

    virtual void execute() = 0;
};
using ExecutableTaskPtr = std::shared_ptr<IExecutableTask>;

template <typename Func>
class ExecutableTask : public IExecutableTask
{
public:
    ExecutableTask(Func && func_)
        : func{std::move(func_)}
    {}

    ~ExecutableTask() override = default;
    ExecutableTask(ExecutableTask && other) = default;
    ExecutableTask & operator=(ExecutableTask && other) = default;

    void execute() override { func(); }

private:
    Func func;
};
} // namespace DB
