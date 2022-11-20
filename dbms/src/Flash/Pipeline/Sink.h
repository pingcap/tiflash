// Copyright 2022 PingCAP, Ltd.
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

#include <Common/DynamicThreadPool.h>
#include <Core/Block.h>
#include <Flash/Pipeline/PStatus.h>
#include <Flash/Pipeline/Utils.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class Sink
{
public:
    virtual ~Sink() = default;

    virtual PStatus write(Block & block) = 0;

    virtual bool isBlocked() = 0;
};
using SinkPtr = std::unique_ptr<Sink>;

class CPUSink : public Sink
{
public:
    PStatus write(Block & block) override
    {
        if (!block)
            return PStatus::FINISHED;

        block.clear();
        doCpuPart(count, 10000);
        return PStatus::NEED_MORE;
    }

    bool isBlocked() override
    {
        return false;
    }

private:
    size_t count = 0;
};

class IOSink : public Sink
{
public:
    PStatus write(Block & block) override
    {
        if (!block)
            return PStatus::FINISHED;
        block.clear();
        assert(!io_future);
        io_future.emplace(DynamicThreadPool::global_instance->schedule(false, []() { doIOPart(); }));
        return PStatus::BLOCKED;
    }

    bool isBlocked() override
    {
        if (!io_future)
            return false;
        if (io_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready)
        {
            io_future.reset();
            return false;
        }
        return true;
    }

private:
    std::optional<std::future<void>> io_future;
};
} // namespace DB
