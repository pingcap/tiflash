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
class Transform
{
public:
    virtual ~Transform() = default;

    virtual PStatus transform(Block & block) = 0;

    virtual Block fetchBlock() = 0;

    virtual bool isBlocked() = 0;
};
using TransformPtr = std::unique_ptr<Transform>;

class CPUTransform : public Transform
{
public:
    PStatus transform(Block & block) override
    {
        if (!block)
            return PStatus::NEED_MORE;

        doCpuPart(count, 100000);
        return PStatus::NEED_MORE;
    }

    Block fetchBlock() override
    {
        return {};
    }

    bool isBlocked() override
    {
        return false;
    }

private:
    size_t count = 0;
};

class IOTransform : public Transform
{
public:
    PStatus transform(Block & block) override
    {
        if (!block)
            return PStatus::NEED_MORE;

        assert(!io_future);
        io_future.emplace(DynamicThreadPool::global_instance->schedule(true, [&, block]() {
            assert(!io_block);
            doIOPart();
            doCpuPart(count, 10);
        }));
        return PStatus::BLOCKED;
    }

    Block fetchBlock() override
    {
        if (io_block)
        {
            // cpu part
            doCpuPart(count, 1000);

            Block block;
            std::swap(block, io_block);
            return block;
        }
        return {};
    }

    bool isBlocked() override
    {
        if (!io_future)
            return false;
        bool is_ready = io_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        if (is_ready)
            io_future.reset();
        return !is_ready;
    }

private:
    std::optional<std::future<void>> io_future;
    Block io_block;
    size_t count = 0;
};
} // namespace DB
