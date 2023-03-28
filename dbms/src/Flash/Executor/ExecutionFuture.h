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

#pragma once

#include <Core/Block.h>
#include <Common/MPMCQueue.h>
#include <Common/Exception.h>
#include <common/types.h>

#include <exception>
#include <memory>
#include <mutex>

namespace DB
{
class ExecutionFuture
{
public:
    void finish()
    {
        result.finish();
    }

    bool push(Block && block)
    {
        return result.push(std::move(block)) == MPMCQueueResult::OK;
    }

    Block next()
    {
        Block ret;
        auto pop_ret = result.pop(ret);
        if (pop_ret != MPMCQueueResult::OK)
        {
            std::lock_guard lock(exception_mu);
            if (exception != nullptr)

        }
        return ret;
    }

private:
    MPMCQueue<Block> result;
    std::mutex exception_mu;
    std::exception_ptr exception;
};
using ExecutionFuturePtr = std::shared_ptr<ExecutionFuture>;

} // namespace DB
