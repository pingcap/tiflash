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

#include <Common/Logger.h>
#include <Operators/Operator.h>

#include <mutex>
#include <list>

namespace DB
{
class FinishFlagSourceOp : public SourceOp
{
public:
    FinishFlagSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        const Block & header_)
        : SourceOp(exec_status_, req_id)
    {
        setHeader(header_);
    }

    String getName() const override
    {
        return "FinishFlagSourceOp";
    }

protected:
    OperatorStatus readImpl(Block &) override
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    OperatorStatus awaitImpl() override
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    OperatorStatus executeIOImpl(Block &) override
    {
        return OperatorStatus::HAS_OUTPUT;
    }
};

class SharedSourceOpPool
{
public:
    void executePrefix()
    {
        std::call_once(prefix_suffix_flag, [&]() {
            for (const auto & op : holder)
                op->operatePrefix();
        });
    }

    void executeSuffix()
    {
        std::call_once(prefix_suffix_flag, [&]() {
            for (const auto & op : holder)
                op->operateSuffix();
        });
    }

    SourceOp * pickReadyOne()
    {
        std::lock_guard lock(mtx);
        return &finish_flag;
    }

    void release(SourceOp * source_op)
    {
        std::lock_guard lock(mtx);
        pool.push_back(source_op);
    }

    Block getHeader() const
    {
        std::lock_guard lock(mtx);
        return holder.back()->getHeader();
    }

private:
    std::once_flag prefix_suffix_flag;
    std::mutex mtx;
    std::vector<SourceOpPtr> holder;
    std::list<SourceOp *> pool;

    FinishFlagSourceOp finish_flag;
};
using SharedSourceOpPoolPtr = std::shared_ptr<SharedSourceOpPool>;

class MultiplexSourceOp : public SourceOp
{
public:
    MultiplexSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        SharedSourceOpPoolPtr pool_)
        : SourceOp(exec_status_, req_id)
        , pool(std::move(pool_))
    {
        assert(pool);
        setHeader(pool->getHeader());
    }

    String getName() const override
    {
        return "MultiplexSourceOp";
    }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        if (cur == nullptr)
        {
            cur = pool->pickReadyOne();
            if (!cur)
                return OperatorStatus::WAITING;
        }
        return cur->readImpl(block);
    }

    OperatorStatus awaitImpl() override
    {
        if (cur == nullptr)
        {
            cur = pool->pickReadyOne();
            return cur ? OperatorStatus::HAS_OUTPUT : OperatorStatus::WAITING;
        }
        auto res = cur->await();
        switch (res)
        {
        case OperatorStatus::WAITING:
            pool->release(cur);
            cur = nullptr;
            return OperatorStatus::WAITING;
        case OperatorStatus::FINISHED:
        case OperatorStatus::CANCELLED:
        case OperatorStatus::WAITING:
            cur = nullptr;
            return OperatorStatus::WAITING;
        default:
            return res;
        }
    }

private:
    SharedSourceOpPoolPtr pool;

    SourceOp * cur = nullptr;
};
} // namespace DB
