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

#include <Operators/Operator.h>

#include <unordered_set>

namespace DB
{
class MultiPartitionSourcePool
{
public:
    explicit MultiPartitionSourcePool(size_t expect_size)
    {
        RUNTIME_CHECK(expect_size > 0);
        pool.resize(expect_size);
    }

    void add(SourceOps && sources)
    {
        for (auto & source : sources)
        {
            pool[pre_index++].push_back(std::move(source));
            if (pre_index == pool.size())
                pre_index = 0;
        }
        sources.clear();
    }

    SourceOps gen()
    {
        while (!pool.empty())
        {
            auto ret = std::move(pool.back());
            pool.pop_back();
            if (!ret.empty())
                return ret;
        }
        return {};
    }

private:
    std::vector<SourceOps> pool;
    size_t pre_index = 0;
};

class ConcatSourceOp : public SourceOp
{
public:
    ConcatSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        SourceOps && pool_)
        : SourceOp(exec_status_, req_id)
        , holder(std::move(pool_))
    {
        assert(!holder.empty());
        setHeader(holder.back()->getHeader());

        for (auto & source : holder)
            pool.insert(source.get());
    }

    String getName() const override
    {
        return "ConcatSourceOp";
    }

    void operatePrefix() override
    {
        for (const auto & source : pool)
            source->operatePrefix();
    }

    void operateSuffix() override
    {
        for (const auto & source : pool)
            source->operateSuffix();
    }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        while (true)
        {
            if unlikely (pool.empty())
                return OperatorStatus::HAS_OUTPUT;

            if (cur == nullptr)
            {
                auto await_res = awaitImpl();
                if (await_res != OperatorStatus::HAS_OUTPUT)
                    return await_res;
                if unlikely (cur == nullptr)
                    return OperatorStatus::HAS_OUTPUT;
            }

            auto res = cur->readImpl(block);
            switch (res)
            {
            case OperatorStatus::WAITING:
                cur = nullptr;
                break;
            case OperatorStatus::HAS_OUTPUT:
                if (!block)
                {
                    pool.erase(cur);
                    cur = nullptr;
                    break;
                }
            default:
                return res;
            }
        }
    }

    OperatorStatus awaitImpl() override
    {
        if (cur != nullptr)
            return OperatorStatus::HAS_OUTPUT;

        if unlikely (pool.empty())
            return OperatorStatus::HAS_OUTPUT;

        for (const auto & source : pool)
        {
            auto res = source->await();
            switch (res)
            {
            case OperatorStatus::HAS_OUTPUT:
                cur = source;
                return OperatorStatus::HAS_OUTPUT;
            case OperatorStatus::WAITING:
                break;
            default:
                return res;
            }
        }

        return OperatorStatus::WAITING;
    }

    OperatorStatus executeIOImpl() override
    {
        if (cur == nullptr)
            return OperatorStatus::WAITING;

        auto res = cur->executeIO();
        switch (res)
        {
        case OperatorStatus::WAITING:
            cur = nullptr;
        default:
            return res;
        }
    }

private:
    SourceOps holder;
    std::unordered_set<SourceOp *> pool;

    SourceOp * cur;
};
} // namespace DB
