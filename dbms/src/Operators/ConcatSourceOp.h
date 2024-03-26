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

#include <Common/Exception.h>
#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Operators/Operator.h>

#include <memory>

namespace DB
{
class SetBlockSinkOp : public SinkOp
{
public:
    SetBlockSinkOp(PipelineExecutorContext & exec_context_, const String & req_id, Block & res_)
        : SinkOp(exec_context_, req_id)
        , res(res_)
    {}

    String getName() const override { return "SetBlockSinkOp"; }

protected:
    ReturnOpStatus writeImpl(Block && block) override
    {
        if unlikely (!block)
            return OperatorStatus::FINISHED;

        assert(!res);
        res = std::move(block);
        return OperatorStatus::NEED_INPUT;
    }

private:
    Block & res;
};

/// Used to merge multiple partitioned tables of storage layer.
class ConcatSourceOp : public SourceOp
{
public:
    ConcatSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        std::vector<PipelineExecBuilder> & exec_builder_pool)
        : SourceOp(exec_context_, req_id)
    {
        RUNTIME_CHECK(!exec_builder_pool.empty());
        setHeader(exec_builder_pool.back().getCurrentHeader());
        for (auto & exec_builder : exec_builder_pool)
        {
            exec_builder.setSinkOp(std::make_unique<SetBlockSinkOp>(exec_context_, req_id, res));
            exec_pool.push_back(exec_builder.build());
        }
    }

    String getName() const override { return "ConcatSourceOp"; }

    // ConcatSourceOp is used to merge multiple partitioned tables of storage layer, so override `getIOProfileInfo` is needed here.
    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operatePrefixImpl() override
    {
        if (!popExec())
            done = true;
    }

    void operateSuffixImpl() override
    {
        if (cur_exec)
        {
            cur_exec->executeSuffix();
            cur_exec.reset();
        }
        exec_pool.clear();
    }

    ReturnOpStatus readImpl(Block & block) override
    {
        if unlikely (done)
            return OperatorStatus::HAS_OUTPUT;

        if unlikely (res)
        {
            std::swap(block, res);
            return OperatorStatus::HAS_OUTPUT;
        }

        while (true)
        {
            assert(cur_exec);
            auto status = cur_exec->execute();
            switch (status.status)
            {
            case OperatorStatus::NEED_INPUT:
                assert(res);
                std::swap(block, res);
                return OperatorStatus::HAS_OUTPUT;
            case OperatorStatus::FINISHED:
                cur_exec->executeSuffix();
                cur_exec.reset();
                if (!popExec())
                {
                    done = true;
                    return OperatorStatus::HAS_OUTPUT;
                }
                break;
            default:
                return status;
            }
        }
    }

    ReturnOpStatus executeIOImpl() override
    {
        if unlikely (done || res)
            return OperatorStatus::HAS_OUTPUT;

        assert(cur_exec);
        auto status = cur_exec->executeIO();
        assert(status.status != OperatorStatus::FINISHED);
        return status;
    }

    ReturnOpStatus awaitImpl() override
    {
        if unlikely (done || res)
            return OperatorStatus::HAS_OUTPUT;

        assert(cur_exec);
        auto status = cur_exec->await();
        assert(status.status != OperatorStatus::FINISHED);
        return status;
    }

private:
    bool popExec()
    {
        assert(!cur_exec);
        if (exec_pool.empty())
        {
            return false;
        }
        else
        {
            cur_exec = std::move(exec_pool.front());
            exec_pool.pop_front();
            cur_exec->executePrefix();
            return true;
        }
    }

private:
    std::deque<PipelineExecPtr> exec_pool;
    PipelineExecPtr cur_exec;

    Block res;
    bool done = false;
};

class ConcatBuilderPool
{
public:
    explicit ConcatBuilderPool(size_t expect_size)
    {
        RUNTIME_CHECK(expect_size > 0);
        pool.resize(expect_size);
    }

    void add(PipelineExecGroupBuilder & group_builder)
    {
        RUNTIME_CHECK(group_builder.groupCnt() == 1);
        for (size_t i = 0; i < group_builder.concurrency(); ++i)
        {
            pool[pre_index++].push_back(std::move(group_builder.getCurBuilder(i)));
            if (pre_index == pool.size())
                pre_index = 0;
        }
    }

    void generate(
        PipelineExecGroupBuilder & result_builder,
        PipelineExecutorContext & exec_context,
        const String & req_id)
    {
        RUNTIME_CHECK(result_builder.empty());
        for (auto & builders : pool)
        {
            if (builders.empty())
            {
                continue;
            }
            else if (builders.size() == 1)
            {
                result_builder.addConcurrency(std::move(builders.back()));
            }
            else
            {
                result_builder.addConcurrency(std::make_unique<ConcatSourceOp>(exec_context, req_id, builders));
            }
        }
    }

private:
    std::vector<std::vector<PipelineExecBuilder>> pool;
    size_t pre_index = 0;
};
} // namespace DB
