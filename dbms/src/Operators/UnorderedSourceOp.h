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
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadResultChannel.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{

/// Read blocks asyncly from Storage Layer by using read thread,
/// The result can not guarantee the keep_order property
class UnorderedSourceOp : public SourceOp
{
public:
    UnorderedSourceOp(
        PipelineExecutorStatus & exec_status_,
        const DM::SegmentReadResultChannelPtr & result_channel_,
        const String & debug_tag)
        : SourceOp(exec_status_, debug_tag)
        , result_channel(result_channel_)
        , log(Logger::get(debug_tag))
        , ref_no(0)
    {
        setHeader(result_channel->header);
        ref_no = result_channel->refConsumer();
        LOG_DEBUG(log, "Created ResultChannelSourceOp, ref_no={}", ref_no);
    }

    ~UnorderedSourceOp() override
    {
        auto remaining_refs = result_channel->derefConsumer();
        LOG_DEBUG(log, "Destroy ResultChannelSourceOp, ref_no={} remaining_refs={}", ref_no, remaining_refs);
    }

    String getName() const override
    {
        return "UnorderedSourceOp";
    }

    void operatePrefix() override
    {
        result_channel->triggerFirstRead();
    }

protected:
    OperatorStatus readImpl(Block & block) override;
    OperatorStatus awaitImpl() override;

private:
    DM::SegmentReadResultChannelPtr result_channel;
    std::optional<Block> t_block;
    LoggerPtr log;
    int64_t ref_no;
};
} // namespace DB
