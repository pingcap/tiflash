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

#include <Operators/Operator.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{

class DMSegmentThreadSourceOp : public SourceOp
{
    static constexpr auto NAME = "DeltaMergeSegmentThread";

public:
    /// If handle_real_type_ is empty, means do not convert handle column back to real type.
    DMSegmentThreadSourceOp(
        PipelineExecutorContext & exec_context_,
        const DM::DMContextPtr & dm_context_,
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        DM::AfterSegmentRead after_segment_read_,
        const DM::ColumnDefines & columns_to_read_,
        const DM::PushDownFilterPtr & filter_,
        UInt64 start_ts_,
        size_t expected_block_size_,
        DM::ReadMode read_mode_,
        const String & req_id);

    String getName() const override;

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus executeIOImpl() override;

private:
    DM::DMContextPtr dm_context;
    DM::SegmentReadTaskPoolPtr task_pool;
    DM::AfterSegmentRead after_segment_read;
    DM::ColumnDefines columns_to_read;
    DM::PushDownFilterPtr filter;
    const UInt64 start_ts;
    const size_t expected_block_size;
    const DM::ReadMode read_mode;

    bool done = false;

    BlockInputStreamPtr cur_stream;

    DM::SegmentPtr cur_segment;

    FilterPtr filter_ignored = nullptr;
    std::optional<Block> t_block;

    size_t total_rows = 0;
};

} // namespace DB
