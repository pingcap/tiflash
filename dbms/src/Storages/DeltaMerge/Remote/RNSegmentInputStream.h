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

#include <Common/MPMCQueue.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM::Remote
{

class RNSegmentInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNSegment";

public:
    RNSegmentInputStream(
        const Context & db_context_,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & ready_segments_,
        const ColumnDefines & columns_to_read_,
        const PushDownFilterPtr & push_down_filter_,
        UInt64 max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        int extra_table_id_index_,
        std::string_view req_id)
        : ready_segments(ready_segments_)
    {}

    ~RNSegmentInputStream() override;

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

private:
    const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> ready_segments;
};

} // namespace DB::DM::Remote
