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

#include <Common/FailPoint.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadResultChannel.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
}

namespace DB::DM
{
class ResultChannelInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "ResultChannelInputStream";

public:
    ResultChannelInputStream(
        const SegmentReadResultChannelPtr & result_channel_,
        const String & debug_tag)
        : result_channel(result_channel_)
        , log(Logger::get(debug_tag))
        , ref_no(0)
    {
        ref_no = result_channel->refConsumer();
        LOG_DEBUG(log, "Created ResultChannelInputStream, ref_no={}", ref_no);
    }

    ~ResultChannelInputStream() override
    {
        auto remaining_refs = result_channel->derefConsumer();
        LOG_DEBUG(log, "Destroy ResultChannelInputStream, ref_no={} remaining_refs={}", ref_no, remaining_refs);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return result_channel->header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    // Currently, res_filter and return_filter is unused.
    Block readImpl(FilterPtr & /*res_filter*/, bool /*return_filter*/) override
    {
        if (done)
            return {};

        while (true)
        {
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);
            Block res;
            result_channel->popBlock(res);
            if (res)
            {
                if (unlikely(res.rows() == 0))
                    continue;
                total_rows += res.rows();
                return res;
            }
            else
            {
                done = true;
                return {};
            }
        }
    }

    void readSuffixImpl() override
    {
        LOG_DEBUG(log, "Finish read from storage, ref_no={} rows={}", ref_no, total_rows);
    }

private:
    SegmentReadResultChannelPtr result_channel;

    bool done = false;
    LoggerPtr log;
    int64_t ref_no;

    size_t total_rows = 0;
};

} // namespace DB::DM
