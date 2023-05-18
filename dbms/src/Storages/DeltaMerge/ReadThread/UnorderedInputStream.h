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
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
}

namespace DB::DM
{
class UnorderedInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "UnorderedInputStream";

public:
    UnorderedInputStream(
        const SegmentReadTaskPoolPtr & task_pool_,
        const ColumnDefines & columns_to_read_,
        int extra_table_id_index_,
        const String & req_id)
        : task_pool(task_pool_)
        , header(AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read_, extra_table_id_index_))
        , log(Logger::get(req_id))
        , ref_no(0)
        , task_pool_added(false)

    {
        ref_no = task_pool->increaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Created, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
    }

    ~UnorderedInputStream() override
    {
        task_pool->decreaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Destroy, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

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
        {
            return {};
        }
        addReadTaskPoolToScheduler();
        while (true)
        {
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);
            Block res;
            task_pool->popBlock(res);
            if (res)
            {
                if (res.rows() > 0)
                {
                    total_rows += res.rows();
                    return res;
                }
                else
                {
                    continue;
                }
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
        LOG_DEBUG(log, "Finish read from storage, pool_id={} ref_no={} rows={}", task_pool->poolId(), ref_no, total_rows);
    }

    void addReadTaskPoolToScheduler()
    {
        if (likely(task_pool_added))
        {
            return;
        }
        std::call_once(task_pool->addToSchedulerFlag(), [&]() { SegmentReadTaskScheduler::instance().add(task_pool); });
        task_pool_added = true;
    }

private:
    SegmentReadTaskPoolPtr task_pool;
    Block header;

    bool done = false;
    LoggerPtr log;
    int64_t ref_no;
    bool task_pool_added;

    size_t total_rows = 0;
};
} // namespace DB::DM
