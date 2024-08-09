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

#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM
{
namespace tests
{
class DeltaMergeStoreRWTest;
}
class UnorderedInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "UnorderedInputStream";

public:
    UnorderedInputStream(
        const SegmentReadTaskPoolPtr & task_pool_,
        const ColumnDefines & columns_to_read_,
        int extra_table_id_index_,
        const String & req_id,
        const RuntimeFilteList & runtime_filter_list_ = std::vector<RuntimeFilterPtr>{},
        int max_wait_time_ms_ = 0)
        : task_pool(task_pool_)
        , header(AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read_, extra_table_id_index_))
        , log(Logger::get(req_id))
        , ref_no(0)
        , task_pool_added(false)
        , runtime_filter_list(runtime_filter_list_)
        , max_wait_time_ms(max_wait_time_ms_)
    {
        ref_no = task_pool->increaseUnorderedInputStreamRefCount();
    }

    ~UnorderedInputStream() override
    {
        if (const auto rc_before_decr = task_pool->decreaseUnorderedInputStreamRefCount(); rc_before_decr == 1)
        {
            LOG_INFO(
                log,
                "All unordered input streams are finished, pool_id={} last_stream_ref_no={}",
                task_pool->pool_id,
                ref_no);
        }
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    // only for unit test
    // The logic order of unit test is error, it will build input stream firstly and register rf secondly.
    // It causes input stream could not get RF list in constructor.
    // So, for unit test, it should call this function separated.
    void setRuntimeFilterInfo(const RuntimeFilteList & runtime_filter_list_, int max_wait_time_ms_)
    {
        runtime_filter_list = runtime_filter_list_;
        max_wait_time_ms = max_wait_time_ms_;
    }

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
        LOG_DEBUG(
            log,
            "Finish read from storage, pool_id={} ref_no={} rows={}",
            task_pool->pool_id,
            ref_no,
            total_rows);
    }

    void addReadTaskPoolToScheduler()
    {
        if (likely(task_pool_added))
        {
            return;
        }
        std::call_once(task_pool->addToSchedulerFlag(), [&]() {
            prepareRuntimeFilter();
            SegmentReadTaskScheduler::instance().add(task_pool);
        });
        task_pool_added = true;
    }

private:
    void prepareRuntimeFilter();

    void pushDownReadyRFList(const std::vector<RuntimeFilterPtr> & ready_rf_list);

    SegmentReadTaskPoolPtr task_pool;
    Block header;

    bool done = false;
    LoggerPtr log;
    int64_t ref_no;
    bool task_pool_added;

    size_t total_rows = 0;

    // runtime filter
    std::vector<RuntimeFilterPtr> runtime_filter_list;
    int max_wait_time_ms;

    friend class tests::DeltaMergeStoreRWTest;
};
} // namespace DB::DM
