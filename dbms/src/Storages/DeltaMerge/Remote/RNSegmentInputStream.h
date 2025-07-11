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
#include <Storages/DeltaMerge/Filter/PushDownExecutor.h>
#include <Storages/DeltaMerge/Remote/RNWorkers_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM::Remote
{

class RNSegmentInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNSegment";

public:
    ~RNSegmentInputStream() override;

    String getName() const override { return NAME; }

    Block getHeader() const override { return action.getHeader(); }

    std::vector<ConnectionProfileInfo> getConnectionProfileInfos() const { return connection_profile_infos; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

public:
    struct Options
    {
        std::string_view debug_tag;
        const RNWorkersPtr & workers;
        const ColumnDefines & columns_to_read;
        int extra_table_id_index;
    };

    explicit RNSegmentInputStream(const Options & options)
        : log(Logger::get(options.debug_tag))
        , workers(options.workers)
        , action(options.columns_to_read, options.extra_table_id_index)
    {
        connection_profile_infos.resize(2);
        connection_profile_infos[INTER_ZONE_INDEX] = ConnectionProfileInfo::createForInterZone();
        connection_profile_infos[INNER_ZONE_INDEX] = ConnectionProfileInfo::createForInnerZone();
    }

    static BlockInputStreamPtr create(const Options & options)
    {
        return std::make_shared<RNSegmentInputStream>(options);
    }

private:
    const LoggerPtr log;
    const RNWorkersPtr workers;
    AddExtraTableIDColumnTransformAction action;

    SegmentReadTaskPtr current_seg_task = nullptr;
    bool done = false;
    size_t processed_seg_tasks = 0;

    double duration_wait_ready_task_sec = 0;
    double duration_read_sec = 0;

    static constexpr size_t INTER_ZONE_INDEX = 0;
    static constexpr size_t INNER_ZONE_INDEX = 1;
    std::vector<ConnectionProfileInfo> connection_profile_infos;
};

} // namespace DB::DM::Remote
