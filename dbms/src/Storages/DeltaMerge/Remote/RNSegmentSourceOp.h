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
#include <Operators/Operator.h>
#include <Storages/DeltaMerge/Remote/RNWorkers_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>

namespace DB::DM::Remote
{

class RNSegmentSourceOp : public SourceOp
{
    static constexpr auto NAME = "RNSegment";

public:
    struct Options
    {
        std::string_view debug_tag;
        PipelineExecutorContext & exec_context;
        const RNWorkersPtr & workers;
        const ColumnDefines & columns_to_read;
        int extra_table_id_index;
    };

    explicit RNSegmentSourceOp(const Options & options)
        : SourceOp(options.exec_context, String(options.debug_tag))
        , workers(options.workers)
        , action(options.columns_to_read, options.extra_table_id_index)
    {
        setHeader(action.getHeader());
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<RNSegmentSourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operateSuffixImpl() override;

    void operatePrefixImpl() override;

    ReturnOpStatus readImpl(Block & block) override;

    ReturnOpStatus awaitImpl() override;

    ReturnOpStatus executeIOImpl() override;

private:
    ReturnOpStatus startGettingNextReadyTask();

private:
    const RNWorkersPtr workers;
    AddExtraTableIDColumnTransformAction action;

    // Temporarily store the block read from current_seg_task->stream and pass it to downstream operators in readImpl.
    std::optional<Block> t_block = std::nullopt;

    SegmentReadTaskPtr current_seg_task = nullptr;
    bool done = false;

    // Count the number of segment tasks obtained.
    size_t processed_seg_tasks = 0;

    // Count the time spent waiting for segment tasks to be ready.
    double duration_wait_ready_task_sec = 0;
    Stopwatch wait_stop_watch{CLOCK_MONOTONIC_COARSE};

    // Count the time consumed by reading blocks in the stream of segment tasks.
    double duration_read_sec = 0;
};

} // namespace DB::DM::Remote
