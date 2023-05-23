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

#include <Common/MPMCQueue.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNSegmentInputStream.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

#include <magic_enum.hpp>

namespace DB::DM::Remote
{

Block RNSegmentInputStream::readImpl(FilterPtr & res_filter, bool return_filter)
{
    if (done)
        return {};

    while (true)
    {
        if (!current_seg_task)
        {
            workers->startInBackground();

            auto pop_result = workers->getReadyChannel()->pop(current_seg_task);
            if (pop_result == MPMCQueueResult::OK)
            {
                processed_seg_tasks += 1;
                RUNTIME_CHECK(current_seg_task != nullptr);
            }
            else if (pop_result == MPMCQueueResult::FINISHED)
            {
                current_seg_task = nullptr;
                LOG_INFO(log, "Finished reading remote segments, rows={} read_segments={}", action.totalRows(), processed_seg_tasks);
                done = true;
                return {};
            }
            else if (pop_result == MPMCQueueResult::CANCELLED)
            {
                current_seg_task = nullptr;
                throw Exception(workers->getReadyChannel()->getCancelReason());
            }
            else
            {
                current_seg_task = nullptr;
                RUNTIME_CHECK_MSG(false, "Unexpected pop result {}", magic_enum::enum_name(pop_result));
            }
        }

        Block res = current_seg_task->getInputStream()->read(res_filter, return_filter);
        if (!res)
        {
            // Current stream is drained, try read from next stream.
            current_seg_task = nullptr;
            continue;
        }

        if (!res.rows())
        {
            // try read from next task
            continue;
        }
        else
        {
            action.transform(res, current_seg_task->meta.physical_table_id);
            return res;
        }
    }
}

} // namespace DB::DM::Remote
