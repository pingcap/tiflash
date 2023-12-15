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
#include <Common/FailPoint.h>
#include <Storages/DeltaMerge/ReadThread/MergedTask.h>

namespace DB::FailPoints
{
extern const char exception_in_merged_task_init[];
} // namespace DB::FailPoints

namespace DB::ErrorCodes
{
extern const int FAIL_POINT_ERROR;
}

namespace DB::DM
{
int MergedTask::readBlock()
{
    initOnce();
    return readOneBlock();
}

void MergedTask::initOnce()
{
    if (inited)
    {
        return;
    }

    for (cur_idx = 0; cur_idx < static_cast<int>(units.size()); cur_idx++)
    {
        auto & [pool, task, stream] = units[cur_idx];
        if (!pool->valid())
        {
            setUnitFinish(cur_idx);
            continue;
        }
        if (pool->isRUExhausted())
        {
            continue;
        }
        stream = pool->buildInputStream(task);
        fiu_do_on(FailPoints::exception_in_merged_task_init, {
            throw Exception("Fail point exception_in_merged_task_init is triggered.", ErrorCodes::FAIL_POINT_ERROR);
        });
    }

    inited = true;
}

int MergedTask::readOneBlock()
{
    int read_block_count = 0;
    for (cur_idx = 0; cur_idx < static_cast<int>(units.size()); cur_idx++)
    {
        if (units[cur_idx].isFinished())
        {
            continue;
        }

        auto & [pool, task, stream] = units[cur_idx];

        if (!pool->valid())
        {
            setUnitFinish(cur_idx);
            continue;
        }

        if (pool->getFreeBlockSlots() <= 0 || pool->isRUExhausted())
        {
            continue;
        }

        if (stream == nullptr)
        {
            stream = pool->buildInputStream(task);
        }

        if (pool->readOneBlock(stream, task))
        {
            read_block_count++;
        }
        else
        {
            setUnitFinish(cur_idx);
        }
    }
    return read_block_count;
}

void MergedTask::setException(const DB::Exception & e)
{
    std::for_each(units.begin(), units.end(), [&e](auto & u) {
        if (u.pool != nullptr)
            u.pool->setException(e);
    });
}

MergedTaskPtr MergedTaskPool::pop(uint64_t pool_id)
{
    std::lock_guard lock(mtx);
    auto itr = std::find_if(merged_task_pool.begin(), merged_task_pool.end(), [pool_id](const auto & merged_task) {
        return merged_task->containPool(pool_id);
    });
    if (itr != merged_task_pool.end())
    {
        auto target = *itr;
        merged_task_pool.erase(itr);
        return target;
    }
    return nullptr; // Not Found.
}

void MergedTaskPool::push(const MergedTaskPtr & t)
{
    std::lock_guard lock(mtx);
    merged_task_pool.push_back(t);
}

bool MergedTaskPool::has(UInt64 pool_id)
{
    std::lock_guard lock(mtx);
    return std::any_of(merged_task_pool.begin(), merged_task_pool.end(), [pool_id](const auto & merged_task) {
        return merged_task->containPool(pool_id);
    });
}
} // namespace DB::DM