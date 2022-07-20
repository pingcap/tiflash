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
#include <Storages/DeltaMerge/ReadThread/MergedTask.h>

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
            setStreamFinished(cur_idx);
            continue;
        }
        stream = pool->buildInputStream(task);
    }

    inited = true;
}

int MergedTask::readOneBlock()
{
    int read_block_count = 0;
    for (cur_idx = 0; cur_idx < static_cast<int>(units.size()); cur_idx++)
    {
        if (isStreamFinished(cur_idx))
        {
            continue;
        }

        auto & [pool, task, stream] = units[cur_idx];

        if (!pool->valid())
        {
            setStreamFinished(cur_idx);
            continue;
        }

        if (pool->getFreeBlockSlots() <= 0)
        {
            continue;
        }

        if (pool->readOneBlock(stream, task->segment))
        {
            read_block_count++;
        }
        else
        {
            setStreamFinished(cur_idx);
        }
    }
    return read_block_count;
}

void MergedTask::setException(const DB::Exception & e)
{
    if (cur_idx >= 0 && cur_idx < static_cast<int>(units.size()))
    {
        auto & pool = units[cur_idx].pool;
        if (pool != nullptr)
        {
            pool->setException(e);
        }
    }
    else
    {
        for (auto & unit : units)
        {
            if (unit.pool != nullptr)
            {
                unit.pool->setException(e);
            }
        }
    }
}

MergedTaskPtr MergedTaskPool::pop(uint64_t pool_id)
{
    std::lock_guard lock(mtx);
    MergedTaskPtr target;
    for (auto itr = merged_task_pool.begin(); itr != merged_task_pool.end(); ++itr)
    {
        if ((*itr)->containPool(pool_id))
        {
            target = *itr;
            merged_task_pool.erase(itr);
            break;
        }
    }
    return target;
}

void MergedTaskPool::push(const MergedTaskPtr & t)
{
    std::lock_guard lock(mtx);
    merged_task_pool.push_back(t);
}
} // namespace DB::DM