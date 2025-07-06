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

#include <Common/BackgroundTask.h>
#include <Common/MemoryAllocTrace.h>
#include <Common/MemoryTracker.h>

#include <fstream>

namespace DB
{
namespace
{
bool isProcStatSupported()
{
    std::ifstream stat_stream("/proc/self/stat", std::ios_base::in);
    return stat_stream.is_open();
}
} // namespace

std::atomic_bool CollectProcInfoBackgroundTask::is_already_begin = false;

void CollectProcInfoBackgroundTask::finish()
{
    std::lock_guard lk(mu);
    end_fin = true;
    cv.notify_all();
}

void CollectProcInfoBackgroundTask::begin()
{
    bool tmp = false;
    if (is_already_begin.compare_exchange_strong(tmp, true))
    {
        if (!isProcStatSupported())
        {
            finish();
            return;
        }
        std::thread t
            = ThreadFactory::newThread(false, "MemTrackThread", &CollectProcInfoBackgroundTask::memCheckJob, this);
        t.detach();
    }
    else
    {
        finish();
    }
}

void CollectProcInfoBackgroundTask::memCheckJob()
{
    try
    {
        while (!end_syn)
        {
            // Update the memory usage of the current process. Defined in Common/MemoryTracker.cpp
            auto res = get_process_mem_usage();
            real_rss = res.resident_bytes;
            proc_num_threads = res.cur_proc_num_threads;
            proc_virt_size = res.cur_virt_bytes;
            baseline_of_query_mem_tracker = root_of_query_mem_trackers->get();
            usleep(100000); // sleep 100ms
        }
    }
    catch (...)
    {
        // ignore exception here.
    }
    finish();
}

void CollectProcInfoBackgroundTask::end() noexcept
{
    end_syn = true;
    std::unique_lock lock(mu);
    cv.wait(lock, [&] { return end_fin; });
}
} // namespace DB
