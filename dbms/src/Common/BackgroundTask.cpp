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

#include <fstream>

namespace DB
{
namespace
{
bool process_mem_usage(double & resident_set, Int64 & cur_proc_num_threads, UInt64 & cur_virt_size)
{
    resident_set = 0.0;

    // 'file' stat seems to give the most reliable results
    std::ifstream stat_stream("/proc/self/stat", std::ios_base::in);
    // if "/proc/self/stat" is not supported
    if (!stat_stream.is_open())
        return false;

    // dummy vars for leading entries in stat that we don't care about
    std::string pid, comm, state, ppid, pgrp, session, tty_nr;
    std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    std::string utime, stime, cutime, cstime, priority, nice;
    std::string itrealvalue, starttime;

    // the field we want
    Int64 rss;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >> minflt >> cminflt
        >> majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >> nice >> cur_proc_num_threads
        >> itrealvalue >> starttime >> cur_virt_size >> rss; // don't care about the rest

    stat_stream.close();

    Int64 page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    resident_set = rss * page_size_kb;
    return true;
}

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
        double resident_set;
        Int64 cur_proc_num_threads = 1;
        UInt64 cur_virt_size = 0;
        while (!end_syn)
        {
            process_mem_usage(resident_set, cur_proc_num_threads, cur_virt_size);
            resident_set *= 1024; // unit: byte
            real_rss = static_cast<Int64>(resident_set);
            proc_num_threads = cur_proc_num_threads;
            proc_virt_size = cur_virt_size;
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
