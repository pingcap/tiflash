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
#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <ext/scope_guard.h>

namespace DB::DM
{
class SegmentReader
{
    inline static const std::string name{"SegmentReader"};

public:
    SegmentReader(WorkQueue<MergedTaskPtr> & task_queue_, const std::vector<int> & cpus_)
        : task_queue(task_queue_)
        , stop(false)
        , log(&Poco::Logger::get(name))
        , cpus(cpus_)
    {
        t = std::thread(&SegmentReader::run, this);
    }

    void setStop()
    {
        stop.store(true, std::memory_order_relaxed);
    }

    ~SegmentReader()
    {
        LOG_DEBUG(log, "Stop begin");
        t.join();
        LOG_DEBUG(log, "Stop end");
    }

    std::thread::id getId() const
    {
        return t.get_id();
    }

private:
    void setCPUAffinity()
    {
        if (cpus.empty())
        {
            return;
        }
#ifdef __linux__
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        for (int i : cpus)
        {
            CPU_SET(i, &cpu_set);
        }
        int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
        if (ret != 0)
        {
            // It can be failed due to some CPU core cannot access, such as CPU offline.
            LOG_WARNING(log, "sched_setaffinity fail, cpus={} errno={}", cpus, std::strerror(errno));
        }
        else
        {
            LOG_DEBUG(log, "sched_setaffinity succ, cpus={}", cpus);
        }
#endif
    }

    bool isStop()
    {
        return stop.load(std::memory_order_relaxed);
    }

    void readSegments()
    {
        MergedTaskPtr merged_task;
        try
        {
            if (!task_queue.pop(merged_task))
            {
                LOG_INFO(log, "Pop fail, stop={}", isStop());
                return;
            }

            int read_count = 0;
            while (!merged_task->allStreamsFinished() && !isStop())
            {
                auto c = merged_task->readBlock();
                read_count += c;
                if (c <= 0)
                {
                    break;
                }
            }
            if (read_count <= 0)
            {
                LOG_DEBUG(log, "All finished, pool_ids={} segment_id={} read_count={}", merged_task->getPoolIds(), merged_task->getSegmentId(), read_count);
            }
            // If `merged_task` is pushed back to `MergedTaskPool`, it can be accessed by another read thread if it is scheduled.
            // So do not push back to `MergedTaskPool` when exception happened since current read thread can still access to this `merged_task` object and set exception message to it.
            // If exception happens, `merged_task` will be released by `shared_ptr` automatically.
            if (!merged_task->allStreamsFinished())
            {
                SegmentReadTaskScheduler::instance().pushMergedTask(merged_task);
            }
        }
        catch (DB::Exception & e)
        {
            LOG_ERROR(log, "ErrMsg: {} StackTrace {}", e.message(), e.getStackTrace().toString());
            if (merged_task != nullptr)
            {
                merged_task->setException(e);
            }
        }
        catch (std::exception & e)
        {
            LOG_ERROR(log, "ErrMsg: {}", e.what());
            if (merged_task != nullptr)
            {
                merged_task->setException(DB::Exception(e.what()));
            }
        }
        catch (...)
        {
            tryLogCurrentException("exception thrown in SegmentReader");
            if (merged_task != nullptr)
            {
                merged_task->setException(DB::Exception("unknown exception thrown in SegmentReader"));
            }
        }
    }

    void run()
    {
        setCPUAffinity();
        setThreadName(name.c_str());
        while (!isStop())
        {
            readSegments();
        }
    }

    WorkQueue<MergedTaskPtr> & task_queue;
    std::atomic<bool> stop;
    Poco::Logger * log;
    std::thread t;
    std::vector<int> cpus;
};

// ===== SegmentReaderPool ===== //

void SegmentReaderPool::addTask(MergedTaskPtr && task)
{
    if (!task_queue.push(std::forward<MergedTaskPtr>(task), nullptr))
    {
        throw Exception("addTask fail");
    }
}

SegmentReaderPool::SegmentReaderPool(int thread_count, const std::vector<int> & cpus)
    : log(&Poco::Logger::get("SegmentReaderPool"))
{
    LOG_INFO(log, "Create start, thread_count={} cpus={}", thread_count, cpus);
    for (int i = 0; i < thread_count; i++)
    {
        readers.push_back(std::make_unique<SegmentReader>(task_queue, cpus));
    }
    LOG_INFO(log, "Create end, thread_count={} cpus={}", thread_count, cpus);
}

SegmentReaderPool::~SegmentReaderPool()
{
    for (auto & reader : readers)
    {
        reader->setStop();
    }
    task_queue.finish();
}

std::vector<std::thread::id> SegmentReaderPool::getReaderIds() const
{
    std::vector<std::thread::id> ids;
    for (const auto & r : readers)
    {
        ids.push_back(r->getId());
    }
    return ids;
}

// ===== SegmentReaderPoolManager ===== //

SegmentReaderPoolManager::SegmentReaderPoolManager()
    : log(&Poco::Logger::get("SegmentReaderPoolManager"))
{}

SegmentReaderPoolManager::~SegmentReaderPoolManager() = default;

void SegmentReaderPoolManager::init(const ServerInfo & server_info)
{
    auto numa_nodes = getNumaNodes(log);
    LOG_INFO(log, "numa_nodes {} => {}", numa_nodes.size(), numa_nodes);
    for (const auto & node : numa_nodes)
    {
        int thread_count = node.empty() ? server_info.cpu_info.logical_cores : node.size();
        reader_pools.push_back(std::make_unique<SegmentReaderPool>(thread_count, node));
        auto ids = reader_pools.back()->getReaderIds();
        reader_ids.insert(ids.begin(), ids.end());
    }
    LOG_INFO(log, "num_readers={}", reader_ids.size());
}

void SegmentReaderPoolManager::addTask(MergedTaskPtr && task)
{
    static std::hash<uint64_t> hash_func;
    auto idx = hash_func(task->getSegmentId()) % reader_pools.size();
    reader_pools[idx]->addTask(std::move(task));
}

// `isSegmentReader` checks whether this thread is a `SegmentReader`.
// Use this function in DMFileBlockInputSteam to check whether enable read thread of this read request,
// Maybe we can pass the argument from DeltaMerge -> SegmentReadTaskPool -> ... -> DMFileBlockInputSteam.
// But this is a long code path and can affect a lot a code.
bool SegmentReaderPoolManager::isSegmentReader() const
{
    return reader_ids.find(std::this_thread::get_id()) != reader_ids.end();
}

void SegmentReaderPoolManager::stop()
{
    reader_pools.clear();
    reader_ids.clear();
}
} // namespace DB::DM
