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

#include <Common/Stopwatch.h>
#include <Common/nocopyable.h>
#include <PSBackground.h>
#include <PSRunnable.h>
#include <PSStressEnv.h>
#include <Poco/ThreadPool.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <fmt/format.h>

#define NORMAL_WORKLOAD 0
template <typename Child>
class StressWorkloadFunc
{
public:
    static String nameFunc()
    {
        return Child::name();
    }
    static UInt64 maskFunc()
    {
        return Child::mask();
    }
};

// Define a workload.
// The derived class must define `static String name()` and `static UInt64 mask()`
// and register itself by macro `REGISTER_WORKLOAD`
class StressWorkload
{
public:
    explicit StressWorkload(StressEnv options_)
        : options(options_)
    {}

    virtual ~StressWorkload() = default;

    virtual String desc() { return ""; }
    virtual void run() {}
    virtual bool verify()
    {
        return true;
    }
    virtual void onFailed() {}
    virtual void onDumpResult();

protected:
    void initPageStorage(DB::PageStorage::Config & config, String path_prefix = "");

    void startBackgroundTimer();

    template <typename T>
    void startWriter(size_t nums_writers, std::function<void(std::shared_ptr<T>)> writer_configure = nullptr)
    {
        writers.clear();
        for (size_t i = 0; i < nums_writers; ++i)
        {
            auto writer = std::make_shared<T>(ps, i);
            if (writer_configure)
            {
                writer_configure(writer);
            }
            writers.insert(writers.end(), writer);
            pool.start(*writer, "writer" + DB::toString(i));
        }
    }

    template <typename T>
    void startReader(size_t nums_readers, std::function<void(std::shared_ptr<T>)> reader_configure = nullptr)
    {
        readers.clear();
        for (size_t i = 0; i < nums_readers; ++i)
        {
            auto reader = std::make_shared<T>(ps, i);
            if (reader_configure)
            {
                reader_configure(reader);
            }
            readers.insert(readers.end(), reader);
            pool.start(*reader, "reader" + DB::toString(i));
        }
    }


    StressEnv options;
    Poco::ThreadPool pool;

    std::shared_ptr<DB::BackgroundProcessingPool> bkg_pool;
    PSPtr ps;
    DB::PSDiskDelegatorPtr delegator;

    std::list<std::shared_ptr<PSRunnable>> writers;
    std::list<std::shared_ptr<PSRunnable>> readers;

    Stopwatch stop_watch;

    StressTimeoutPtr stress_time;
    PSScannerPtr scanner;
    PSGcPtr gc;
    PSMetricsDumperPtr metrics_dumper;
};


class StressWorkloadManger
{
private:
    using WorkloadCreator = std::function<std::shared_ptr<StressWorkload>(const StressEnv &)>;
    // mask -> (name, creator)
    std::map<UInt64, std::pair<String, WorkloadCreator>> funcs;
    UInt64 registed_masks = 0;

    StressWorkloadManger() = default;

public:
    DISALLOW_COPY_AND_MOVE(StressWorkloadManger);

    static StressWorkloadManger & getInstance()
    {
        static StressWorkloadManger instance;
        return instance;
    }

    void setEnv(const StressEnv & env_)
    {
        options = env_;
    }

    void reg(const String & name, const UInt64 & mask, const WorkloadCreator workload_creator)
    {
        if (mask & registed_masks)
        {
            fmt::print(stderr, "Current mask is {}, you can not register mask {}. ", registed_masks, mask);
            assert(false);
        }
        registed_masks |= mask;
        funcs[mask] = std::make_pair(name, workload_creator);
    }

    std::pair<String, WorkloadCreator> get(const UInt64 mask)
    {
        auto it = funcs.find(mask);
        if (it == funcs.end())
            throw DB::Exception(fmt::format("Not registed workload. Mask: {}. ", mask));
        return it->second;
    }

    String toWorkloadSelctedString() const
    {
        String debug_string = "Selected Workloads : ";
        for (const auto & it : funcs)
        {
            if (options.situation_mask & it.first)
            {
                debug_string += fmt::format("   Name: {}, Mask: {}. ", it.second.first, it.first);
            }
        }
        return debug_string;
    }

    String toDebugStirng() const
    {
        String debug_string = "Support Workloads: \n";
        for (const auto & it : funcs)
        {
            debug_string += fmt::format("   Name: {}, mask: {}. \n", it.second.first, it.first);
        }
        debug_string += fmt::format("   Need to run all over? try use `-M {}`", registed_masks);
        return debug_string;
    }

    void runWorkload();

private:
    StressEnv options;
};

#define REGISTER_WORKLOAD(WORKLOAD)                                                     \
    static void __attribute__((constructor)) _work_load_register_named_##WORKLOAD(void) \
    {                                                                                   \
        StressWorkloadManger::getInstance().reg(                                        \
            WORKLOAD::nameFunc(),                                                       \
            WORKLOAD::maskFunc(),                                                       \
            [](const StressEnv & opts) -> std::shared_ptr<StressWorkload> {             \
                return std::make_shared<WORKLOAD>(opts);                                \
            });                                                                         \
    }
