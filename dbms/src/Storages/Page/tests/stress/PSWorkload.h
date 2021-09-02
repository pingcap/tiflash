#pragma once

#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Encryption/MockKeyManager.h>
#include <Poco/Logger.h>
#include <Poco/ThreadPool.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/format.h>

#include "PSBackground.h"
#include "PSRunnable.h"
#include "PSStressEnv.h"

#define NORMAL_WORKLOAD 0

template <typename Child>
class StressWorkloadFunc
{
public:
    static UInt64 mask_func()
    {
        return Child::mask();
    }
};

class StressWorkload
{
public:
    virtual ~StressWorkload(){};

public:
    virtual void init(StressEnv & options_)
    {
        options = options_;
    }

    virtual void run(){};
    virtual bool verify()
    {
        return true;
    };
    virtual void failed(){};
    virtual void result();

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

    void startReader(size_t nums_readers);

protected:
    StressEnv options;
    Poco::ThreadPool pool;

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
    using Workload = std::function<StressWorkload *()>;
    std::map<UInt64, Workload> funcs;
    // std::map<UInt64,std::function<StressWorkload*()>> funcs;
    UInt64 mask = 0;

private:
    StressWorkloadManger(){};
    ~StressWorkloadManger(){};

public:
    static StressWorkloadManger & getInstance()
    {
        static StressWorkloadManger instance;
        return instance;
    }

    void setEnv(StressEnv & env_)
    {
        options = env_;
    }

    void reg(const UInt64 mask_, Workload func)
    {
        if (mask_ & mask)
        {
            assert(false);
        }
        mask |= mask_;
        funcs[mask_] = func;
    }

    Workload get(const UInt64 mask_)
    {
        auto it = funcs.find(mask_);
        if (it == funcs.end())
            throw DB::Exception(
                fmt::format("No registed workload. mask {} . ", mask_));
        return it->second;
    }

    void runWorkload();

private:
    StressEnv options;
};

#define REGISTER_WORKLOAD(WORKLOAD)                                                     \
    static void __attribute__((constructor)) _work_load_register_named_##WORKLOAD(void) \
    {                                                                                   \
        StressWorkloadManger::getInstance().reg(WORKLOAD::mask_func(),                  \
                                                []() -> WORKLOAD * {                    \
                                                    return new WORKLOAD();              \
                                                });                                     \
    }\
