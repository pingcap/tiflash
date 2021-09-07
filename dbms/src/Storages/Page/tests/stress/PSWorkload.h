#pragma once

#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Encryption/MockKeyManager.h>
#include <PSBackground.h>
#include <PSRunnable.h>
#include <PSStressEnv.h>
#include <Poco/Logger.h>
#include <Poco/ThreadPool.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/format.h>

#define NORMAL_WORKLOAD 0

template <typename Child>
class StressWorkloadFunc
{
public:
    static String name_func()
    {
        return Child::name();
    }
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

    virtual String desc() { return ""; };
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
    using workload_func = std::function<StressWorkload *()>;
    std::map<UInt64, std::pair<String, workload_func>> funcs;
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

    void reg(const String & name_, const UInt64 & mask_, const workload_func func)
    {
        if (mask_ & mask)
        {
            fmt::print(stderr, "Current mask is {}, you can not regster mask {}.\n", mask, mask_);
            assert(false);
        }
        mask |= mask_;
        funcs[mask_] = std::make_pair(name_, func);
    }

    std::pair<String, workload_func> get(const UInt64 mask_)
    {
        auto it = funcs.find(mask_);
        if (it == funcs.end())
            throw DB::Exception(fmt::format("No registed workload. mask {} . ", mask_));
        return it->second;
    }

    String toWorkloadSelctedString() const
    {
        String debug_string = "Selected Workloads : \n";
        for (auto & it : funcs)
        {
            if (options.situation_mask & it.first)
            {
                debug_string += fmt::format("   Name : {} , mask : {}. \n", it.second.first, it.first);
            }
        }
        return debug_string;
    }

    String toDebugStirng() const
    {
        String debug_string = "Support Workloads : \n";
        for (auto & it : funcs)
        {
            debug_string += fmt::format("   Name : {} , mask : {}. \n", it.second.first, it.first);
        }
        debug_string += fmt::format("   Need to run all over? try use `-M {}`", mask);
        return debug_string;
    }

    void runWorkload();

private:
    StressEnv options;
};

#define REGISTER_WORKLOAD(WORKLOAD)                                                     \
    static void __attribute__((constructor)) _work_load_register_named_##WORKLOAD(void) \
    {                                                                                   \
        StressWorkloadManger::getInstance().reg(WORKLOAD::name_func(),                  \
                                                WORKLOAD::mask_func(),                  \
                                                []() -> WORKLOAD * {                    \
                                                    return new WORKLOAD();              \
                                                });                                     \
    }
