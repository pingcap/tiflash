#include <Common/MemoryTracker.h>
#include <Encryption/MockKeyManager.h>
#include <PSWorkload.h>
#include <Poco/Logger.h>
#include <TestUtils/MockDiskDelegator.h>

void StressWorkload::onDumpResult()
{
    UInt64 time_interval = stop_watch.elapsedMilliseconds();
    LOG_INFO(options.logger, fmt::format("result in {}ms", time_interval));
    double seconds_run = 1.0 * time_interval / 1000;

    size_t total_pages_written = 0;
    size_t total_bytes_written = 0;

    for (auto & writer : writers)
    {
        total_pages_written += writer->pages_used;
        total_bytes_written += writer->bytes_used;
    }

    size_t total_pages_read = 0;
    size_t total_bytes_read = 0;

    for (auto & reader : readers)
    {
        total_pages_read += reader->pages_used;
        total_bytes_read += reader->bytes_used;
    }

    LOG_INFO(options.logger,
             fmt::format(
                 "W: {} pages, {:.4f} GB, {:.4f} GB/s",
                 total_pages_written,
                 static_cast<double>(total_bytes_written) / DB::GB,
                 static_cast<double>(total_bytes_written) / DB::GB / seconds_run));
    LOG_INFO(options.logger,
             fmt::format(
                 "R: {} pages, {:.4f} GB, {:.4f} GB/s",
                 total_pages_read,
                 static_cast<double>(total_bytes_read) / DB::GB,
                 static_cast<double>(total_bytes_read) / DB::GB / seconds_run));

    if (options.status_interval != 0)
    {
        LOG_INFO(options.logger, metrics_dumper->toString());
    }
}

void StressWorkload::initPageStorage(DB::PageStorage::Config & config, String path_prefix)
{
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);

    if (path_prefix.empty())
    {
        // FIXME: running with `MockDiskDelegatorMulti` is not well-testing
        if (options.paths.empty())
            throw DB::Exception("Can not run without paths");
        if (options.paths.size() == 1)
            delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);
        else
            delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(options.paths);
    }
    else
    {
        // Running Special test use this path
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0] + "/" + path_prefix);
    }

    ps = std::make_shared<DB::PageStorage>("stress_test", delegator, config, file_provider);
    ps->restore();
    {
        size_t num_of_pages = 0;
        ps->traverse([&num_of_pages](const DB::Page & page) {
            (void)page;
            num_of_pages++;
        });
        LOG_INFO(StressEnv::logger, fmt::format("Recover {} pages.", num_of_pages));
    }
}

void StressWorkload::startBackgroundTimer()
{
    // A background thread that do GC
    gc = std::make_shared<PSGc>(ps);
    gc->start();

    // A background thread that scan all pages
    scanner = std::make_shared<PSScanner>(ps);
    scanner->start();

    if (options.status_interval > 0)
    {
        // Dump metrics periodically
        metrics_dumper = std::make_shared<PSMetricsDumper>(options.status_interval);
        metrics_dumper->start();
    }

    if (options.timeout_s > 0)
    {
        // Expected timeout for testing
        stress_time = std::make_shared<StressTimeout>(options.timeout_s);
        stress_time->start();
    }
}

void StressWorkloadManger::runWorkload()
{
    if (options.situation_mask == NORMAL_WORKLOAD)
    {
        String name;
        WorkloadCreator func;
        std::tie(name, func) = get(NORMAL_WORKLOAD);
        auto workload = std::shared_ptr<StressWorkload>(func(options));
        LOG_INFO(StressEnv::logger, fmt::format("Start Running {} , {}", name, workload->desc()));
        workload->run();
        workload->onDumpResult();
        return;
    }

    // skip NORMAL_WORKLOAD
    funcs.erase(funcs.find(NORMAL_WORKLOAD));
    LOG_INFO(options.logger, toWorkloadSelctedString());

    for (auto & it : funcs)
    {
        if (options.situation_mask & it.first)
        {
            auto & name = it.second.first;
            auto & creator = it.second.second;
            auto workload = creator(options);
            LOG_INFO(StressEnv::logger, fmt::format("Start Running {} , {}", name, workload->desc()));
            workload->run();
            if (!workload->verify())
            {
                LOG_WARNING(StressEnv::logger, fmt::format("work load : {} failed.", name));
                workload->onFailed();
                break;
            }
            else
            {
                workload->onDumpResult();
            }
        }
    }
}
