#include "PSWorkload.h"


void StressWorkload::result()
{
    UInt64 timeInterval = stop_watch.elapsedMilliseconds();
    fmt::print(stderr, "result in {}ms\n", timeInterval);
    double seconds_run = 1.0 * timeInterval / 1000;

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

    fmt::print(stderr,
               "W: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
               total_pages_written,
               (double)total_bytes_written / DB::GB,
               (double)total_bytes_written / DB::GB / seconds_run);
    fmt::print(stderr,
               "R: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
               total_pages_read,
               (double)total_bytes_read / DB::GB,
               (double)total_bytes_read / DB::GB / seconds_run);

    if (options.status_interval != 0)
    {
        fmt::print(stderr, metrics_dumper->toString());
    }
}

void StressWorkload::initPageStorage(DB::PageStorage::Config & config, String path_prefix)
{
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);

    if (path_prefix.size() == 0)
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
    gc = std::make_shared<PSGc>(ps);
    gc->start();

    scanner = std::make_shared<PSScanner>(ps);
    scanner->start();

    if (options.status_interval > 0)
    {
        metrics_dumper = std::make_shared<PSMetricsDumper>(options.status_interval);
        metrics_dumper->start();
    }

    if (options.timeout_s > 0)
    {
        stress_time = std::make_shared<StressTimeout>(options.timeout_s);
        stress_time->start();
    }
}
void StressWorkload::startReader(size_t nums_readers)
{
    readers.clear();
    for (size_t i = 0; i < nums_readers; ++i)
    {
        auto reader = std::make_shared<PSReader>(ps, i, options.read_delay_ms);
        readers.insert(readers.end(), reader);
        pool.start(*reader, "reader" + DB::toString(i));
    }
}

void StressWorkloadManger::runWorkload()
{
    if (options.situation_mask == NORMAL_WORKLOAD)
    {
        String name;
        workload_func func;
        std::tie(name, func) = get(NORMAL_WORKLOAD);
        auto workload = std::shared_ptr<StressWorkload>(func());
        workload->init(options);
        LOG_INFO(StressEnv::logger, fmt::format("Start Running {} , {}", name, workload->desc()));
        workload->run();
        workload->result();
        return;
    }

    // skip NORMAL_WORKLOAD
    funcs.erase(funcs.find(NORMAL_WORKLOAD));
    std::cout << toWorkloadSelctedString() << std::endl;

    for (auto & it : funcs)
    {
        if (options.situation_mask & it.first)
        {
            auto & name = it.second.first;
            auto & func = it.second.second;
            auto workload = std::shared_ptr<StressWorkload>(func());
            workload->init(options);
            LOG_INFO(StressEnv::logger, fmt::format("Start Running {} , {}", it.second.first, workload->desc()));
            workload->run();
            if (workload->verify())
            {
                workload->failed();
                break;
            }
            else
            {
                workload->result();
            }
        }
    }
}