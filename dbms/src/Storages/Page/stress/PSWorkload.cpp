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

#include <Common/MemoryTracker.h>
#include <Encryption/MockKeyManager.h>
#include <PSWorkload.h>
#include <Poco/Logger.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
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

    if (options.running_ps_version == 2)
    {
        bkg_pool = std::make_shared<DB::BackgroundProcessingPool>(4, "bg-page-");
        ps = std::make_shared<DB::PS::V2::PageStorage>("stress_test", delegator, config, file_provider, *bkg_pool);
    }
    else if (options.running_ps_version == 3)
    {
        ps = std::make_shared<DB::PS::V3::PageStorageImpl>("stress_test", delegator, config, file_provider);
    }
    else
    {
        throw DB::Exception(fmt::format("Invalid PageStorage version {}",
                                        options.running_ps_version));
    }

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
    if (options.just_init_pages || options.situation_mask == NORMAL_WORKLOAD)
    {
        String name;
        WorkloadCreator func;
        std::tie(name, func) = get(NORMAL_WORKLOAD);
        auto workload = std::shared_ptr<StressWorkload>(func(options));
        LOG_INFO(StressEnv::logger, fmt::format("Start Running {} , {}", name, workload->desc()));
        workload->run();
        if (!options.just_init_pages)
        {
            workload->onDumpResult();
        }
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
            if (options.verify && !workload->verify())
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
