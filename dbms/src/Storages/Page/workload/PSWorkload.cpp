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
#include <Poco/Logger.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSWorkload.h>
#include <TestUtils/MockDiskDelegator.h>

#include <ext/scope_guard.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace DB::PS::tests
{
void StressWorkload::onDumpResult()
{
    UInt64 time_interval = stop_watch.elapsedMilliseconds();
    LOG_INFO(options.logger, "result in {}ms", time_interval);
    double seconds_run = 1.0 * time_interval / 1000;

    Poco::JSON::Object::Ptr details = new Poco::JSON::Object();

    size_t total_pages_written = 0;
    size_t total_bytes_written = 0;

    Poco::JSON::Array::Ptr json_writers(new Poco::JSON::Array());
    for (auto & writer : writers)
    {
        total_pages_written += writer->pages_used;
        total_bytes_written += writer->bytes_used;

        Poco::JSON::Object::Ptr json_writer = new Poco::JSON::Object();
        json_writer->set("pages", writer->pages_used);
        json_writer->set("bytes", writer->bytes_used);
        json_writers->add(json_writer);
    }
    details->set("writers", json_writers);

    size_t total_pages_read = 0;
    size_t total_bytes_read = 0;

    Poco::JSON::Array::Ptr json_readers(new Poco::JSON::Array());
    for (auto & reader : readers)
    {
        total_pages_read += reader->pages_used;
        total_bytes_read += reader->bytes_used;

        Poco::JSON::Object::Ptr json_reader = new Poco::JSON::Object();
        json_reader->set("pages", reader->pages_used);
        json_reader->set("bytes", reader->bytes_used);
        json_readers->add(json_reader);
    }
    details->set("readers", json_readers);

    LOG_INFO(options.logger, "{}", [&]() {
        std::stringstream ss;
        details->stringify(ss);
        return ss.str();
    }());

    LOG_INFO(options.logger,
             "W: {} pages, {:.4f} GB, {:.4f} GB/s",
             total_pages_written,
             static_cast<double>(total_bytes_written) / DB::GB,
             static_cast<double>(total_bytes_written) / DB::GB / seconds_run);
    LOG_INFO(options.logger,
             "R: {} pages, {:.4f} GB, {:.4f} GB/s",
             total_pages_read,
             static_cast<double>(total_bytes_read) / DB::GB,
             static_cast<double>(total_bytes_read) / DB::GB / seconds_run);

    if (options.status_interval != 0)
    {
        LOG_INFO(options.logger, metrics_dumper->toString());
    }
}

void StressWorkload::initPageStorage(DB::PageStorageConfig & config, String path_prefix)
{
    auto file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);

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
    else if (options.running_ps_version == 4)
    {
        uni_ps = DB::UniversalPageStorage::create("stress_test", delegator, config, "", file_provider);
    }
    else
    {
        throw DB::Exception(fmt::format("Invalid PageStorage version {}",
                                        options.running_ps_version));
    }

    size_t num_of_pages = 0;
    if (ps)
    {
        ps->restore();
        ps->traverse([&num_of_pages](PageId page_id, const DB::Page &) {
            UNUSED(page_id);
            num_of_pages++;
        });
        LOG_INFO(StressEnv::logger, "Recover {} pages.", num_of_pages);
    }
    else
    {
        uni_ps->restore();
    }

    runtime_stat = std::make_unique<GlobalStat>();
}

void StressWorkload::initPages(const DB::PageId & max_page_id)
{
    auto writer = std::make_shared<PSWriter>(ps, uni_ps, 0, runtime_stat);
    for (DB::PageId page_id = 0; page_id <= max_page_id; ++page_id)
    {
        RandomPageId r(page_id);
        writer->write(r);
        if (page_id % 100 == 0)
            LOG_INFO(StressEnv::logger, "writer wrote page {}", page_id);
    }
}

void StressWorkload::startBackgroundTimer()
{
    // A background thread that do GC
    if (options.gc_interval_s > 0)
    {
        gc = std::make_shared<PSGc>(ps, uni_ps, options.gc_interval_s);
        gc->start();
    }

    // A background thread that get snapshot statics,
    // mock `AsynchronousMetrics` that report metrics
    // to grafana.
    scanner = std::make_shared<PSSnapStatGetter>(ps, uni_ps);
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

void PageWorkloadFactory::runWorkload()
{
    if (options.situation_mask == NORMAL_WORKLOAD)
    {
        String name;
        WorkloadCreator func;
        std::tie(name, func) = get(NORMAL_WORKLOAD);
        running_workload = std::shared_ptr<StressWorkload>(func(options));
        LOG_INFO(StressEnv::logger, "Start Running {}, {}", name, running_workload->desc());
        running_workload->run();
        running_workload->onDumpResult();
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
            running_workload = creator(options);
            SCOPE_EXIT({ running_workload.reset(); });
            LOG_INFO(StressEnv::logger, "Start Running {}, {}", name, running_workload->desc());
            running_workload->run();
            if (options.verify && !running_workload->verify())
            {
                LOG_WARNING(StressEnv::logger, "work load: {} failed.", name);
                running_workload->onFailed();
                break;
            }
            else
            {
                running_workload->onDumpResult();
            }
        }
    }
}
} // namespace DB::PS::tests
