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

#include <Common/MemoryTracker.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Poco/Logger.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSWorkload.h>
#include <Storages/Page/workload/TiFlashMetricsHelper.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/core.h>

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
    LOG_INFO(options.logger, "workload result dumped after {}ms", time_interval);
    double seconds_run = 1.0 * time_interval / 1000;

    auto histograms = DB::tests::TiFlashMetricsHelper::collectHistorgrams( //
        {"tiflash_storage_page_write_duration_seconds"});

    for (const auto & [hist_id, hist] : histograms)
    {
        auto stats = DB::tests::TiFlashMetricsHelper::histogramStats(hist);
        LOG_INFO(
            options.logger,
            "name={} type={} avg={:.3f}ms p99={:.3f}ms p999={:.3f}ms",
            hist_id.name,
            hist_id.type,
            stats.avgms,
            stats.p99ms,
            stats.p999ms);
    }

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

    LOG_INFO(options.logger, "Summary by thread: {}\n", [&] {
        std::stringstream ss;
        details->stringify(ss);
        return ss.str();
    }());

    {
        // Output to stdout for performance test summary
        Poco::JSON::Object::Ptr summary = new Poco::JSON::Object();
        Poco::JSON::Object::Ptr writers_summary = new Poco::JSON::Object();
        writers_summary->set("pages", total_pages_written);
        writers_summary->set("bytes_written", total_bytes_written / DB::GB);
        writers_summary->set("write_speed", fmt::format("{:.3f}", 1.0 * total_bytes_written / DB::GB / seconds_run));
        summary->set("write", writers_summary);
        Poco::JSON::Object::Ptr readers_summary = new Poco::JSON::Object();
        readers_summary->set("pages", total_pages_read);
        readers_summary->set("bytes_read", total_bytes_read / DB::GB);
        readers_summary->set("read_speed", fmt::format("{:.3f}", 1.0 * total_bytes_read / DB::GB / seconds_run));
        summary->set("read", readers_summary);
        if (options.status_interval != 0)
            metrics_dumper->addJSONSummaryTo(summary);

        Poco::JSON::Object::Ptr write_latency_summary = new Poco::JSON::Object();
        for (const auto & [hist_id, hist] : histograms)
        {
            auto stats = DB::tests::TiFlashMetricsHelper::histogramStats(hist);
            Poco::JSON::Object::Ptr write_latency_type = new Poco::JSON::Object();
            write_latency_type->set("avg", fmt::format("{:.3f}", stats.avgms));
            write_latency_type->set("p99", fmt::format("{:.3f}", stats.p99ms));
            write_latency_type->set("p999", fmt::format("{:.3f}", stats.p999ms));
            write_latency_summary->set(hist_id.type, write_latency_type);
        }
        summary->set("write_latency", write_latency_summary);

        fmt::print(stdout, "Workload summary: {}\n", [&]() {
            std::stringstream ss;
            summary->stringify(ss);
            return ss.str();
        }());
    }
}

void StressWorkload::initPageStorage(DB::PageStorageConfig & config, String path_prefix)
{
    DB::FileProviderPtr file_provider
        = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);

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
        throw DB::Exception(fmt::format("Invalid PageStorage version {}", options.running_ps_version));
    }

    ps->restore();

    {
        size_t num_of_pages = 0;
        ps->traverse([&num_of_pages](const DB::Page & page) {
            (void)page;
            num_of_pages++;
        });
        LOG_INFO(options.logger, "Recover {} pages.", num_of_pages);
    }

    runtime_stat = std::make_unique<GlobalStat>();
}

void StressWorkload::initPages(const DB::PageIdU64 & max_page_id)
{
    auto writer = std::make_shared<PSWriter>(ps, 0, runtime_stat, options.logger);
    for (DB::PageIdU64 page_id = 0; page_id <= max_page_id; ++page_id)
    {
        RandomPageId r(page_id);
        writer->write(r);
        if (page_id % 100 == 0)
            LOG_INFO(options.logger, "writer wrote page {}", page_id);
    }
}

void StressWorkload::startBackgroundTimer()
{
    // A background thread that do GC
    if (options.gc_interval_s > 0)
    {
        gc = std::make_shared<PSGc>(ps, options.gc_interval_s);
        gc->start();
    }

    // A background thread that get snapshot statics,
    // mock `AsynchronousMetrics` that report metrics
    // to grafana.
    scanner = std::make_shared<PSSnapStatGetter>(ps, options.logger);
    scanner->start();

    if (options.status_interval > 0)
    {
        // Dump metrics periodically
        metrics_dumper = std::make_shared<PSMetricsDumper>(options.status_interval, options.logger);
        metrics_dumper->start();
    }

    if (options.timeout_s > 0)
    {
        // Expected timeout for testing
        stress_time = std::make_shared<StressTimeout>(options.timeout_s, options.logger);
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
        running_workload = func(options);
        LOG_INFO(options.logger, "Start Running {}, {}", name, running_workload->desc());
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
            LOG_INFO(options.logger, "Start Running {}, {}", name, running_workload->desc());
            running_workload->run();
            if (options.verify && !running_workload->verify())
            {
                LOG_WARNING(options.logger, "work load: {} failed.", name);
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

void PageWorkloadFactory::stopWorkload()
{
    if (running_workload)
    {
        running_workload->stop();
        running_workload.reset();
    }
}

} // namespace DB::PS::tests
