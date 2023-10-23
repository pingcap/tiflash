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
#include <Poco/Logger.h>
#include <Poco/Timer.h>
#include <Storages/Page/workload/PSBackground.h>
#include <fmt/format.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace DB::PS::tests
{
void PSMetricsDumper::addJSONSummaryTo(Poco::JSON::Object::Ptr & root) const
{
    for (const auto & m : metrics)
    {
        Poco::JSON::Object::Ptr metrics_obj = new Poco::JSON::Object();
        metrics_obj->set("latest", m.second.latest);
        double avg = m.second.loop_times == 0 ? 0.0 : (1.0 * m.second.summary / m.second.loop_times);
        metrics_obj->set("avg", avg);
        metrics_obj->set("top", m.second.biggest);

        root->set(m.second.name, metrics_obj);
    }
}

void PSMetricsDumper::onTime(Poco::Timer & /*timer*/)
{
    for (auto & metric : metrics)
    {
        auto latest = CurrentMetrics::get(metric.first);
        if (likely(latest != 0))
        {
            auto & info = metric.second;
            info.loop_times++;
            info.latest = latest;
            info.summary += latest;
            info.biggest = std::max(info.biggest, latest);
            LOG_INFO(logger, info.toString());
        }
    }
}

void PSMetricsDumper::start()
{
    if (status_interval != 0)
    {
        timer_status.start(Poco::TimerCallback<PSMetricsDumper>(*this, &PSMetricsDumper::onTime));
    }
}

void PSGc::doGcOnce()
{
    gc_stop_watch.start();
    try
    {
        auto tracker = MemoryTracker::create();
        tracker->setDescription("(Stress Test GC)");
        current_memory_tracker = tracker.get();
        ps->gc();
        current_memory_tracker = nullptr;
    }
    catch (...)
    {
        StressEnvStatus::getInstance().setStat(STATUS_LOOP);
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
    gc_stop_watch.stop();
}

void PSGc::start()
{
    gc_timer.start(Poco::TimerCallback<PSGc>(*this, &PSGc::onTime));
}

void PSSnapStatGetter::onTime(Poco::Timer & /*timer*/)
{
    try
    {
        LOG_INFO(logger, "Scanner start");
        auto stat = ps->getSnapshotsStat();
        LOG_INFO(
            logger,
            "Scanner get {} snapshots, longest lifetime: {:.3f}s longest from thread: {}, tracing_id: {}",
            stat.num_snapshots,
            stat.longest_living_seconds,
            stat.longest_living_from_thread_id,
            stat.longest_living_from_tracing_id);
    }
    catch (...)
    {
        // if gc throw exception stop the test
        StressEnvStatus::getInstance().setStat(STATUS_EXCEPTION);
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

void PSSnapStatGetter::start()
{
    scanner_timer.start(Poco::TimerCallback<PSSnapStatGetter>(*this, &PSSnapStatGetter::onTime));
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void StressTimeout::onTime(Poco::Timer & /* t */)
{
    LOG_INFO(logger, "timeout.");
    StressEnvStatus::getInstance().setStat(STATUS_TIMEOUT);
}

void StressTimeout::start()
{
    timeout_timer.start(Poco::TimerCallback<StressTimeout>(*this, &StressTimeout::onTime));
}
} // namespace DB::PS::tests
