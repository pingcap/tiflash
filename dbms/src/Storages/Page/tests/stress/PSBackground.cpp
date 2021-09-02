#include "PSBackground.h"

#include <Common/MemoryTracker.h>
#include <Poco/Logger.h>
#include <Poco/Timer.h>
#include <fmt/format.h>

void PSMetricsDumper::onTime(Poco::Timer & /*timer*/)
{
    lastest_memory = CurrentMetrics::get(CurrentMetrics::MemoryTracking);
    if (likely(lastest_memory != 0))
    {
        loop_times++;
        memory_summary += lastest_memory;
        memory_biggest = memory_biggest > lastest_memory ? memory_biggest : lastest_memory;
        LOG_INFO(StressEnv::logger, toString());
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
    try
    {
        MemoryTracker tarcker;
        tarcker.setDescription("(Stress Test GC)");
        current_memory_tracker = &tarcker;
        ps->gc();
        current_memory_tracker = nullptr;
    }
    catch (...)
    {
        StressEnvStatus::getInstance().setStat(STATUS_LOOP);
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

void PSGc::start()
{
    gc_timer.start(Poco::TimerCallback<PSGc>(*this, &PSGc::onTime));
}

void PSScanner::onTime(Poco::Timer & /*timer*/)
{
    size_t num_snapshots = 0;
    double oldest_snapshot_seconds = 0.0;
    unsigned oldest_snapshot_thread = 0;
    try
    {
        LOG_INFO(StressEnv::logger, "Scanner start");
        std::tie(num_snapshots, oldest_snapshot_seconds, oldest_snapshot_thread) = ps->getSnapshotsStat();
        LOG_INFO(StressEnv::logger,
                 fmt::format("Scanner get {} snapshots, longest lifetime: {:.3f}s longest from thread: {}",
                             num_snapshots,
                             oldest_snapshot_seconds,
                             oldest_snapshot_thread));
    }
    catch (...)
    {
        // if gc throw exception stop the test
        StressEnvStatus::getInstance().setStat(STATUS_EXCEPTION);
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

void PSScanner::start()
{
    scanner_timer.start(Poco::TimerCallback<PSScanner>(*this, &PSScanner::onTime));
}

void StressTimeout::onTime(Poco::Timer & /* t */)
{
    LOG_INFO(StressEnv::logger, "timeout.");
    StressEnvStatus::getInstance().setStat(STATUS_TIMEOUT);
}

void StressTimeout::start()
{
    timeout_timer.start(Poco::TimerCallback<StressTimeout>(*this, &StressTimeout::onTime));
}
