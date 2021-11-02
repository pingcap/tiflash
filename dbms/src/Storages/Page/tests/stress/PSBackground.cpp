#include <Common/MemoryTracker.h>
#include <PSBackground.h>
#include <Poco/Logger.h>
#include <Poco/Timer.h>
#include <fmt/format.h>

void PSMetricsDumper::onTime(Poco::Timer & /*timer*/)
{
    for (auto & metric : metrics)
    {
        auto lastest = CurrentMetrics::get(metric.first);
        if (likely(lastest != 0))
        {
            auto & info = metric.second;
            info.loop_times++;
            info.lastest = lastest;
            info.summary += lastest;
            info.biggest = std::max(info.biggest, lastest);
            LOG_INFO(StressEnv::logger, info.toString());
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
        MemoryTracker tracker;
        tracker.setDescription("(Stress Test GC)");
        current_memory_tracker = &tracker;
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

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void StressTimeout::onTime(Poco::Timer & /* t */)
{
    LOG_INFO(StressEnv::logger, "timeout.");
    StressEnvStatus::getInstance().setStat(STATUS_TIMEOUT);
}

void StressTimeout::start()
{
    timeout_timer.start(Poco::TimerCallback<StressTimeout>(*this, &StressTimeout::onTime));
}
