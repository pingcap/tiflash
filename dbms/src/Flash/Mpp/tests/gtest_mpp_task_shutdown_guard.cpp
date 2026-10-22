// Copyright 2026 PingCAP, Inc.
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

#include <Flash/Mpp/MPPTaskShutdownGuard.h>
#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <thread>
#include <vector>

namespace DB::tests
{
namespace
{
struct ProbeTestClock
{
    using duration = std::chrono::steady_clock::duration;
    using time_point = std::chrono::time_point<ProbeTestClock>;
    static time_point now() { return time_point(duration(ticks.load())); }
    static inline std::atomic<duration::rep> ticks{0};
};

struct BlockingProbeClock
{
    using duration = ProbeTestClock::duration;
    using time_point = std::chrono::time_point<BlockingProbeClock>;
    static time_point now()
    {
        if (entered != nullptr)
        {
            entered->set_value();
            resume.wait();
        }
        return time_point(duration(ProbeTestClock::ticks.load()));
    }
    static inline std::promise<void> * entered = nullptr;
    static inline std::shared_future<void> resume;
};

class TestMPPTaskShutdownGuard : public testing::Test
{
protected:
    void SetUp() override { ProbeTestClock::ticks = 0; }

    static void advance(std::chrono::seconds elapsed)
    {
        ProbeTestClock::ticks += std::chrono::duration_cast<ProbeTestClock::duration>(elapsed).count();
    }

    MPPTaskShutdownGuard<ProbeTestClock> guard;
};
} // namespace

TEST_F(TestMPPTaskShutdownGuard, NoProbeDoesNotDelayShutdown)
{
    guard.setUnavailable();
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, DefaultWindowExpiresAtSixtySecondsAfterProbe)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(std::chrono::seconds(10));
    guard.setUnavailable();
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(50));
    advance(std::chrono::seconds(49));
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(1));
    advance(std::chrono::seconds(1));
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
    advance(std::chrono::seconds(1));
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, LatestSuccessfulProbeRenewsWindow)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(std::chrono::seconds(59));
    ASSERT_TRUE(guard.checkAvailable());
    guard.setUnavailable();
    advance(std::chrono::seconds(1));
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(59));
}

TEST_F(TestMPPTaskShutdownGuard, UnavailableProbesCannotExtendWindow)
{
    ASSERT_TRUE(guard.checkAvailable());
    guard.setUnavailable();
    advance(std::chrono::seconds(59));
    EXPECT_FALSE(guard.checkAvailable());
    guard.setUnavailable();
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(1));
    advance(std::chrono::seconds(1));
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, ExpiredProbeDoesNotDelayShutdown)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(std::chrono::seconds(61));
    guard.setUnavailable();
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, ConfiguredWindowAndDisabledProtection)
{
    ASSERT_TRUE(guard.checkAvailable());
    guard.setUnavailable();
    advance(std::chrono::seconds(5));
    EXPECT_EQ(guard.getRemainingProtection(std::chrono::seconds(10)), std::chrono::seconds(5));
    EXPECT_EQ(guard.getRemainingProtection(std::chrono::seconds(120)), std::chrono::seconds(115));
    EXPECT_EQ(guard.getRemainingProtection(std::chrono::seconds(0)), std::chrono::seconds(0));
    advance(std::chrono::seconds(5));
    EXPECT_EQ(guard.getRemainingProtection(std::chrono::seconds(10)), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, ConcurrentProbesAndShutdown)
{
    // Ensure there is a successful probe irrespective of how the racing threads are scheduled.
    ASSERT_TRUE(guard.checkAvailable());
    std::atomic<bool> start{false};
    std::vector<std::thread> probes;
    for (size_t i = 0; i < 8; ++i)
    {
        probes.emplace_back([&] {
            while (!start.load())
                std::this_thread::yield();
            for (size_t j = 0; j < 100; ++j)
                guard.checkAvailable();
        });
    }
    start = true;
    guard.setUnavailable();
    for (auto & probe : probes)
        probe.join();
    advance(std::chrono::seconds(60));
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), std::chrono::seconds(0));
}

TEST_F(TestMPPTaskShutdownGuard, SuccessfulProbeIsRecordedBeforeConcurrentShutdown)
{
    using namespace std::chrono_literals;
    MPPTaskShutdownGuard<BlockingProbeClock> blocking_guard;
    std::promise<void> entered;
    std::promise<void> resume;
    auto probe_entered = entered.get_future();
    BlockingProbeClock::entered = &entered;
    BlockingProbeClock::resume = resume.get_future().share();
    auto probe = std::async(std::launch::async, [&] { return blocking_guard.checkAvailable(); });
    const auto entered_status = probe_entered.wait_for(2s);
    EXPECT_EQ(entered_status, std::future_status::ready);
    std::promise<void> shutdown_started;
    auto shutdown_entered = shutdown_started.get_future();
    auto shutdown = std::async(std::launch::async, [&] {
        shutdown_started.set_value();
        blocking_guard.setUnavailable();
    });
    shutdown_entered.wait();
    // Clock::now() is called with the guard locked. Shutdown cannot finish before the successful probe is recorded.
    if (entered_status == std::future_status::ready)
        EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    advance(5s);
    resume.set_value();
    EXPECT_TRUE(probe.get());
    shutdown.get();
    BlockingProbeClock::entered = nullptr;
    advance(59s);
    EXPECT_FALSE(blocking_guard.checkAvailable());
    EXPECT_EQ(blocking_guard.getRemainingProtection(), 1s);
    advance(1s);
    EXPECT_EQ(blocking_guard.getRemainingProtection(), 0s);
}
} // namespace DB::tests
