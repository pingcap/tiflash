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

namespace DB::tests
{
using namespace std::chrono_literals;
namespace
{
struct ProbeTestClock
{
    using duration = std::chrono::steady_clock::duration;
    using time_point = std::chrono::time_point<ProbeTestClock>;
    static time_point now()
    {
        if (entered != nullptr)
        {
            entered->set_value();
            resume.wait();
        }
        return time_point(duration(ticks.load()));
    }
    static inline std::atomic<duration::rep> ticks{0};
    static inline std::promise<void> * entered = nullptr;
    static inline std::shared_future<void> resume;
};

class TestMPPTaskShutdownGuard : public testing::Test
{
protected:
    void SetUp() override
    {
        ProbeTestClock::ticks = 0;
        ProbeTestClock::entered = nullptr;
    }

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
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
}

TEST_F(TestMPPTaskShutdownGuard, WindowExpiresAtFiveSecondsAfterProbe)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(2s);
    guard.setUnavailable();
    EXPECT_EQ(guard.getRemainingProtection(), 3s);
    advance(2s);
    EXPECT_EQ(guard.getRemainingProtection(), 1s);
    advance(1s);
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
    advance(1s);
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
}

TEST_F(TestMPPTaskShutdownGuard, OnlySuccessfulProbesRenewWindow)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(4s);
    ASSERT_TRUE(guard.checkAvailable());
    guard.setUnavailable();
    advance(4s);
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), 1s);
    advance(1s);
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
}

TEST_F(TestMPPTaskShutdownGuard, ExpiredProbeDoesNotDelayShutdown)
{
    ASSERT_TRUE(guard.checkAvailable());
    advance(6s);
    guard.setUnavailable();
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
}

TEST_F(TestMPPTaskShutdownGuard, SuccessfulProbeIsRecordedBeforeConcurrentShutdown)
{
    std::promise<void> entered;
    std::promise<void> resume;
    auto probe_entered = entered.get_future();
    ProbeTestClock::entered = &entered;
    ProbeTestClock::resume = resume.get_future().share();
    auto probe = std::async(std::launch::async, [&] { return guard.checkAvailable(); });
    const auto entered_status = probe_entered.wait_for(2s);
    EXPECT_EQ(entered_status, std::future_status::ready);
    std::promise<void> shutdown_started;
    auto shutdown_entered = shutdown_started.get_future();
    auto shutdown = std::async(std::launch::async, [&] {
        shutdown_started.set_value();
        guard.setUnavailable();
    });
    shutdown_entered.wait();
    // Clock::now() is called with the guard locked. Shutdown cannot finish before the successful probe is recorded.
    if (entered_status == std::future_status::ready)
        EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    advance(5s);
    resume.set_value();
    EXPECT_TRUE(probe.get());
    shutdown.get();
    ProbeTestClock::entered = nullptr;
    advance(4s);
    EXPECT_FALSE(guard.checkAvailable());
    EXPECT_EQ(guard.getRemainingProtection(), 1s);
    advance(1s);
    EXPECT_EQ(guard.getRemainingProtection(), 0s);
}
} // namespace DB::tests
