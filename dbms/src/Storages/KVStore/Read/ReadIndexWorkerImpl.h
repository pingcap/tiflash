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

#pragma once

#include <Common/MemoryAllocTrace.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <fmt/chrono.h>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

#include <queue>

namespace DB
{
// #define ADD_TEST_DEBUG_LOG_FMT

#ifdef ADD_TEST_DEBUG_LOG_FMT

static Poco::Logger * global_logger_for_test = nullptr;
static std::mutex global_logger_mutex;

#define TEST_LOG_FMT(...) LOG_ERROR(global_logger_for_test, __VA_ARGS__)

inline void F_TEST_LOG_FMT(const std::string & s)
{
    auto _ = std::lock_guard(global_logger_mutex);
    std::cout << s << std::endl;
}
#else
#define TEST_LOG_FMT(...)
inline void F_TEST_LOG_FMT(const std::string &) {}
#endif


class BlockedReadIndexHelperTrait
{
public:
    explicit BlockedReadIndexHelperTrait(uint64_t timeout_ms_)
        : time_point(SteadyClock::now() + std::chrono::milliseconds{timeout_ms_})
    {}
    virtual AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point) = 0;

    // block current runtime and wait.
    virtual AsyncNotifier::Status blockedWait()
    {
        // TODO: use async process if supported by framework
        return blockedWaitUtil(time_point);
    }
    virtual ~BlockedReadIndexHelperTrait() = default;

protected:
    SteadyClock::time_point time_point;
};

class BlockedReadIndexHelper final : public BlockedReadIndexHelperTrait
{
public:
    BlockedReadIndexHelper(uint64_t timeout_ms_, AsyncWaker & waker_)
        : BlockedReadIndexHelperTrait(timeout_ms_)
        , waker(waker_)
    {}

    const AsyncWaker & getWaker() const { return waker; }

    AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point time_point) override
    {
        return waker.waitUtil(time_point);
    }

    ~BlockedReadIndexHelper() override = default;

private:
    AsyncWaker & waker;
};

class BlockedReadIndexHelperV3 final : public BlockedReadIndexHelperTrait
{
public:
    BlockedReadIndexHelperV3(uint64_t timeout_ms_, AsyncWaker::Notifier & notifier_)
        : BlockedReadIndexHelperTrait(timeout_ms_)
        , notifier(notifier_)
    {}

    AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point time_point) override
    {
        return notifier.blockedWaitUtil(time_point);
    }

    ~BlockedReadIndexHelperV3() override = default;

private:
    AsyncWaker::Notifier & notifier;
};


inline std::optional<ReadIndexTask> makeReadIndexTask(
    const TiFlashRaftProxyHelper & helper,
    kvrpcpb::ReadIndexRequest & req)
{
    if (likely(!MockStressTestCfg::enable))
        return helper.makeReadIndexTask(req);
    else
    {
        auto ori_id = req.context().region_id();
        req.mutable_context()->set_region_id(
            ori_id % MockStressTestCfg::RegionIdPrefix); // set region id to original one.
        TEST_LOG_FMT("hacked ReadIndexTask to req {}", req.ShortDebugString());
        auto res = helper.makeReadIndexTask(req);
        req.mutable_context()->set_region_id(ori_id);
        return res;
    }
}


struct ReadIndexNotifyCtrl : MutexLockWrap
{
    using Data = std::deque<std::pair<RegionID, Timestamp>>;

    bool empty() const NO_THREAD_SAFETY_ANALYSIS
    {
        auto _ = genLockGuard();
        return data.empty();
    }
    void add(RegionID id, Timestamp ts) NO_THREAD_SAFETY_ANALYSIS
    {
        auto _ = genLockGuard();
        data.emplace_back(id, ts);
    }
    Data popAll() NO_THREAD_SAFETY_ANALYSIS
    {
        auto _ = genLockGuard();
        return std::move(data);
    }

    void wake() const { notifier->wake(); }

    explicit ReadIndexNotifyCtrl(AsyncWaker::NotifierPtr notifier_)
        : notifier(notifier_)
    {}

    Data data;
    AsyncWaker::NotifierPtr notifier;
};

struct RegionReadIndexNotifier final : AsyncNotifier
{
    void wake() override
    {
        notify->add(region_id, ts);
        notify->wake();
    }
    Status blockedWaitUtil(const SteadyClock::time_point &) override { return Status::Timeout; }

    ~RegionReadIndexNotifier() override = default;

    RegionReadIndexNotifier(RegionID region_id_, Timestamp ts_, const ReadIndexNotifyCtrlPtr & notify_)
        : region_id(region_id_)
        , ts(ts_)
        , notify(notify_)
    {}

    RegionID region_id;
    Timestamp ts;
    ReadIndexNotifyCtrlPtr notify;
};

} // namespace DB
