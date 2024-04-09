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

#include <Common/nocopyable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/Utils.h>
#include <common/logger_useful.h>
#include <kvproto/kvrpcpb.pb.h>

#include <algorithm>
#include <condition_variable>
#include <memory>
#include <thread>

namespace DB
{
namespace tests
{
class ReadIndexTest;
} // namespace tests

struct AsyncWaker
{
    struct Notifier final
        : AsyncNotifier
        , MutexCondVarWrap
    {
        // usually sender invoke `wake`, receiver invoke `blockedWaitUtil`
        // NOT thread safe
        Status blockedWaitUtil(const SteadyClock::time_point &) override;
        // thread safe
        void wake() override;

        ~Notifier() override = default;

    private:
        // multi notifiers single receiver model. use another flag to avoid waiting endlessly.
        AlignedStruct<std::atomic_bool, CPU_CACHE_LINE_SIZE> is_awake{false};
    };
    using NotifierPtr = std::shared_ptr<Notifier>;

    // proxy will call this function to invoke `AsyncNotifier::wake`
    static void wake(RawVoidPtr notifier_);

    // create a `Notifier` in heap & let proxy wrap it and return as rust ptr with specific type.
    explicit AsyncWaker(const TiFlashRaftProxyHelper & helper_);

    AsyncNotifier::Status waitUtil(SteadyClock::time_point);

    RawVoidPtr getRaw() const;

    AsyncWaker(const TiFlashRaftProxyHelper & helper_, AsyncNotifier * notifier_);

private:
    RawRustPtrWrap inner;
    // notifier is always in heap and is maintained in raftstore proxy as shared obj.
    AsyncNotifier & notifier;
};

struct ReadIndexWorker;
using ReadIndexWorkers = std::vector<std::unique_ptr<ReadIndexWorker>>;
struct ReadIndexFuture;
using ReadIndexFuturePtr = std::shared_ptr<ReadIndexFuture>;

class ReadIndexWorkerManager : boost::noncopyable
{
public:
    using FnGetTickTime = std::function<std::chrono::milliseconds()>;

    ReadIndexWorker & getWorkerByRegion(RegionID region_id);

    explicit ReadIndexWorkerManager(
        const TiFlashRaftProxyHelper & proxy_helper_,
        KVStore & kvstore_,
        size_t workers_cnt,
        FnGetTickTime && fn_min_dur_handle_region_,
        size_t runner_cnt);

    void wakeAll(); // wake all runners to handle tasks
    void asyncRun();
    void runOneRound(SteadyClock::duration min_dur, size_t id);
    void stop();
    ~ReadIndexWorkerManager();
    BatchReadIndexRes batchReadIndex(
        const std::vector<kvrpcpb::ReadIndexRequest> & reqs,
        uint64_t timeout_ms = 10 * 1000);

    static std::unique_ptr<ReadIndexWorkerManager> newReadIndexWorkerManager(
        const TiFlashRaftProxyHelper & proxy_helper,
        KVStore & kvstore,
        size_t cap,
        FnGetTickTime && fn_min_dur_handle_region,
        size_t runner_cnt = 1);

    ReadIndexFuturePtr genReadIndexFuture(const kvrpcpb::ReadIndexRequest & req);

private:
    void runOneRoundAll(SteadyClock::duration min_dur = std::chrono::milliseconds{0});

    enum class State : uint8_t
    {
        Running,
        Stopping,
        Terminated,
    };

    friend class tests::ReadIndexTest;

    struct ReadIndexRunner : boost::noncopyable
    {
        void wake() const;

        void stop();

        void blockedWaitFor(std::chrono::milliseconds timeout) const;

        /// Traverse its workers and try to execute tasks.
        void runOneRound(SteadyClock::duration min_dur);

        /// Create one thread to run asynchronously.
        void asyncRun();

        ReadIndexRunner(
            size_t id_,
            size_t runner_cnt_,
            KVStore & kvstore_,
            ReadIndexWorkers & workers_,
            LoggerPtr logger_,
            FnGetTickTime fn_min_dur_handle_region_,
            AsyncWaker::NotifierPtr global_notifier_);

        const size_t id;
        const size_t runner_cnt;
        KVStore & kvstore;
        ReadIndexWorkers & workers;
        LoggerPtr logger;
        const FnGetTickTime fn_min_dur_handle_region;
        /// The workers belonged to runner share same notifier.
        AsyncWaker::NotifierPtr global_notifier;
        std::unique_ptr<std::thread> work_thread;
        std::atomic<State> state{State::Running};
    };

private:
    const TiFlashRaftProxyHelper & proxy_helper;
    KVStore & kvstore;
    /// Each runner is mapped to a part of workers(worker_id % runner_cnt == runner_id).
    std::vector<std::unique_ptr<ReadIndexRunner>> runners;
    /// Each worker controls read-index process of region(region_id % worker_cnt == worker_id).
    ReadIndexWorkers workers;
    LoggerPtr logger;
};

struct ReadIndexNotifyCtrl;
using ReadIndexNotifyCtrlPtr = std::shared_ptr<ReadIndexNotifyCtrl>;

struct RegionNotifyMap : MutexLockWrap
{
    using Data = std::unordered_set<RegionID>;

    bool empty() const
    {
        auto _ = genLockGuard();
        return data.empty();
    }
    void add(RegionID id)
    {
        auto _ = genLockGuard();
        data.emplace(id);
    }
    Data popAll()
    {
        auto _ = genLockGuard();
        return std::move(data);
    }

    Data data;
};

struct ReadIndexFuture : MutexLockWrap
{
    void update(kvrpcpb::ReadIndexResponse resp);

    std::optional<kvrpcpb::ReadIndexResponse> poll(const std::shared_ptr<AsyncNotifier> & notifier_ = nullptr) const;

    kvrpcpb::ReadIndexRequest req;
    kvrpcpb::ReadIndexResponse resp;
    bool finished{false};
    mutable std::shared_ptr<AsyncNotifier> notifier;
};

struct ReadIndexDataNode : MutexLockWrap
{
    struct ReadIndexElement
    {
        using Task = std::optional<std::pair<ReadIndexTask, AsyncWaker>>;

        explicit ReadIndexElement(const RegionID region_id_, const Timestamp start_ts_)
            : region_id(region_id_)
            , start_ts(start_ts_)
        {}

        DISALLOW_COPY(ReadIndexElement);

        void doTriggerCallbacks();

        void doForceSet(const kvrpcpb::ReadIndexResponse & f);

        void doPoll(const TiFlashRaftProxyHelper & helper, std::chrono::milliseconds timeout);

        const RegionID region_id;
        const Timestamp start_ts;
        Task task_pair;
        kvrpcpb::ReadIndexResponse resp;
        std::deque<ReadIndexFuturePtr> callbacks;
        SteadyClock::time_point start_time = SteadyClock::now();
    };

    struct WaitingTasks : MutexLockWrap
    {
        using Data = std::deque<std::pair<Timestamp, ReadIndexFuturePtr>>;

        void add(Timestamp ts, ReadIndexFuturePtr f)
        {
            auto _ = genLockGuard();
            waiting_tasks.emplace_back(ts, std::move(f));
        }

        std::optional<Data> popAll()
        {
            auto _ = genLockGuard();
            if (waiting_tasks.empty())
                return {};
            return std::move(waiting_tasks);
        }

        size_t size() const
        {
            auto _ = genLockGuard();
            return waiting_tasks.size();
        }

        Data waiting_tasks;
    };

    using RunningTasks = std::map<Timestamp, ReadIndexElement>;
    using HistorySuccessTasks = std::optional<std::pair<Timestamp, kvrpcpb::ReadIndexResponse>>;

    void doAddHistoryTasks(Timestamp ts, kvrpcpb::ReadIndexResponse && resp);

    void doConsume(const TiFlashRaftProxyHelper & helper, RunningTasks::iterator it);

    void consume(const TiFlashRaftProxyHelper & helper, Timestamp ts);

    void runOneRound(const TiFlashRaftProxyHelper & helper, const ReadIndexNotifyCtrlPtr & notify);

    ReadIndexFuturePtr insertTask(const kvrpcpb::ReadIndexRequest & req);

    ~ReadIndexDataNode();

    explicit ReadIndexDataNode(const RegionID region_id_)
        : region_id(region_id_)
    {}

    const RegionID region_id;

    WaitingTasks waiting_tasks;
    RunningTasks running_tasks;
    HistorySuccessTasks history_success_tasks;

    size_t cnt_use_history_tasks{};
};

using ReadIndexDataNodePtr = std::shared_ptr<ReadIndexDataNode>;

struct ReadIndexWorker
{
    struct DataMap : SharedMutexLockWrap
    {
        ReadIndexDataNodePtr upsertDataNode(RegionID region_id) const;

        ReadIndexDataNodePtr tryGetDataNode(RegionID region_id) const;

        ReadIndexDataNodePtr getDataNode(RegionID region_id) const;

        void invoke(std::function<void(std::unordered_map<RegionID, ReadIndexDataNodePtr> &)> &&);

        void removeRegion(RegionID);

        mutable std::unordered_map<RegionID, ReadIndexDataNodePtr> region_map;
    };

    void consumeReadIndexNotifyCtrl();

    void consumeRegionNotifies(SteadyClock::duration min_dur);

    ReadIndexFuturePtr genReadIndexFuture(const kvrpcpb::ReadIndexRequest & req);

    // try to consume read-index response notifications & region waiting list
    void runOneRound(SteadyClock::duration min_dur);

    explicit ReadIndexWorker(
        const TiFlashRaftProxyHelper & proxy_helper_,
        KVStore & kvstore_,
        size_t id_,
        AsyncWaker::NotifierPtr notifier_);

    size_t getID() const { return id; }

    static std::chrono::milliseconds getMaxReadIndexTaskTimeout()
    {
        return max_read_index_task_timeout.load(std::memory_order_relaxed);
    }
    static void setMaxReadIndexTaskTimeout(std::chrono::milliseconds t) { max_read_index_task_timeout = t; }
    //    static size_t getMaxReadIndexHistory()
    //    {
    //        return max_read_index_history.load(std::memory_order_relaxed);
    //    }
    //    static void setMaxReadIndexHistory(size_t x)
    //    {
    //        x = x == 0 ? 1 : x;
    //        max_read_index_history = x;
    //    }
    bool lastRunTimeout(SteadyClock::duration timeout) const;

    void removeRegion(uint64_t);

    static std::atomic<std::chrono::milliseconds> max_read_index_task_timeout;
    //    static std::atomic<size_t> max_read_index_history;

    const TiFlashRaftProxyHelper & proxy_helper;
    KVStore & kvstore;
    const size_t id;
    DataMap data_map;

    // proxy can add read-index response notifications and wake runner through it.
    ReadIndexNotifyCtrlPtr read_index_notify_ctrl;

    // insert region read-index request to waiting list, then add notifications.
    // worker will generate related task and send to proxy.
    RegionNotifyMap region_notify_map;

    // no need to be protected
    std::atomic<SteadyClock::time_point> last_run_time{SteadyClock::time_point::min()};
};

struct MockStressTestCfg
{
    /// Usually, there may be a small number of region in one store. Using prefix can multiply the number.
    static const uint64_t RegionIdPrefix;
    /// If enabled, `ReadIndexStressTest` will modify region id in `kvrpcpb::ReadIndexRequest` to add prefix.
    static bool enable;
};

} // namespace DB
