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

#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ReadIndexWorker.h>
#include <kvproto/raft_serverpb.pb.h>
#include <raft_cmdpb.pb.h>

#include <ext/singleton.h>

namespace DB
{
kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts);

struct MockProxyRegion : MutexLockWrap
{
    raft_serverpb::RegionLocalState getState();
    raft_serverpb::RaftApplyState getApply();
    void updateAppliedIndex(uint64_t index);
    uint64_t getLatestAppliedIndex();
    uint64_t getLatestCommitTerm();
    uint64_t getLatestCommitIndex();
    void updateCommitIndex(uint64_t index);
    void setSate(raft_serverpb::RegionLocalState);
    explicit MockProxyRegion(uint64_t id);

    struct NormalWrite
    {
        std::vector<HandleID> keys;
        std::vector<std::string> vals;
        std::vector<WriteCmdType> cmd_types;
        std::vector<ColumnFamilyType> cmd_cf;
    };

    struct AdminCommand
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        raft_cmdpb::AdminCmdType cmd_type() const
        {
            return request.cmd_type();
        }
    };

    struct CachedCommand
    {
        uint64_t term;
        std::variant<NormalWrite, AdminCommand> inner;

        bool has_admin_request() const
        {
            return std::holds_alternative<AdminCommand>(inner);
        }

        bool has_write_request() const
        {
            return std::holds_alternative<NormalWrite>(inner);
        }

        AdminCommand & admin()
        {
            return std::get<AdminCommand>(inner);
        }

        NormalWrite & write()
        {
            return std::get<NormalWrite>(inner);
        }
    };

    const uint64_t id;
    raft_serverpb::RegionLocalState state;
    raft_serverpb::RaftApplyState apply;
    std::map<uint64_t, CachedCommand> commands;
};

using MockProxyRegionPtr = std::shared_ptr<MockProxyRegion>;

struct MockAsyncNotifier
{
    RawCppPtr data; // notifier
    void (*wake_fn)(RawVoidPtr);
    void wake() const
    {
        wake_fn(data.ptr);
    }
    ~MockAsyncNotifier()
    {
        GcRawCppPtr(data.ptr, data.type);
    }
};

struct MockAsyncWaker
{
    std::shared_ptr<MockAsyncNotifier> data;
};

struct RawMockReadIndexTask : MutexLockWrap
{
    std::optional<kvrpcpb::ReadIndexResponse> poll(std::shared_ptr<MockAsyncNotifier> waker);

    void update(bool lock = false, bool region_error = false);

    kvrpcpb::ReadIndexRequest req;
    kvrpcpb::ReadIndexResponse resp;

    MockProxyRegionPtr region;
    std::shared_ptr<MockAsyncNotifier> waker;
    bool has_lock{false};
    bool has_region_error{false};
    bool finished{false};
};

struct MockReadIndexTask
{
    std::shared_ptr<RawMockReadIndexTask> data;
};

struct MockRaftStoreProxy : MutexLockWrap
{
    MockProxyRegionPtr getRegion(uint64_t id);

    MockProxyRegionPtr doGetRegion(uint64_t id);

    MockReadIndexTask * makeReadIndexTask(kvrpcpb::ReadIndexRequest req);

    void init(size_t region_num);

    size_t size() const;

    void wake();

    void testRunNormal(const std::atomic_bool & over);

    void runOneRound();

    void unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb);

    static TiFlashRaftProxyHelper SetRaftStoreProxyFFIHelper(RaftStoreProxyPtr);
    /// Mutation funcs.
    struct FailCond
    {
        enum Type
        {
            NORMAL,
            BEFORE_KVSTORE_WRITE,
            BEFORE_KVSTORE_ADVANCE,
            BEFORE_PROXY_ADVANCE,
        };
        Type type = NORMAL;
    };

    /// boostrap a region.
    void bootstrap(
        KVStore & kvs,
        TMTContext & tmt,
        UInt64 region_id);

    /// boostrap a table, since applying snapshot needs table schema.
    TableID bootstrap_table(
        Context & ctx,
        KVStore & kvs,
        TMTContext & tmt);

    /// clear tables.
    void clear_tables(
        Context & ctx,
        KVStore & kvs,
        TMTContext & tmt);

    /// We assume that we generate one command, and immediately commit.
    /// normal write to a region.
    std::tuple<uint64_t, uint64_t> normalWrite(
        UInt64 region_id,
        std::vector<HandleID> && keys,
        std::vector<std::string> && vals,
        std::vector<WriteCmdType> && cmd_types,
        std::vector<ColumnFamilyType> && cmd_cf);

    /// Create a compactLog admin command, returns (index, term) of the admin command itself.
    std::tuple<uint64_t, uint64_t> compactLog(UInt64 region_id, UInt64 compact_index);

    struct Cf
    {
        Cf(UInt64 region_id_, TableID table_id_, ColumnFamilyType type_);

        // Actual data will be stored in MockSSTReader.
        void finish_file();
        void freeze() { freezed = true; }

        void insert(HandleID key, std::string val);

        ColumnFamilyType cf_type() const
        {
            return type;
        }

        // Only use this after all sst_files is generated.
        // vector::push_back can cause destruction of std::string,
        // which is referenced by SSTView.
        std::vector<SSTView> ssts() const;

    protected:
        UInt64 region_id;
        TableID table_id;
        ColumnFamilyType type;
        std::vector<std::string> sst_files;
        std::vector<std::pair<std::string, std::string>> kvs;
        int c;
        bool freezed;
    };

    void snapshot(
        KVStore & kvs,
        TMTContext & tmt,
        UInt64 region_id,
        std::vector<Cf> && cfs,
        uint64_t index,
        uint64_t term);

    void doApply(
        KVStore & kvs,
        TMTContext & tmt,
        const FailCond & cond,
        UInt64 region_id,
        uint64_t index);

    void replay(
        KVStore & kvs,
        TMTContext & tmt,
        uint64_t region_id,
        uint64_t to);

    MockRaftStoreProxy()
    {
        log = Logger::get("MockRaftStoreProxy");
        table_id = 1;
    }

    std::unordered_set<uint64_t> region_id_to_drop;
    std::unordered_set<uint64_t> region_id_to_error;
    std::map<uint64_t, MockProxyRegionPtr> regions;
    std::list<std::shared_ptr<RawMockReadIndexTask>> tasks;
    AsyncWaker::Notifier notifier;
    TableID table_id;
    LoggerPtr log;
};

enum class RawObjType : uint32_t
{
    None,
    MockReadIndexTask,
    MockAsyncWaker,
};

struct GCMonitor : MutexLockWrap
    , public ext::Singleton<GCMonitor>
{
    void add(RawObjType type, int64_t diff);

    bool checkClean();

    bool empty();

    std::unordered_map<RawObjType, int64_t> data;

    static GCMonitor global_gc_monitor;
};

} // namespace DB
