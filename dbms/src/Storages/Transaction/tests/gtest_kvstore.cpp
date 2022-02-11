#include <Debug/MockSSTReader.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "region_helper.h"

namespace DB
{
namespace RegionBench
{
extern void setupPutRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &, const TiKVValue &);
extern void setupDelRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &);
} // namespace RegionBench

extern std::optional<RegionDataReadInfoList> ReadRegionCommitCache(const RegionPtr & region, bool lock_region = true);
extern void RemoveRegionCommitCache(const RegionPtr & region, const RegionDataReadInfoList & data_list_read, bool lock_region = true);

namespace tests
{
RegionPtr makeRegion(UInt64 id, const std::string start_key, const std::string end_key)
{
    return std::make_shared<Region>(
        RegionMeta(createPeer(2, true), createRegionInfo(id, std::move(start_key), std::move(end_key)), initialApplyState()));
}

class RegionKVStoreTest : public ::testing::Test
{
public:
    RegionKVStoreTest()
        = default;

    static void SetUpTestCase() {}
    static void testBasic();
    static void testKVStore();
    static void testRegion();
    static void testRegionManager();

private:
    static void testRaftSplit(KVStore & kvs, TMTContext & tmt);
    static void testRaftMerge(KVStore & kvs, TMTContext & tmt);
    static void testRaftChangePeer(KVStore & kvs, TMTContext & tmt);
    static void testRaftMergeRollback(KVStore & kvs, TMTContext & tmt);
};

void RegionKVStoreTest::testRaftMergeRollback(KVStore & kvs, TMTContext & tmt)
{
    uint64_t region_id = 7;
    {
        auto source_region = kvs.getRegion(region_id);
        auto target_region = kvs.getRegion(1);

        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        {
            request.set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);
            auto * prepare_merge = request.mutable_prepare_merge();
            {
                auto min_index = source_region->appliedIndex();
                prepare_merge->set_min_index(min_index);

                metapb::Region * target = prepare_merge->mutable_target();
                *target = target_region->getMetaRegion();
            }
        }
        kvs.handleAdminRaftCmd(std::move(request),
                               std::move(response),
                               region_id,
                               31,
                               6,
                               tmt);
        ASSERT_TRUE(source_region->isMerging());
    }
    {
        auto region = kvs.getRegion(region_id);

        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        {
            request.set_cmd_type(raft_cmdpb::AdminCmdType::RollbackMerge);

            auto * rollback_merge = request.mutable_rollback_merge();
            {
                auto merge_state = region->getMergeState();
                rollback_merge->set_commit(merge_state.commit());
            }
        }
        kvs.handleAdminRaftCmd(std::move(request),
                               std::move(response),
                               region_id,
                               32,
                               6,
                               tmt);
        ASSERT_EQ(region->peerState(), raft_serverpb::PeerState::Normal);
    }
}

void RegionKVStoreTest::testRaftSplit(KVStore & kvs, TMTContext & tmt)
{
    {
        auto region = kvs.getRegion(1);
        auto table_id = 1;
        region->insert("lock", RecordKVFormat::genKey(table_id, 3), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert("write", RecordKVFormat::genKey(table_id, 3, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        region->insert("lock", RecordKVFormat::genKey(table_id, 8), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 8, 5), TiKVValue("value1"));
        region->insert("write", RecordKVFormat::genKey(table_id, 8, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));

        ASSERT_EQ(region->dataInfo(), "[write 2 lock 2 default 2 ]");
    }
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    {
        // split region
        auto region_id = 1;
        RegionID region_id2 = 7;
        auto source_region = kvs.getRegion(region_id);
        metapb::RegionEpoch new_epoch;
        new_epoch.set_version(source_region->version() + 1);
        new_epoch.set_conf_ver(source_region->confVer());
        TiKVKey start_key1, start_key2, end_key1, end_key2;
        {
            start_key1 = RecordKVFormat::genKey(1, 5);
            start_key2 = RecordKVFormat::genKey(1, 0);
            end_key1 = RecordKVFormat::genKey(1, 10);
            end_key2 = RecordKVFormat::genKey(1, 5);
        }
        {
            request.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);
            raft_cmdpb::BatchSplitResponse * splits = response.mutable_splits();
            {
                auto * region = splits->add_regions();
                region->set_id(region_id);
                region->set_start_key(start_key1);
                region->set_end_key(end_key1);
                region->add_peers();
                *region->mutable_region_epoch() = new_epoch;
            }
            {
                auto * region = splits->add_regions();
                region->set_id(region_id2);
                region->set_start_key(start_key2);
                region->set_end_key(end_key2);
                region->add_peers();
                *region->mutable_region_epoch() = new_epoch;
            }
        }
    }
    kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 1, 20, 5, tmt);
    {
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
        ASSERT_TRUE(mmp.count(7) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 5), RecordKVFormat::genKey(1, 10)));
        ASSERT_TRUE(mmp.count(1) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[write 1 lock 1 default 1 ]");
        ASSERT_EQ(kvs.getRegion(7)->dataInfo(), "[lock 1 ]");
    }
    // rollback 1 to before split
    // 7 is persisted
    {
        kvs.handleDestroy(1, tmt);
        {
            auto lock = kvs.genRegionWriteLock();
            auto region = makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10));
            lock.regions.emplace(1, region);
            lock.index.add(region);
        }
        auto table_id = 1;
        auto region = kvs.getRegion(1);
        region->insert("lock", RecordKVFormat::genKey(table_id, 3), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert("write", RecordKVFormat::genKey(table_id, 3, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        region->insert("lock", RecordKVFormat::genKey(table_id, 8), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 8, 5), TiKVValue("value1"));
        region->insert("write", RecordKVFormat::genKey(table_id, 8, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));

        ASSERT_EQ(region->dataInfo(), "[write 2 lock 2 default 2 ]");
    }
    {
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
        ASSERT_TRUE(mmp.count(7) != 0);
        ASSERT_TRUE(mmp.count(1) != 0);
        ASSERT_EQ(mmp.size(), 2);
    }
    // split again
    kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 1, 20, 5, tmt);
    {
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
        ASSERT_TRUE(mmp.count(7) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 5), RecordKVFormat::genKey(1, 10)));
        ASSERT_TRUE(mmp.count(1) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[write 1 lock 1 default 1 ]");
        ASSERT_EQ(kvs.getRegion(7)->dataInfo(), "[lock 1 ]");
    }
}

void RegionKVStoreTest::testRaftChangePeer(KVStore & kvs, TMTContext & tmt)
{
    {
        auto lock = kvs.genRegionWriteLock();
        auto region = makeRegion(88, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 100));
        lock.regions.emplace(88, region);
        lock.index.add(region);
    }
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);
        auto meta = kvs.getRegion(88)->getMetaRegion();
        meta.mutable_peers()->Clear();
        meta.add_peers()->set_id(2);
        meta.add_peers()->set_id(4);
        *response.mutable_change_peer()->mutable_region() = meta;
        kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 88, 6, 5, tmt);
        ASSERT_NE(kvs.getRegion(88), nullptr);
    }
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeerV2);
        auto meta = kvs.getRegion(88)->getMetaRegion();
        meta.mutable_peers()->Clear();
        meta.add_peers()->set_id(3);
        meta.add_peers()->set_id(4);
        *response.mutable_change_peer()->mutable_region() = meta;
        kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 88, 7, 5, tmt);
        ASSERT_EQ(kvs.getRegion(88), nullptr);
    }
}

void RegionKVStoreTest::testRaftMerge(KVStore & kvs, TMTContext & tmt)
{
    {
        kvs.getRegion(1)->clearAllData();
        kvs.getRegion(7)->clearAllData();

        {
            auto region = kvs.getRegion(1);
            auto table_id = 1;
            region->insert("lock", RecordKVFormat::genKey(table_id, 6), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
            region->insert("default", RecordKVFormat::genKey(table_id, 6, 5), TiKVValue("value1"));
            region->insert("write", RecordKVFormat::genKey(table_id, 6, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        }
        {
            auto region = kvs.getRegion(7);
            auto table_id = 1;
            region->insert("lock", RecordKVFormat::genKey(table_id, 2), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
            region->insert("default", RecordKVFormat::genKey(table_id, 2, 5), TiKVValue("value1"));
            region->insert("write", RecordKVFormat::genKey(table_id, 2, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        }
    }

    {
        auto region_id = 7;
        auto source_region = kvs.getRegion(region_id);
        auto target_region = kvs.getRegion(1);

        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;

        {
            request.set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);

            auto * prepare_merge = request.mutable_prepare_merge();
            {
                auto min_index = source_region->appliedIndex();
                prepare_merge->set_min_index(min_index);

                metapb::Region * target = prepare_merge->mutable_target();
                *target = target_region->getMetaRegion();
            }
        }

        kvs.handleAdminRaftCmd(std::move(request),
                               std::move(response),
                               source_region->id(),
                               35,
                               6,
                               tmt);
        ASSERT_EQ(source_region->peerState(), raft_serverpb::PeerState::Merging);
    }

    {
        auto source_id = 7, target_id = 1;
        auto source_region = kvs.getRegion(source_id);
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;

        {
            request.set_cmd_type(raft_cmdpb::AdminCmdType::CommitMerge);
            auto * commit_merge = request.mutable_commit_merge();
            {
                commit_merge->set_commit(source_region->appliedIndex());
                *commit_merge->mutable_source() = source_region->getMetaRegion();
            }
        }
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
            ASSERT_TRUE(mmp.count(target_id) != 0);
            ASSERT_EQ(mmp.size(), 2);
        }

        kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request),
                               raft_cmdpb::AdminResponse(response),
                               target_id,
                               36,
                               6,
                               tmt);

        ASSERT_EQ(kvs.getRegion(source_id), nullptr);
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
            ASSERT_TRUE(mmp.count(1) != 0);
            ASSERT_EQ(mmp.size(), 1);
        }
        {
            // add 7 back
            auto lock = kvs.genRegionWriteLock();
            auto region = makeRegion(7, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5));
            lock.regions.emplace(7, region);
            lock.index.add(region);
        }
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
            ASSERT_TRUE(mmp.count(7) != 0);
            ASSERT_TRUE(mmp.count(1) != 0);
            ASSERT_EQ(mmp.size(), 2);
        }
        kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request),
                               raft_cmdpb::AdminResponse(response),
                               target_id,
                               36,
                               6,
                               tmt);
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)));
            ASSERT_TRUE(mmp.count(1) != 0);
            ASSERT_EQ(mmp.size(), 1);
        }
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[lock 2 ]");
    }
}

void RegionKVStoreTest::testRegion()
{
    TableID table_id = 100;
    auto region = makeRegion(1, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 1000));
    {
        ASSERT_TRUE(region->checkIndex(5));
    }
    {
        auto start_ts = 199;
        auto req = GenRegionReadIndexReq(*region, start_ts);
        ASSERT_EQ(req.ranges().size(), 1);
        ASSERT_EQ(req.start_ts(), start_ts);
        ASSERT_EQ(region->getMetaRegion().region_epoch().DebugString(),
                  req.context().region_epoch().DebugString());
        ASSERT_EQ(*region->getRange()->rawKeys().first, req.ranges()[0].start_key());
        ASSERT_EQ(*region->getRange()->rawKeys().second, req.ranges()[0].end_key());
    }
    {
        region->insert("lock", RecordKVFormat::genKey(table_id, 3), RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert("write", RecordKVFormat::genKey(table_id, 3, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        ASSERT_EQ(1, region->writeCFCount());
        ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        {
            auto iter = region->createCommittedScanner();
            auto lock = iter.getLockInfo({100, nullptr});
            ASSERT_NE(lock, nullptr);
            auto k = lock->intoLockInfo();
            ASSERT_EQ(k->lock_version(), 3);
        }
        {
            std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region);
            ASSERT_TRUE(data_list_read);
            ASSERT_EQ(1, data_list_read->size());
            RemoveRegionCommitCache(region, *data_list_read);
        }
        ASSERT_EQ(0, region->writeCFCount());
        {
            region->remove("lock", RecordKVFormat::genKey(table_id, 3));
            auto iter = region->createCommittedScanner();
            auto lock = iter.getLockInfo({100, nullptr});
            ASSERT_EQ(lock, nullptr);
        }
        region->clearAllData();
    }
    {
        region->insert("write", RecordKVFormat::genKey(table_id, 3, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        ASSERT_EQ(region->dataInfo(), "[write 1 ]");

        auto ori_size = region->dataSize();
        try
        {
            // insert duplicate records
            region->insert("write", RecordKVFormat::genKey(table_id, 3, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "Found existing key in hex: 7480000000000000FF645F728000000000FF0000030000000000FAFFFFFFFFFFFFFFF7");
        }
        ASSERT_EQ(ori_size, region->dataSize());

        region->tryCompactionFilter(100);
        ASSERT_EQ(region->dataInfo(), "[]");
    }
    {
        region->insert("write", RecordKVFormat::genKey(table_id, 4, 8), RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::DelFlag, 5));
        ASSERT_EQ(1, region->writeCFCount());
        region->remove("write", RecordKVFormat::genKey(table_id, 4, 8));
        ASSERT_EQ(1, region->writeCFCount());
        {
            std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region);
            ASSERT_TRUE(data_list_read);
            ASSERT_EQ(1, data_list_read->size());
            RemoveRegionCommitCache(region, *data_list_read);
        }
        ASSERT_EQ(0, region->writeCFCount());
    }
    {
        ASSERT_EQ(0, region->dataSize());

        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        ASSERT_LT(0, region->dataSize());

        region->remove("default", RecordKVFormat::genKey(table_id, 3, 5));
        ASSERT_EQ(0, region->dataSize());

        // remove duplicate records
        region->remove("default", RecordKVFormat::genKey(table_id, 3, 5));
        ASSERT_EQ(0, region->dataSize());
    }
}

void RegionKVStoreTest::testKVStore()
{
    std::string path = TiFlashTestEnv::getTemporaryPath("/region_kvs_tmp") + "/basic";

    Poco::File file(path);
    if (file.exists())
        file.remove(true);
    file.createDirectories();

    auto ctx = TiFlashTestEnv::getContext(
        DB::Settings(),
        Strings{
            path,
        });
    KVStore & kvs = *ctx.getTMTContext().getKVStore();
    kvs.restore(nullptr);
    {
        auto store = metapb::Store{};
        store.set_id(1234);
        kvs.setStore(store);
        ASSERT_EQ(kvs.getStoreID(), store.id());
    }
    {
        ASSERT_EQ(kvs.getRegion(0), nullptr);
        auto lock = kvs.genRegionWriteLock();
        {
            auto region = makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10));
            lock.regions.emplace(1, region);
            lock.index.add(region);
        }
        {
            auto region = makeRegion(2, RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20));
            lock.regions.emplace(2, region);
            lock.index.add(region);
        }
        {
            auto region = makeRegion(3, RecordKVFormat::genKey(1, 30), RecordKVFormat::genKey(1, 40));
            lock.regions.emplace(3, region);
            lock.index.add(region);
        }
    }
    {
        kvs.tryPersist(1);
        kvs.gcRegionPersistedCache(Seconds{0});
    }
    {
        ASSERT_EQ(kvs.regionSize(), 3);
        auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 15), TiKVKey("")));
        ASSERT_EQ(mmp.size(), 2);
        kvs.handleDestroy(3, ctx.getTMTContext());
        kvs.handleDestroy(3, ctx.getTMTContext());
    }
    {
        RegionMap mmp;
        kvs.handleRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 15), TiKVKey("")), [&](RegionMap m, const KVStoreTaskLock &) {
            mmp = m;
        });
        ASSERT_EQ(mmp.size(), 1);
        ASSERT_EQ(mmp.at(2)->id(), 2);
    }
    {
        {
            raft_cmdpb::RaftCmdRequest request;
            {
                auto lock_key = RecordKVFormat::genKey(1, 2333);
                TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, "pk", 77, 0);
                RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key, lock_value);
                auto write_key = RecordKVFormat::genKey(1, 2333, 1);
                TiKVValue write_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 2333);
                RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Write, write_key, write_value);
            }
            try
            {
                ASSERT_EQ(kvs.handleWriteRaftCmd(std::move(request), 1, 6, 6, ctx.getTMTContext()),
                          EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Raw TiDB PK: 800000000000091D, Prewrite ts: 2333 can not found in default cf for key: 7480000000000000FF015F728000000000FF00091D0000000000FAFFFFFFFFFFFFFFFE");
                ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[write 1 lock 1 ]");
                kvs.getRegion(1)->tryCompactionFilter(1000);
            }
            try
            {
                raft_cmdpb::RaftCmdRequest request;
                {
                    auto key = RecordKVFormat::genKey(1, 2333, 1);
                    RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Default, key, "v1");
                }
                {
                    // duplicate
                    auto key = RecordKVFormat::genKey(1, 2333, 1);
                    RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Default, key, "v1");
                }
                ASSERT_EQ(kvs.handleWriteRaftCmd(std::move(request), 1, 6, 6, ctx.getTMTContext()),
                          EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Found existing key in hex: 7480000000000000FF015F728000000000FF00091D0000000000FAFFFFFFFFFFFFFFFE");
            }
            ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[lock 1 default 1 ]");
            kvs.getRegion(1)->remove("default", RecordKVFormat::genKey(1, 2333, 1));
            try
            {
                raft_cmdpb::RaftCmdRequest request;
                {
                    RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Default, std::string("k1"), "v1");
                }
                ASSERT_EQ(kvs.handleWriteRaftCmd(std::move(request), 1, 6, 6, ctx.getTMTContext()),
                          EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Unexpected eof");
            }
        }
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[lock 1 ]");
        {
            raft_cmdpb::RaftCmdRequest request;
            {
                auto lock_key = RecordKVFormat::genKey(1, 2333);
                TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, "pk", 77, 0);
                RegionBench::setupDelRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key);
            }
            ASSERT_EQ(kvs.handleWriteRaftCmd(std::move(request), 1, 7, 6, ctx.getTMTContext()),
                      EngineStoreApplyRes::None);
        }
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[]");

        ASSERT_EQ(
            kvs.handleWriteRaftCmd(raft_cmdpb::RaftCmdRequest{}, 8192, 7, 6, ctx.getTMTContext()),
            EngineStoreApplyRes::NotFound);
    }
    {
        kvs.handleDestroy(2, ctx.getTMTContext());
        ASSERT_EQ(kvs.regionSize(), 1);
    }
    {
        testRaftSplit(kvs, ctx.getTMTContext());
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{}, raft_cmdpb::AdminResponse{}, 8192, 5, 6, ctx.getTMTContext()), EngineStoreApplyRes::NotFound);
    }
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;

        request.mutable_compact_log();
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);

        ASSERT_EQ(kvs.handleAdminRaftCmd(std::move(request), std::move(response), 7, 22, 6, ctx.getTMTContext()), EngineStoreApplyRes::Persist);
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 7, 23, 6, ctx.getTMTContext()), EngineStoreApplyRes::None);
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::ComputeHash);
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 7, 24, 6, ctx.getTMTContext()), EngineStoreApplyRes::None);
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::VerifyHash);
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 7, 25, 6, ctx.getTMTContext()), EngineStoreApplyRes::None);
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 8192, 5, 6, ctx.getTMTContext()), EngineStoreApplyRes::NotFound);
        {
            kvs.setRegionCompactLogConfig(0, 0, 0);
            request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
            ASSERT_EQ(kvs.handleAdminRaftCmd(std::move(request), std::move(response), 7, 26, 6, ctx.getTMTContext()), EngineStoreApplyRes::Persist);
        }
    }
    {
        testRaftMergeRollback(kvs, ctx.getTMTContext());
        testRaftMerge(kvs, ctx.getTMTContext());
    }
    {
        testRaftChangePeer(kvs, ctx.getTMTContext());
    }
    {
        auto ori_snapshot_apply_method = kvs.snapshot_apply_method;
        kvs.snapshot_apply_method = TiDB::SnapshotApplyMethod::Block;
        SCOPE_EXIT({
            kvs.snapshot_apply_method = ori_snapshot_apply_method;
        });
        auto region_id = 19;
        auto region = makeRegion(region_id, RecordKVFormat::genKey(1, 50), RecordKVFormat::genKey(1, 60));
        auto region_id_str = std::to_string(19);
        auto & mmp = MockSSTReader::getMockSSTData();
        MockSSTReader::getMockSSTData().clear();
        MockSSTReader::Data default_kv_list;
        {
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 55, 5).getStr(), TiKVValue("value1").getStr());
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 58, 5).getStr(), TiKVValue("value2").getStr());
        }
        mmp[MockSSTReader::Key{region_id_str, ColumnFamilyType::Default}] = std::move(default_kv_list);
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Default,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });
        {
            RegionMockTest mock_test(ctx.getTMTContext().getKVStore(), region);

            kvs.handleApplySnapshot(
                region->getMetaRegion(),
                2,
                SSTViewVec{sst_views.data(), sst_views.size()},
                8,
                5,
                ctx.getTMTContext());
            ASSERT_EQ(kvs.getRegion(19)->dataInfo(), "[default 2 ]");
            region = makeRegion(19, RecordKVFormat::genKey(1, 50), RecordKVFormat::genKey(1, 60));
            region->handleWriteRaftCmd({}, 10, 10, ctx.getTMTContext());
            kvs.checkAndApplySnapshot<RegionPtrWithBlock>(region, ctx.getTMTContext());
            ASSERT_EQ(kvs.getRegion(19)->dataInfo(), "[]");
        }
    }
    {
        auto region_id = 19;
        auto region_id_str = std::to_string(19);
        auto & mmp = MockSSTReader::getMockSSTData();
        MockSSTReader::getMockSSTData().clear();
        MockSSTReader::Data default_kv_list;
        {
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 55, 5).getStr(), TiKVValue("value1").getStr());
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 58, 5).getStr(), TiKVValue("value2").getStr());
        }
        mmp[MockSSTReader::Key{region_id_str, ColumnFamilyType::Default}] = std::move(default_kv_list);

        // Mock SST data for handle [star, end)
        auto region = kvs.getRegion(region_id);

        RegionMockTest mock_test(ctx.getTMTContext().getKVStore(), region);

        {
            auto ori_snapshot_apply_method = kvs.snapshot_apply_method;
            kvs.snapshot_apply_method = TiDB::SnapshotApplyMethod::Block;
            SCOPE_EXIT({
                kvs.snapshot_apply_method = ori_snapshot_apply_method;
            });
            // Mocking ingest a SST for column family "Write"
            std::vector<SSTView> sst_views;
            sst_views.push_back(SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            });
            kvs.handleIngestSST(
                region_id,
                SSTViewVec{sst_views.data(), sst_views.size()},
                100,
                1,
                ctx.getTMTContext());
            ASSERT_EQ(kvs.getRegion(19)->dataInfo(), "[default 2 ]");
        }
    }
}

void RegionKVStoreTest::testRegionManager()
{
    RegionManager manager;
    {
        uint64_t rid = 666;
        {
            auto tlock = manager.genRegionTaskLock(rid);
            auto & region = manager.region_task_ctrl.regions.at(rid);
            auto lock1 = std::unique_lock(region.mutex, std::try_to_lock);
            ASSERT_FALSE(lock1.owns_lock());
        }
        {
            auto & region = manager.region_task_ctrl.regions.at(rid);
            auto lock1 = std::unique_lock(region.mutex, std::try_to_lock);
            ASSERT_TRUE(lock1.owns_lock());
        }
    }
}

void test_mergeresult()
{
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "x", ""), createRegionInfo(1000, "", "x")).source_at_left, false);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "", "x"), createRegionInfo(1000, "x", "")).source_at_left, true);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "x", "y"), createRegionInfo(1000, "y", "z")).source_at_left, true);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "y", "z"), createRegionInfo(1000, "x", "y")).source_at_left, false);
}

void RegionKVStoreTest::testBasic()
{
    {
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        ASSERT_EQ(root_map.size(), 2);

        region_index.add(makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)));

        ASSERT_EQ(root_map.begin()->second.region_map.size(), 0);

        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 3)));
        region_index.add(makeRegion(3, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)));

        auto res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);

        region_index.add(makeRegion(4, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)));

        ASSERT_EQ(root_map.size(), 7);

        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 4);

        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);

        res = region_index.findByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 5)));
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res.find(1) != res.end());
        ASSERT_TRUE(res.find(2) != res.end());
        ASSERT_TRUE(res.find(4) != res.end());

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)), 4);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)), 3);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 2);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 3)), 2);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 1);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)), 1);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_TRUE(res.empty());

        ASSERT_EQ(root_map.size(), 2);
    }

    {
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), TiKVKey()), 1);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            ASSERT_EQ(res, "void DB::RegionsRangeIndex::remove(const DB::RegionRange &, DB::RegionID): not found region 1");
        }

        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 5)));
        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 4), RecordKVFormat::genKey(1, 5)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            ASSERT_EQ(res, "void DB::RegionsRangeIndex::remove(const DB::RegionRange &, DB::RegionID): not found start key");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 4)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            ASSERT_EQ(res, "void DB::RegionsRangeIndex::remove(const DB::RegionRange &, DB::RegionID): not found end key");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 3)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            ASSERT_EQ(res, "void DB::RegionsRangeIndex::remove(const DB::RegionRange &, DB::RegionID): range of region 2 is empty");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), TiKVKey()), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            ASSERT_EQ(res, "void DB::RegionsRangeIndex::remove(const DB::RegionRange &, DB::RegionID): not found region 2");
        }

        region_index.clear();

        try
        {
            region_index.add(makeRegion(6, RecordKVFormat::genKey(6, 6), RecordKVFormat::genKey(6, 6)));
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::string tar = "Illegal region range, should not happen";
            ASSERT(res.size() > tar.size());
            ASSERT_EQ(res.substr(0, tar.size()), tar);
        }

        region_index.clear();

        region_index.add(makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)));
        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)));
        region_index.add(makeRegion(3, RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)));

        ASSERT_EQ(root_map.size(), 6);
        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)), 3);
        ASSERT_EQ(root_map.size(), 5);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)), 1);
        ASSERT_EQ(root_map.size(), 4);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)), 2);
        ASSERT_EQ(root_map.size(), 2);
    }
    {
        test_mergeresult();
    }
}

TEST_F(RegionKVStoreTest, run)
try
{
    testBasic();
    testRegionManager();
    testKVStore();
    testRegion();
}
CATCH

} // namespace tests
} // namespace DB