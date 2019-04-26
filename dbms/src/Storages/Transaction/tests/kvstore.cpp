#include <ext/scope_guard.h>

#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/applySnapshot.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Raft/RaftContext.h>

#include "region_helper.h"

using namespace DB;

const std::string dir_path = "./kvstore_tmp_/";

raft_serverpb::KeyValue lock_kv(TableID table_id, HandleID handle_id)
{
    raft_serverpb::KeyValue r;
    *(r.mutable_key())   = RecordKVFormat::genKey(table_id, handle_id).getStr();
    *(r.mutable_value()) = RecordKVFormat::encodeLockCfValue('P', "hehe", 0, 0).getStr();
    return r;
}

raft_serverpb::KeyValue default_kv(TableID table_id, HandleID handle_id, Timestamp ts, const String v)
{
    raft_serverpb::KeyValue r;
    *(r.mutable_key())   = RecordKVFormat::genKey(table_id, handle_id, ts).getStr();
    *(r.mutable_value()) = v;
    return r;
}

raft_serverpb::KeyValue write_kv(TableID table_id, HandleID handle_id, Timestamp ts, Timestamp pre_ts)
{
    raft_serverpb::KeyValue r;
    *(r.mutable_key())   = RecordKVFormat::genKey(table_id, handle_id, ts).getStr();
    *(r.mutable_value()) = RecordKVFormat::encodeWriteCfValue('P', pre_ts).getStr();
    return r;
}

int main(int, char **)
{
    bool suc = true;

    SCOPE_EXIT({
        // remove tmp dir
        Poco::File(dir_path).remove(true);
    });

    Poco::File dir(dir_path);
    if (dir.exists())
        dir.remove(true);
    dir.createDirectory();

    TableID table_id  = 100;
    UInt64 region_id = 33;
    UInt64 term      = 5;

    auto kvstore = std::make_shared<KVStore>(dir_path);
    {
        std::vector<enginepb::SnapshotRequest> reqs;
        {
            enginepb::SnapshotRequest r;
            *(r.mutable_state()->mutable_peer())        = createPeer(1, true);
            *(r.mutable_state()->mutable_region())      = createRegionInfo(region_id, //
                                                                      RecordKVFormat::genKey(table_id, 1).getStr(),
                                                                      RecordKVFormat::genKey(table_id, 100).getStr());
            *(r.mutable_state()->mutable_apply_state()) = initialApplyState();

            reqs.push_back(r);
        }

        {
            enginepb::SnapshotRequest r;
            *(r.mutable_data()->mutable_cf()) = "default";
            auto * kvs                        = r.mutable_data()->mutable_data();
            *(kvs->Add())                     = default_kv(table_id, 1, 1, "v1");
            *(kvs->Add())                     = default_kv(table_id, 2, 2, "v3");
            *(kvs->Add())                     = default_kv(table_id, 3, 3, "v3");

            reqs.push_back(r);
        }

        {
            enginepb::SnapshotRequest r;
            *(r.mutable_data()->mutable_cf()) = "lock";
            auto * kvs                        = r.mutable_data()->mutable_data();
            *(kvs->Add())                     = lock_kv(table_id, 111);
            *(kvs->Add())                     = lock_kv(table_id, 222);
            *(kvs->Add())                     = lock_kv(table_id, 333);

            reqs.push_back(r);
        }

        {
            enginepb::SnapshotRequest r;
            *(r.mutable_data()->mutable_cf()) = "write";
            auto * kvs                        = r.mutable_data()->mutable_data();
            *(kvs->Add())                     = write_kv(table_id, 1, 1, 1);
            *(kvs->Add())                     = write_kv(table_id, 2, 2, 2);
            *(kvs->Add())                     = write_kv(table_id, 3, 3, 3);

            reqs.push_back(r);
        }

        size_t index         = 0;
        auto   read_snapshot = [&](enginepb::SnapshotRequest * req) {
            if (index < reqs.size())
            {
                *req = reqs.at(index++);
                return true;
            }
            else
                return false;
        };

        applySnapshot(kvstore, read_snapshot);
    }

    {
        ASSERT_CHECK((bool)kvstore->getRegion(region_id), suc);
    }

    RaftContext context;
    UInt64      index = 6;

    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);

        auto & req  = *(cmd.mutable_admin_request());
        auto & resp = *(cmd.mutable_admin_response());

        req.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);
        resp.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);

        auto & change_peer = *(req.mutable_change_peer());
        change_peer.set_change_type(eraftpb::ConfChangeType::AddLearnerNode);
        *(change_peer.mutable_peer()) = createPeer(1, true);

        *(resp.mutable_change_peer()->mutable_region()) = createRegionInfo(region_id, //
                                                                           RecordKVFormat::genKey(table_id, 1).getStr(),
                                                                           RecordKVFormat::genKey(table_id, 100).getStr());

        kvstore->onServiceCommand(cmds, context);
    }

    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);

        auto & req  = *(cmd.mutable_admin_request());
        auto & resp = *(cmd.mutable_admin_response());

        req.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);
        resp.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);

        {
            auto & splits = *(req.mutable_splits());
            splits.set_right_derive(false);
            auto & split_reqs = *(splits.mutable_requests());

            auto & sr = *(split_reqs.Add());
            sr.set_split_key(RecordKVFormat::genKey(table_id, 3).getStr());
            sr.set_new_region_id(330);
            sr.add_new_peer_ids(1);
        }
        {
            auto & splits  = *(resp.mutable_splits());
            auto   region1 = createRegionInfo(region_id, //
                                            RecordKVFormat::genKey(table_id, 1).getStr(),
                                            RecordKVFormat::genKey(table_id, 3).getStr());
            auto   region2 = createRegionInfo(330, //
                                            RecordKVFormat::genKey(table_id, 3).getStr(),
                                            RecordKVFormat::genKey(table_id, 100).getStr());

            *(splits.mutable_regions()->Add()) = region1;
            *(splits.mutable_regions()->Add()) = region2;
        }

        kvstore->onServiceCommand(cmds, context);
    }

    {// snapshot
        {
            enginepb::SnapshotRequest request;
            enginepb::SnapshotState * state = request.mutable_state();
            state->mutable_region()->set_id(666);

            TiKVKey start_key = RecordKVFormat::genKey(table_id, 200);
            TiKVKey end_key = RecordKVFormat::genKey(table_id, 300);

            state->mutable_region()->set_start_key(start_key.getStr());
            state->mutable_region()->set_end_key(end_key.getStr());

            RegionMeta region_meta(state->peer(), state->region(), initialApplyState());
            region_meta.setApplied(index + 10, term);
            RegionPtr region = std::make_shared<Region>(std::move(region_meta));

            kvstore->onSnapshot(region, nullptr);
        }

        {
            enginepb::CommandRequestBatch cmds;
            enginepb::CommandRequest & cmd = *(cmds.mutable_requests()->Add());

            cmd.mutable_header()->set_region_id(region_id);
            cmd.mutable_header()->set_term(term);
            cmd.mutable_header()->set_index(index++);
            cmd.mutable_header()->set_sync_log(true);

            auto & req = *(cmd.mutable_admin_request());
            auto & resp = *(cmd.mutable_admin_response());

            req.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);
            resp.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);

            {
                auto & splits = *(req.mutable_splits());
                splits.set_right_derive(false);
                auto & split_reqs = *(splits.mutable_requests());

                auto & sr = *(split_reqs.Add());
                sr.set_split_key(RecordKVFormat::genKey(table_id, 200).getStr());
                sr.set_new_region_id(666);
                sr.add_new_peer_ids(111);
            }
            {
                auto & splits = *(resp.mutable_splits());
                auto region1 = createRegionInfo(region_id, //
                                                RecordKVFormat::genKey(table_id, 1).getStr(),
                                                RecordKVFormat::genKey(table_id, 3).getStr());
                auto region2 = createRegionInfo(666, //
                                                RecordKVFormat::genKey(table_id, 200).getStr(),
                                                RecordKVFormat::genKey(table_id, 300).getStr());

                *(splits.mutable_regions()->Add()) = region1;
                *(splits.mutable_regions()->Add()) = region2;
            }

            kvstore->onServiceCommand(cmds, context);
        }
    }

    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);
        auto & requests = *cmd.mutable_requests();

        {
            auto & req = *(requests.Add());
            req.set_cmd_type(raft_cmdpb::CmdType::Put);
            req.mutable_put()->set_cf("default");
            req.mutable_put()->set_key(RecordKVFormat::genKey(table_id, 1, 4).getStr());
            req.mutable_put()->set_value("");
        }
        {
            auto & req = *(requests.Add());
            req.set_cmd_type(raft_cmdpb::CmdType::Put);
            req.mutable_put()->set_cf("lock");
            req.mutable_put()->set_key(RecordKVFormat::genKey(table_id, 1, 4).getStr());
            req.mutable_put()->set_value(RecordKVFormat::encodeLockCfValue(Region::PutFlag, "primary key", 4, 0,
                "value").getStr());
        }
        {
            auto & req = *(requests.Add());
            req.set_cmd_type(raft_cmdpb::CmdType::Put);
            req.mutable_put()->set_cf("write");
            req.mutable_put()->set_key(RecordKVFormat::genKey(table_id, 1, 5).getStr());
            req.mutable_put()->set_value(RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 4, "value").getStr());
        }
        {
            auto & req = *(requests.Add());
            req.set_cmd_type(raft_cmdpb::CmdType::Delete);
            req.mutable_delete_()->set_cf("lock");
            req.mutable_delete_()->set_key(RecordKVFormat::genKey(table_id, 1, 4).getStr());
        }

        kvstore->onServiceCommand(cmds, context);
    }

    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);

        cmd.mutable_header()->set_context("context");

        auto & req  = *(cmd.mutable_admin_request());
        auto & resp = *(cmd.mutable_admin_response());

        req.set_cmd_type(raft_cmdpb::AdminCmdType::ComputeHash);
        resp.set_cmd_type(raft_cmdpb::AdminCmdType::ComputeHash);

        kvstore->onServiceCommand(cmds, context);
    }

    if (false) // It cause panic!
    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);

        auto & req  = *(cmd.mutable_admin_request());
        auto & resp = *(cmd.mutable_admin_response());

        req.set_cmd_type(raft_cmdpb::AdminCmdType::VerifyHash);
        resp.set_cmd_type(raft_cmdpb::AdminCmdType::VerifyHash);

        auto & verify_req = *(req.mutable_verify_hash());
        verify_req.set_index(index - 2);
        verify_req.set_hash("fake hash");

        kvstore->onServiceCommand(cmds, context);
    }

    {
        kvstore->tryPersistAndReport(context, Seconds(0), Seconds(0));

        auto kvstore2 = std::make_shared<KVStore>(dir_path);

        kvstore2->restore([&](pingcap::kv::RegionVerID) -> pingcap::kv::RegionClientPtr {
            return nullptr;
        }, nullptr);

        kvstore->traverseRegions([&](const RegionID region_id, const RegionPtr & region1){
            auto region2 = kvstore2->getRegion(region_id);
            ASSERT_CHECK(region2 != nullptr, suc);
            ASSERT_CHECK_EQUAL(*region2, *region1, suc);
        });

        kvstore2->traverseRegions([&](const RegionID region_id, const RegionPtr & region2){
            auto region1 = kvstore->getRegion(region_id);
            ASSERT_CHECK(region1 != nullptr, suc);
            ASSERT_CHECK_EQUAL(*region2, *region1, suc);
        });
    }

    {
        enginepb::CommandRequestBatch cmds;
        enginepb::CommandRequest &    cmd = *(cmds.mutable_requests()->Add());

        cmd.mutable_header()->set_region_id(region_id);
        cmd.mutable_header()->set_term(term);
        cmd.mutable_header()->set_index(index++);
        cmd.mutable_header()->set_sync_log(true);

        auto & req  = *(cmd.mutable_admin_request());
        auto & resp = *(cmd.mutable_admin_response());

        req.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);
        resp.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);

        resp.mutable_change_peer()->mutable_region()->set_id(region_id);

        auto & change_peer = *(req.mutable_change_peer());
        change_peer.set_change_type(eraftpb::ConfChangeType::RemoveNode);
        *(change_peer.mutable_peer()) = createPeer(1, true);

        kvstore->onServiceCommand(cmds, context);

        ASSERT_CHECK_EQUAL(2, kvstore->regionSize(), suc);

        kvstore->tryPersistAndReport(context, Seconds(0), Seconds(0));
    }

    return suc ? 0 : 1;
}
