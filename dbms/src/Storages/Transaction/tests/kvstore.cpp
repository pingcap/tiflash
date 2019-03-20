#include <ext/scope_guard.h>

#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/applySnapshot.h>

#include "region_helper.h"

using namespace DB;

const std::string dir_path = "./kvstore_tmp_/";

raft_serverpb::KeyValue kv(TableID table_id, HandleID handle_id, Timestamp ts, const std::string v)
{
    raft_serverpb::KeyValue r;
    *(r.mutable_key())   = RecordKVFormat::genKey(table_id, handle_id, ts).getStr();
    *(r.mutable_value()) = v;
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
            *(kvs->Add())                     = kv(table_id, 1, 1, "v1");
            *(kvs->Add())                     = kv(table_id, 2, 2, "v3");
            *(kvs->Add())                     = kv(table_id, 3, 3, "v3");

            reqs.push_back(r);
        }

        {
            enginepb::SnapshotRequest r;
            *(r.mutable_data()->mutable_cf()) = "lock";
            auto * kvs                        = r.mutable_data()->mutable_data();
            *(kvs->Add())                     = kv(table_id, 1, 1, "v1");
            *(kvs->Add())                     = kv(table_id, 2, 2, "v3");
            *(kvs->Add())                     = kv(table_id, 3, 3, "v3");

            reqs.push_back(r);
        }

        {
            enginepb::SnapshotRequest r;
            *(r.mutable_data()->mutable_cf()) = "write";
            auto * kvs                        = r.mutable_data()->mutable_data();
            *(kvs->Add())                     = kv(table_id, 1, 1, "v1");
            *(kvs->Add())                     = kv(table_id, 2, 2, "v3");
            *(kvs->Add())                     = kv(table_id, 3, 3, "v3");

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

        {
            auto         kvstore2 = std::make_shared<KVStore>(dir_path);
            const auto & regions1 = kvstore->getRegions();
            const auto & regions2 = kvstore2->getRegions();
            for (auto && [region_id1, region1] : regions1)
            {
                auto it = regions2.find(region_id1);
                ASSERT_CHECK(it != regions2.end(), suc);
                ASSERT_CHECK_EQUAL(*(it->second), *region1, suc);
            }
            for (auto && [region_id2, region2] : regions2)
            {
                auto it = regions1.find(region_id2);
                ASSERT_CHECK(it != regions1.end(), suc);
                ASSERT_CHECK_EQUAL(*(it->second), *region2, suc);
            }
        }
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

        auto & change_peer = *(req.mutable_change_peer());
        change_peer.set_change_type(eraftpb::ConfChangeType::RemoveNode);
        *(change_peer.mutable_peer()) = createPeer(1, true);

        kvstore->onServiceCommand(cmds, context);

        ASSERT_CHECK_EQUAL(1, kvstore->getRegions().size(), suc);

        kvstore->tryPersistAndReport(context, Seconds(0), Seconds(0));
    }

    return suc ? 0 : 1;
}
