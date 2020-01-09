#include <Common/typeid_cast.h>
#include <Debug/DBGInvoker.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncMockRaftCommand.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Raft/RaftContext.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

void MockRaftCommand::dbgFuncRegionBatchSplit(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 8)
    {
        throw Exception("Args not matched, should be: region-id1, database-name, table-name, start1, end1, start2, end2, region-id2",
            ErrorCodes::BAD_ARGUMENTS);
    }
    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
    HandleID start1 = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    HandleID end1 = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
    HandleID start2 = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value);
    HandleID end2 = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[6]).value);
    RegionID region_id2 = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[7]).value);

    auto table = MockTiDB::instance().getTableByName(database_name, table_name);
    auto table_id = table->id();

    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;

    auto source_region = kvstore->getRegion(region_id);

    metapb::RegionEpoch new_epoch;
    new_epoch.set_version(source_region->version() + 1);
    new_epoch.set_conf_ver(source_region->confVer());
    {
        enginepb::CommandRequest * cmd = cmds.add_requests();
        enginepb::CommandRequestHeader * header = cmd->mutable_header();
        header->set_region_id(region_id);
        header->set_term(MockTiKV::instance().getRaftTerm(region_id));
        header->set_index(MockTiKV::instance().getRaftIndex(region_id));
        header->set_sync_log(false);

        cmd->mutable_admin_request()->set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);
        raft_cmdpb::AdminResponse * response = cmd->mutable_admin_response();
        raft_cmdpb::BatchSplitResponse * splits = response->mutable_splits();
        {
            auto region = splits->add_regions();
            region->set_id(region_id);
            TiKVKey start_key = RecordKVFormat::genKey(table_id, start1);
            TiKVKey end_key = RecordKVFormat::genKey(table_id, end1);
            region->set_start_key(start_key);
            region->set_end_key(end_key);
            region->add_peers();
            *region->mutable_region_epoch() = new_epoch;
        }
        {
            auto region = splits->add_regions();
            region->set_id(region_id2);
            TiKVKey start_key = RecordKVFormat::genKey(table_id, start2);
            TiKVKey end_key = RecordKVFormat::genKey(table_id, end2);
            region->set_start_key(start_key);
            region->set_end_key(end_key);
            region->add_peers();
            *region->mutable_region_epoch() = new_epoch;
        }
    }
    kvstore->onServiceCommand(std::move(cmds), raft_ctx);

    std::stringstream ss;
    ss << "execute batch split, region " << region_id << " into (" << region_id << "," << region_id2 << ")";
    output(ss.str());
}

void MockRaftCommand::dbgFuncPrepareMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
    {
        throw Exception("Args not matched, should be: source-id1, target-id2", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID target_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);

    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);
    auto target_region = kvstore->getRegion(target_id);

    {
        enginepb::CommandRequest * cmd = cmds.add_requests();
        enginepb::CommandRequestHeader * header = cmd->mutable_header();
        header->set_region_id(region_id);
        header->set_term(MockTiKV::instance().getRaftTerm(region_id));
        header->set_index(MockTiKV::instance().getRaftIndex(region_id));
        header->set_sync_log(false);

        raft_cmdpb::AdminRequest * request = cmd->mutable_admin_request();
        request->set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);

        auto prepare_merge = request->mutable_prepare_merge();
        {
            auto min_index = region->appliedIndex();
            prepare_merge->set_min_index(min_index);

            metapb::Region * target = prepare_merge->mutable_target();
            *target = target_region->getMetaRegion();
        }
    }

    kvstore->onServiceCommand(std::move(cmds), raft_ctx);

    std::stringstream ss;
    ss << "execute prepare merge, source " << region_id << " target " << target_id;
    output(ss.str());
}

void MockRaftCommand::dbgFuncCommitMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
    {
        throw Exception("Args not matched, should be: source-id1, current-id2", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID source_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID current_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);

    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto source_region = kvstore->getRegion(source_id);
    auto current_region = kvstore->getRegion(current_id);

    {
        enginepb::CommandRequest * cmd = cmds.add_requests();
        enginepb::CommandRequestHeader * header = cmd->mutable_header();
        header->set_region_id(current_id);
        header->set_term(MockTiKV::instance().getRaftTerm(current_id));
        header->set_index(MockTiKV::instance().getRaftIndex(current_id));
        header->set_sync_log(false);

        raft_cmdpb::AdminRequest * request = cmd->mutable_admin_request();
        request->set_cmd_type(raft_cmdpb::AdminCmdType::CommitMerge);

        auto commit_merge = request->mutable_commit_merge();
        {
            commit_merge->set_commit(source_region->appliedIndex());
            *commit_merge->mutable_source() = source_region->getMetaRegion();
        }
    }

    kvstore->onServiceCommand(std::move(cmds), raft_ctx);

    std::stringstream ss;
    ss << "execute commit merge, source " << source_id << " current " << current_id;
    output(ss.str());
}

void MockRaftCommand::dbgFuncRollbackMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RaftContext raft_ctx(&context, nullptr, nullptr);
    enginepb::CommandRequestBatch cmds;

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);

    {
        enginepb::CommandRequest * cmd = cmds.add_requests();
        enginepb::CommandRequestHeader * header = cmd->mutable_header();
        header->set_region_id(region_id);
        header->set_term(MockTiKV::instance().getRaftTerm(region_id));
        header->set_index(MockTiKV::instance().getRaftIndex(region_id));
        header->set_sync_log(false);

        raft_cmdpb::AdminRequest * request = cmd->mutable_admin_request();
        request->set_cmd_type(raft_cmdpb::AdminCmdType::RollbackMerge);

        auto rollback_merge = request->mutable_rollback_merge();
        {
            auto merge_state = region->getMergeState();
            rollback_merge->set_commit(merge_state.commit());
        }
    }

    kvstore->onServiceCommand(std::move(cmds), raft_ctx);

    std::stringstream ss;
    ss << "execute rollback merge, region " << region_id;
    output(ss.str());
}

} // namespace DB
