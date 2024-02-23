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

#include <Common/typeid_cast.h>
#include <Debug/DBGInvoker.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/MockTiKV.h>
#include <Debug/dbgKVStore/dbgFuncMockRaftCommand.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

void MockRaftCommand::dbgFuncRegionBatchSplit(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
    auto table = MockTiDB::instance().getTableByName(database_name, table_name);
    auto table_info = table->table_info;
    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    if (4 + handle_column_size * 4 != args.size())
        throw Exception(
            "Args not matched, should be: region-id1, database-name, table-name, start1, end1, start2, end2, "
            "region-id2",
            ErrorCodes::BAD_ARGUMENTS);
    auto region_id2
        = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[args.size() - 1]).value));

    auto table_id = table->id();
    TiKVKey start_key1, start_key2, end_key1, end_key2;
    if (table_info.is_common_handle)
    {
        std::vector<Field> start_keys1;
        std::vector<Field> start_keys2;
        std::vector<Field> end_keys1;
        std::vector<Field> end_keys2;

        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];

            auto start_field1
                = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[3 + i]).value);
            TiDB::DatumBumpy start_datum1 = TiDB::DatumBumpy(start_field1, column_info.tp);
            start_keys1.emplace_back(start_datum1.field());
            auto end_field1 = RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[3 + handle_column_size + i]).value);
            TiDB::DatumBumpy end_datum1 = TiDB::DatumBumpy(end_field1, column_info.tp);
            end_keys1.emplace_back(end_datum1.field());

            auto start_field2 = RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[3 + handle_column_size * 2 + i]).value);
            TiDB::DatumBumpy start_datum2 = TiDB::DatumBumpy(start_field2, column_info.tp);
            start_keys2.emplace_back(start_datum2.field());
            auto end_field2 = RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[3 + handle_column_size * 3 + i]).value);
            TiDB::DatumBumpy end_datum2 = TiDB::DatumBumpy(end_field2, column_info.tp);
            end_keys2.emplace_back(end_datum2.field());
        }

        start_key1 = RecordKVFormat::genKey(table_info, start_keys1);
        start_key2 = RecordKVFormat::genKey(table_info, start_keys2);
        end_key1 = RecordKVFormat::genKey(table_info, end_keys1);
        end_key2 = RecordKVFormat::genKey(table_info, end_keys2);
    }
    else
    {
        auto start1 = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value));
        auto end1 = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value));
        auto start2 = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value));
        auto end2 = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[6]).value));
        start_key1 = RecordKVFormat::genKey(table_id, start1);
        start_key2 = RecordKVFormat::genKey(table_id, start2);
        end_key1 = RecordKVFormat::genKey(table_id, end1);
        end_key2 = RecordKVFormat::genKey(table_id, end2);
    }


    auto source_region = kvstore->getRegion(region_id);

    metapb::RegionEpoch new_epoch;
    new_epoch.set_version(source_region->version() + 1);
    new_epoch.set_conf_ver(source_region->confVer());
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
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

    kvstore->handleAdminRaftCmd(
        std::move(request),
        std::move(response),
        region_id,
        MockTiKV::instance().getRaftIndex(region_id),
        MockTiKV::instance().getRaftTerm(region_id),
        tmt);

    output(fmt::format("execute batch split, region {} into ({},{})", region_id, region_id, region_id2));
}

void MockRaftCommand::dbgFuncPrepareMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
    {
        throw Exception("Args not matched, should be: source-id1, target-id2", ErrorCodes::BAD_ARGUMENTS);
    }

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));
    auto target_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value));

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);
    auto target_region = kvstore->getRegion(target_id);
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;

    {
        request.set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);

        auto * prepare_merge = request.mutable_prepare_merge();
        {
            auto min_index = region->appliedIndex();
            prepare_merge->set_min_index(min_index);

            metapb::Region * target = prepare_merge->mutable_target();
            *target = target_region->cloneMetaRegion();
        }
    }

    kvstore->handleAdminRaftCmd(
        std::move(request),
        std::move(response),
        region_id,
        MockTiKV::instance().getRaftIndex(region_id),
        MockTiKV::instance().getRaftTerm(region_id),
        tmt);

    output(fmt::format("execute prepare merge, source {} target {}", region_id, target_id));
}

void MockRaftCommand::dbgFuncCommitMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
    {
        throw Exception("Args not matched, should be: source-id1, current-id2", ErrorCodes::BAD_ARGUMENTS);
    }

    auto source_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));
    auto current_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value));

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto source_region = kvstore->getRegion(source_id);
    auto current_region = kvstore->getRegion(current_id);
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;

    {
        request.set_cmd_type(raft_cmdpb::AdminCmdType::CommitMerge);
        auto * commit_merge = request.mutable_commit_merge();
        {
            commit_merge->set_commit(source_region->appliedIndex());
            *commit_merge->mutable_source() = source_region->cloneMetaRegion();
        }
    }

    kvstore->handleAdminRaftCmd(
        std::move(request),
        std::move(response),
        current_id,
        MockTiKV::instance().getRaftIndex(current_id),
        MockTiKV::instance().getRaftTerm(current_id),
        tmt);

    output(fmt::format("execute commit merge, source {} current {}", source_id, current_id));
}

void MockRaftCommand::dbgFuncRollbackMerge(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;

    {
        request.set_cmd_type(raft_cmdpb::AdminCmdType::RollbackMerge);

        auto * rollback_merge = request.mutable_rollback_merge();
        {
            auto merge_state = region->cloneMergeState();
            rollback_merge->set_commit(merge_state.commit());
        }
    }

    kvstore->handleAdminRaftCmd(
        std::move(request),
        std::move(response),
        region_id,
        MockTiKV::instance().getRaftIndex(region_id),
        MockTiKV::instance().getRaftTerm(region_id),
        tmt);

    output(fmt::format("execute rollback merge, region {}", region_id));
}

} // namespace DB
