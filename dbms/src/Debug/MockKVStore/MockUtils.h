// Copyright 2024 PingCAP, Inc.
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

#include <Parsers/IAST_fwd.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{
class KVStore;
class TMTContext;
} // namespace DB

namespace DB::RegionBench
{
metapb::Peer createPeer(UInt64 id, bool);

metapb::Region createMetaRegion( //
    RegionID region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt,
    std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt);

metapb::Region createMetaRegionCommonHandle( //
    RegionID region_id,
    const std::string & start_key,
    const std::string & end_key,
    std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt,
    std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt);

RegionPtr makeRegionForTable(
    UInt64 region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    const TiFlashRaftProxyHelper * proxy_helper = nullptr);

RegionPtr makeRegionForRange(
    UInt64 id,
    std::string start_key,
    std::string end_key,
    const TiFlashRaftProxyHelper * proxy_helper = nullptr);

RegionPtr makeRegion(RegionMeta && meta);

// Generates a lock value which fills all fields, only for test use.
TiKVValue encodeFullLockCfValue(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value,
    Timestamp min_commit_ts,
    Timestamp for_update_ts,
    uint64_t txn_size,
    const std::vector<std::string> & async_commit,
    const std::vector<uint64_t> & rollback,
    UInt64 generation = 0);

void setupPutRequest(raft_cmdpb::Request * req, const std::string & cf, const TiKVKey & key, const TiKVValue & value);
void setupDelRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &);

void encodeRow(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);

void insert(
    const TiDB::TableInfo & table_info,
    RegionID region_id,
    HandleID handle_id,
    ASTs::const_iterator begin,
    ASTs::const_iterator end,
    Context & context,
    const std::optional<std::tuple<Timestamp, UInt8>> & tso_del = {});

void addRequestsToRaftCmd(
    raft_cmdpb::RaftCmdRequest & request,
    const TiKVKey & key,
    const TiKVValue & value,
    UInt64 prewrite_ts,
    UInt64 commit_ts,
    bool del,
    const String & pk = "pk");

void concurrentBatchInsert(
    const TiDB::TableInfo & table_info,
    Int64 concurrent_num,
    Int64 flush_num,
    Int64 batch_num,
    UInt64 min_strlen,
    UInt64 max_strlen,
    Context & context);

void remove(const TiDB::TableInfo & table_info, RegionID region_id, HandleID handle_id, Context & context);

Int64 concurrentRangeOperate(
    const TiDB::TableInfo & table_info,
    HandleID start_handle,
    HandleID end_handle,
    Context & context,
    Int64 magic_num,
    bool del);

Field convertField(const TiDB::ColumnInfo & column_info, const Field & field);

TableID getTableID(
    Context & context,
    const std::string & database_name,
    const std::string & table_name,
    const std::string & partition_id);

const TiDB::TableInfo & getTableInfo(Context & context, const String & database_name, const String & table_name);

EngineStoreApplyRes applyWriteRaftCmd(
    KVStore & kvstore,
    raft_cmdpb::RaftCmdRequest && request,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt,
    ::DB::DM::WriteResult * write_result_ptr = nullptr);

void handleApplySnapshot(
    KVStore & kvstore,
    metapb::Region && region,
    uint64_t peer_id,
    SSTViewVec,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t>,
    TMTContext & tmt);

} // namespace DB::RegionBench
