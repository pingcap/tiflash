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

#include <Debug/MockKVStore/MockUtils.h>
#include <Parsers/IAST.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <TiDB/Schema/TiDB.h>
#include <kvproto/raft_cmdpb.pb.h>

#include <optional>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{
class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
class KVStore;
class TMTContext;
} // namespace DB

namespace DB::RegionBench
{
RegionPtr createRegion(
    TableID table_id,
    RegionID region_id,
    const HandleID & start,
    const HandleID & end,
    std::optional<uint64_t> index = std::nullopt);

Regions createRegions(
    TableID table_id,
    size_t region_num,
    size_t key_num_each_region,
    HandleID handle_begin,
    RegionID new_region_id_begin);

RegionPtr createRegion(
    const TiDB::TableInfo & table_info,
    RegionID region_id,
    std::vector<Field> & start_keys,
    std::vector<Field> & end_keys);

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
    const String pk = "pk");

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

namespace DB
{
using QualifiedName = std::pair<String, String>;
String mappedDatabase(Context & context, const String & database_name);
std::optional<String> mappedDatabaseWithOptional(Context & context, const String & database_name);
std::optional<QualifiedName> mappedTableWithOptional(
    Context & context,
    const String & database_name,
    const String & table_name);
QualifiedName mappedTable(
    Context & context,
    const String & database_name,
    const String & table_name,
    bool include_tombstone = false);

} // namespace DB
