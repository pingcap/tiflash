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

#include <Storages/KVStore/TiKVHelpers/KeyspaceSnapshot.h>
#include <TiDB/Schema/TiDB_fwd.h>
#include <common/logger_useful.h>

#include <optional>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace DB
{
// The enum results are completely the same as the DDL Action listed in the "parser/model/ddl.go" of TiDB codebase, which must be keeping in sync.
// https://github.com/pingcap/tidb/blob/9dfbccb01b76e6a5f2fc6f6562b8645dd5a151b1/pkg/parser/model/ddl.go#L29-L30
enum class SchemaActionType : Int8
{
    None = 0,
    CreateSchema = 1,
    DropSchema = 2,
    CreateTable = 3,
    DropTable = 4,
    AddColumn = 5,
    DropColumn = 6,
    AddIndex = 7,
    DropIndex = 8,
    AddForeignKey = 9,
    DropForeignKey = 10,
    TruncateTable = 11,
    ModifyColumn = 12,
    RebaseAutoID = 13,
    RenameTable = 14,
    SetDefaultValue = 15,
    ShardRowID = 16,
    ModifyTableComment = 17,
    RenameIndex = 18,
    AddTablePartition = 19,
    DropTablePartition = 20,
    CreateView = 21,
    ModifyTableCharsetAndCollate = 22,
    TruncateTablePartition = 23,
    DropView = 24,
    RecoverTable = 25,
    ModifySchemaCharsetAndCollate = 26,
    LockTable = 27,
    UnlockTable = 28,
    RepairTable = 29,
    SetTiFlashReplica = 30,
    UpdateTiFlashReplicaStatus = 31,
    AddPrimaryKey = 32,
    DropPrimaryKey = 33,
    CreateSequence = 34,
    AlterSequence = 35,
    DropSequence = 36,
    AddColumns = 37,
    DropColumns = 38,
    ModifyTableAutoIdCache = 39,
    RebaseAutoRandomBase = 40,
    AlterIndexVisibility = 41,
    ExchangeTablePartition = 42,
    AddCheckConstraint = 43,
    DropCheckConstraint = 44,
    AlterCheckConstraint = 45,
    AlterTableAlterPartition = 46,
    RenameTables = 47,
    DropIndexes = 48,
    AlterTableAttributes = 49,
    AlterTablePartitionAttributes = 50,
    CreatePlacementPolicy = 51,
    AlterPlacementPolicy = 52,
    DropPlacementPolicy = 53,
    AlterTablePartitionPlacement = 54,
    ModifySchemaDefaultPlacement = 55,
    AlterTablePlacement = 56,
    AlterCacheTable = 57,
    AlterTableStatsOptions = 58,
    AlterNoCacheTable = 59,
    CreateTables = 60,
    ActionMultiSchemaChange = 61,
    ActionFlashbackCluster = 62,
    ActionRecoverSchema = 63,
    ActionReorganizePartition = 64,
    ActionAlterTTLInfo = 65,
    ActionAlterTTLRemove = 67,
    ActionCreateResourceGroup = 68,
    ActionAlterResourceGroup = 69,
    ActionDropResourceGroup = 70,
    ActionAlterTablePartitioning = 71,
    ActionRemovePartitioning = 72,


    // If we support new type from TiDB.
    // MaxRecognizedType also needs to be changed.
    // It should always be equal to the maximum supported type + 1
    MaxRecognizedType = 73,
};

struct AffectedOption
{
    AffectedOption() = default;
    explicit AffectedOption(Poco::JSON::Object::Ptr json);
    DatabaseID schema_id;
    TableID table_id;
    TableID old_table_id;
    DatabaseID old_schema_id;

    void deserialize(Poco::JSON::Object::Ptr json);
};
struct SchemaDiff
{
    Int64 version;
    SchemaActionType type;
    DatabaseID schema_id;
    TableID table_id;

    TableID old_table_id;
    DatabaseID old_schema_id;
    bool regenerate_schema_map{false};

    std::vector<AffectedOption> affected_opts;

    void deserialize(const String & data);
};

struct SchemaGetter
{
    static constexpr Int64 SchemaVersionNotExist = -1;

    KeyspaceSnapshot snap;

    KeyspaceID keyspace_id;

    LoggerPtr log;

    SchemaGetter(pingcap::kv::Cluster * cluster_, UInt64 tso_, KeyspaceID keyspace_id_)
        : snap(keyspace_id_, cluster_, tso_)
        , keyspace_id(keyspace_id_)
        , log(Logger::get())
    {}

    Int64 getVersion();

    bool checkSchemaDiffExists(Int64 ver);

    std::optional<SchemaDiff> getSchemaDiff(Int64 ver);

    static String getSchemaDiffKey(Int64 ver);

    bool checkDBExists(const String & key);

    static String getDBKey(DatabaseID db_id);

    static String getTableKey(TableID table_id);

    TiDB::DBInfoPtr getDatabase(DatabaseID db_id);

    TiDB::TableInfoPtr getTableInfo(DatabaseID db_id, TableID table_id, bool try_mvcc = true)
    {
        if (try_mvcc)
            return getTableInfoImpl</*mvcc_get*/ true>(db_id, table_id).first;
        return getTableInfoImpl</*mvcc_get*/ false>(db_id, table_id).first;
    }

    std::pair<TiDB::TableInfoPtr, bool> getTableInfoAndCheckMvcc(DatabaseID db_id, TableID table_id)
    {
        return getTableInfoImpl</*mvcc_get*/ true>(db_id, table_id);
    }

    std::vector<TiDB::DBInfoPtr> listDBs();

    std::vector<TiDB::TableInfoPtr> listTables(DatabaseID db_id);

    KeyspaceID getKeyspaceID() const { return keyspace_id; }

private:
    template <bool mvcc_get>
    std::pair<TiDB::TableInfoPtr, bool> getTableInfoImpl(DatabaseID db_id, TableID table_id);
};

} // namespace DB
