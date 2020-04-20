#pragma once

#include <Storages/Transaction/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <pingcap/kv/Snapshot.h>
#pragma GCC diagnostic pop

#include <common/logger_useful.h>

namespace DB
{

enum SchemaActionType : Int8
{
    SchemaActionNone = 0,
    SchemaActionCreateSchema = 1,
    SchemaActionDropSchema = 2,
    SchemaActionCreateTable = 3,
    SchemaActionDropTable = 4,
    SchemaActionAddColumn = 5,
    SchemaActionDropColumn = 6,
    SchemaActionAddIndex = 7,
    SchemaActionDropIndex = 8,
    SchemaActionAddForeignKey = 9,
    SchemaActionDropForeignKey = 10,
    SchemaActionTruncateTable = 11,
    SchemaActionModifyColumn = 12,
    SchemaActionRebaseAutoID = 13,
    SchemaActionRenameTable = 14,
    SchemaActionSetDefaultValue = 15,
    SchemaActionShardRowID = 16,
    SchemaActionModifyTableComment = 17,
    SchemaActionRenameIndex = 18,
    SchemaActionAddTablePartition = 19,
    SchemaActionDropTablePartition = 20,
    SchemaActionCreateView = 21,
    SchemaActionModifyTableCharsetAndCollate = 22,
    SchemaActionTruncateTablePartition = 23,
    SchemaActionDropView = 24,
    SchemaActionRecoverTable = 25,
    SchemaActionModifySchemaCharsetAndCollate = 26,
    SchemaActionLockTable = 27,
    SchemaActionUnlockTable = 28,
    SchemaActionRepairTable = 29,
    SchemaActionSetTiFlashReplica = 30,
    SchemaActionUpdateTiFlashReplicaStatus = 31,
    SchemaActionAddPrimaryKey = 32,
    SchemaActionDropPrimaryKey = 33,
    SchemaActionCreateSequence = 34,
    SchemaActionAlterSequence = 35,
    SchemaActionDropSequence = 36
};

struct SchemaDiff
{
    Int64 version;
    SchemaActionType type;
    DatabaseID schema_id;
    TableID table_id;

    DatabaseID old_table_id;
    TableID old_schema_id;

    void deserialize(const String & data);
};

struct SchemaGetter
{
    pingcap::kv::Snapshot snap;

    Logger * log;

    SchemaGetter(pingcap::kv::Cluster * cluster_, UInt64 tso_) : snap(cluster_, tso_), log(&Logger::get("SchemaGetter")) {}

    Int64 getVersion();

    SchemaDiff getSchemaDiff(Int64 ver);

    String getSchemaDiffKey(Int64 ver);

    bool checkDBExists(const String & key);

    String getDBKey(DatabaseID db_id);

    String getTableKey(TableID table_id);

    TiDB::DBInfoPtr getDatabase(DatabaseID db_id);

    TiDB::TableInfoPtr getTableInfo(DatabaseID db_id, TableID table_id);

    std::vector<TiDB::DBInfoPtr> listDBs();

    std::vector<TiDB::TableInfoPtr> listTables(DatabaseID db_id);
};

} // namespace DB
