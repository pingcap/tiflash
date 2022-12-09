#pragma once

#include <Storages/Transaction/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <pingcap/kv/Snapshot.h>
#pragma GCC diagnostic pop

#include <common/logger_useful.h>

namespace DB
{

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

    std::vector<AffectedOption> affected_opts;

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
