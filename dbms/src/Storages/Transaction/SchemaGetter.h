#pragma once

#include<Storages/Transaction/TiDB.h>
#include<tikv/Snapshot.h>

namespace DB {

enum SchemaActionType : Int8 {
    SchemaActionNone                          = 0,
    SchemaActionCreateSchema                  = 1,
    SchemaActionDropSchema                    = 2,
    SchemaActionCreateTable                   = 3,
    SchemaActionDropTable                     = 4,
    SchemaActionAddColumn                     = 5,
    SchemaActionDropColumn                    = 6,
    SchemaActionAddIndex                      = 7,
    SchemaActionDropIndex                     = 8,
    SchemaActionAddForeignKey                 = 9,
    SchemaActionDropForeignKey                = 10,
    SchemaActionTruncateTable                 = 11,
    SchemaActionModifyColumn                  = 12,
    SchemaActionRebaseAutoID                  = 13,
    SchemaActionRenameTable                   = 14,
    SchemaActionSetDefaultValue               = 15,
    SchemaActionShardRowID                    = 16,
    SchemaActionModifyTableComment            = 17,
    SchemaActionRenameIndex                   = 18,
    SchemaActionAddTablePartition             = 19,
    SchemaActionDropTablePartition            = 20,
    SchemaActionCreateView                    = 21,
    SchemaActionModifyTableCharsetAndCollate  = 22,
    SchemaActionTruncateTablePartition        = 23,
    SchemaActionDropView                      = 24,
    SchemaActionRecoverTable                  = 25,
    SchemaActionModifySchemaCharsetAndCollate = 26
};

struct SchemaDiff {
    Int64 version;
    SchemaActionType type;
    Int64 schema_id;
    Int64 table_id;

    Int64 old_table_id;
    Int64 old_schema_id;

    void deserialize(const String & data);
};

struct SchemaGetter {
    pingcap::kv::Snapshot snap;


    SchemaGetter(pingcap::kv::RegionCachePtr regionCachePtr_, pingcap::kv::RpcClientPtr rpcClientPtr_, UInt64 tso_) : snap(regionCachePtr_, rpcClientPtr_, tso_) {}

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

}
