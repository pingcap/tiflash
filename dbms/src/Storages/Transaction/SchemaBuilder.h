#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaGetter.h>

namespace DB
{

struct SchemaBuilder
{

    SchemaGetter & getter;

    Context & context;

    std::unordered_map<DB::DatabaseID, String> & databases;

    Int64 target_version;

    Logger * log;

    SchemaBuilder(SchemaGetter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, String> & dbs_, Int64 version)
        : getter(getter_), context(context_), databases(dbs_), target_version(version), log(&Logger::get("SchemaBuilder"))
    {}

    void applyDiff(const SchemaDiff & diff);

    void applyDropSchema(DatabaseID schema_id);

    void syncAllSchema();

    void applyRenameTableImpl(const String & old_db, const String & new_db, const String & old_table, const String & new_table);

private:
    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchemaImpl(TiDB::DBInfoPtr db_info);

    void applyCreateTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyDropTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyAlterTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyAlterTableImpl(TiDB::TableInfoPtr table_info, const String & db_name, StorageMergeTree * storage);

    //void applyAddPartition(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    //void applyDropPartition(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyCreatePhysicalTableImpl(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info);

    void applyCreateTableImpl(const TiDB::DBInfo & db_info, TiDB::TableInfo & table_info);

    void applyDropTableImpl(const String &, const String &);

    void applyRenameTable(TiDB::DBInfoPtr db_info, TiDB::DatabaseID old_db_id, TiDB::TableID table_id);


    void createTables(std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> table_dbs);

    void alterAndRenameTables(std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> table_dbs);

    void dropInvalidTables(std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> table_dbs);
};

} // namespace DB
