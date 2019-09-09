#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaGetter.h>

namespace DB
{

template <typename Getter>
struct SchemaBuilder
{

    Getter & getter;

    Context & context;

    std::unordered_map<DB::DatabaseID, String> & databases;

    Int64 target_version;

    Logger * log;

    SchemaBuilder(Getter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, String> & dbs_, Int64 version)
        : getter(getter_), context(context_), databases(dbs_), target_version(version), log(&Logger::get("SchemaBuilder"))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

    void applyRenameTableImpl(const String & old_db, const String & new_db, const String & old_table, const String & new_table);

private:
    void applyDropSchema(DatabaseID schema_id);

    void applyDropSchemaImpl(const String & db_name);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchemaImpl(TiDB::DBInfoPtr db_info);

    void applyCreateTable(TiDB::DBInfoPtr db_info, Int64 table_id);

    void applyDropTable(TiDB::DBInfoPtr db_info, Int64 table_id);

    void applyAlterTable(TiDB::DBInfoPtr db_info, Int64 table_id);

    void applyAlterTableImpl(TiDB::TableInfoPtr table_info, const String & db_name, StorageMergeTree * storage);

    void applyAlterPartition(TiDB::DBInfoPtr db_info, Int64 table_id);

    void applyCreatePhysicalTableImpl(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info);

    void applyCreateTableImpl(const TiDB::DBInfo & db_info, TiDB::TableInfo & table_info);

    void applyDropTableImpl(const String &, const String &);

    void applyRenameTable(TiDB::DBInfoPtr db_info, TiDB::DatabaseID old_db_id, TiDB::TableID table_id);


    void createTables(std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> table_dbs);

    void alterAndRenameTables(std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> table_dbs);

    void dropInvalidTablesAndDBs(const std::vector<std::pair<TiDB::TableInfoPtr, TiDB::DBInfoPtr>> & table_dbs, const std::set<String> &);

    bool isIgnoreDB(const String & name);
};

} // namespace DB
