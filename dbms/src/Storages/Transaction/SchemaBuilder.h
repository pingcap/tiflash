#pragma once

#include <Interpreters/Context.h>
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

    void updateDB(TiDB::DBInfoPtr db_info);

    void applyDropSchema(DatabaseID schema_id);

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
};

} // namespace DB
