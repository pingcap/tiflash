#pragma once

#include <Interpreters/Context.h>
#include <Storages/Transaction/SchemaGetter.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & databases;

    Int64 target_version;

    Logger * log;

    SchemaBuilder(Getter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & dbs_, Int64 version)
        : getter(getter_), context(context_), databases(dbs_), target_version(version), log(&Logger::get("SchemaBuilder"))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

private:
    void applyDropSchema(DatabaseID schema_id);

    /// Parameter schema_name should be mapped.
    void applyDropSchemaByName(const String & schema_name);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchemaByDBInfo(TiDB::DBInfoPtr db_info);

    void applyCreateTable(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyCreateTableByTableInfo(TiDB::DBInfoPtr db_info, TiDB::TableInfo & table_info);

    void applyCreateTableOrPartition(TiDB::DBInfoPtr db_info, TiDB::TableInfo & table_info);

    void applyDropTable(TiDB::DBInfoPtr db_info, TableID table_id);

    /// Parameter schema_name should be mapped.
    void applyDropTableOrPartition(const String & db_name, TableID table_id);

    void applyAlterTable(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyAlterTableOrPartition(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info, ManageableStoragePtr storage);

    void applyPartitionDiff(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyRenameTable(TiDB::DBInfoPtr new_db_info, TiDB::TableID table_id);

    void applyRenameTableOrPartition(TiDB::DBInfoPtr new_db_info, const TiDB::TableInfo & new_table_info, ManageableStoragePtr storage);
};

} // namespace DB
