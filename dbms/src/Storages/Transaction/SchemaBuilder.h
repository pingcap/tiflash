#pragma once

#include <Storages/Transaction/SchemaGetter.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/Context.h>

namespace DB {

struct SchemaBuilder {

    SchemaGetter & getter;
    Context & context;
    std::unordered_map<DB::DatabaseID, String> & databases;

    SchemaBuilder(SchemaGetter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, String> dbs_): getter(getter_), context(context_), databases(dbs_){}

    void applyDiff(const SchemaDiff & diff);

    void updateDB(TiDB::DBInfoPtr db_info);

    void applyDropSchema(DatabaseID schema_id);

private:

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchemaImpl(TiDB::DBInfoPtr db_info);


    void applyCreateTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyDropTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyAlterTable(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyAlterTableImpl(TiDB::TableInfoPtr table_info, StorageMergeTree* storage);

    //void applyAddPartition(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    //void applyDropPartition(TiDB::DBInfoPtr dbInfo, Int64 table_id);

    void applyCreatePhysicalTableImpl(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info);

    void applyCreateTableImpl(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info);

    void applyDropTableImpl(const String &, const String &);
};
    bool applyCreateSchemaImpl(TiDB::DBInfoPtr db_info);

}
