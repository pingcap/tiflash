#include <IO/WriteHelpers.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

using namespace TiDB;

inline void setAlterCommandColumn(Logger * log, AlterCommand & command, const ColumnInfo & column_info)
{
    command.column_name = column_info.name;
    command.data_type = getDataTypeByColumnInfo(column_info);
    if (!column_info.origin_default_value.isEmpty())
    {
        LOG_DEBUG(log, "add default value for column: " + column_info.name);
        command.default_expression = ASTPtr(new ASTLiteral(column_info.defaultValueToField()));
    }
}

inline AlterCommands detectSchemaChanges(Logger * log, const TableInfo & table_info, const TableInfo & orig_table_info)
{
    AlterCommands alter_commands;

    /// Detect new columns.
    // TODO: Detect rename or type-changed columns.
    for (const auto & column_info : table_info.columns)
    {
        const auto & orig_column_info = std::find_if(orig_table_info.columns.begin(),
            orig_table_info.columns.end(),
            [&](const TiDB::ColumnInfo & orig_column_info_) { return orig_column_info_.id == column_info.id; });

        AlterCommand command;
        if (orig_column_info == orig_table_info.columns.end())
        {
            // New column.
            // TODO: support after column.
            command.type = AlterCommand::ADD_COLUMN;
            setAlterCommandColumn(log, command, column_info);
        }
        else
        {
            // Column unchanged.
            continue;
        }

        alter_commands.emplace_back(std::move(command));
    }

    /// Detect dropped columns.
    for (const auto & orig_column_info : orig_table_info.columns)
    {
        const auto & column_info = std::find_if(table_info.columns.begin(),
            table_info.columns.end(),
            [&](const TiDB::ColumnInfo & column_info_) { return column_info_.id == orig_column_info.id; });

        AlterCommand command;
        if (column_info == table_info.columns.end())
        {
            // Dropped column.
            command.type = AlterCommand::DROP_COLUMN;
            command.column_name = orig_column_info.name;
        }
        else
        {
            // Column unchanged.
            continue;
        }

        alter_commands.emplace_back(std::move(command));
    }

    /// Detect type changed columns.
    for (const auto & orig_column_info : orig_table_info.columns)
    {
        const auto & column_info = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
            // TODO: Check primary key.
            // TODO: Support Rename Column;
            return column_info_.id == orig_column_info.id && column_info_.tp != orig_column_info.tp;
        });

        AlterCommand command;
        if (column_info == table_info.columns.end())
        {
            // Column unchanged.
            continue;
        }
        else
        {
            // Type changed column.
            command.type = AlterCommand::MODIFY_COLUMN;
            setAlterCommandColumn(log, command, *column_info);
        }

        alter_commands.emplace_back(std::move(command));
    }

    return alter_commands;
}

void SchemaBuilder::applyAlterTableImpl(TableInfoPtr table_info, const String & db_name, StorageMergeTree * storage)
{
    table_info->schema_version = target_version;
    auto orig_table_info = storage->getTableInfo();
    auto alter_commands = detectSchemaChanges(log, *table_info, orig_table_info);

    std::stringstream ss;
    ss << "Detected schema changes: " << db_name << "." << table_info->name << "\n";
    for (const auto & command : alter_commands)
    {
        // TODO: Other command types.
        if (command.type == AlterCommand::ADD_COLUMN)
            ss << "ADD COLUMN " << command.column_name << " " << command.data_type->getName() << ", ";
        else if (command.type == AlterCommand::DROP_COLUMN)
            ss << "DROP COLUMN " << command.column_name << ", ";
        else if (command.type == AlterCommand::MODIFY_COLUMN)
            ss << "MODIFY COLUMN " << command.column_name << " " << command.data_type->getName() << ", ";
    }
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": " << ss.str());

    // Call storage alter to apply schema changes.
    storage->alterForTMT(alter_commands, *table_info, db_name, context);

    if (table_info->is_partition_table)
    {
        // create partition table.
        for (auto part_def : table_info->partition.definitions)
        {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id);
            storage->alterForTMT(alter_commands, new_table_info, db_name, context);
        }
    }

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Schema changes apply done.");
}

void SchemaBuilder::applyAlterTable(TiDB::DBInfoPtr dbInfo, Int64 table_id)
{
    auto table_info = getter.getTableInfo(dbInfo->id, table_id);
    auto & tmt_context = context.getTMTContext();
    auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_id).get());
    if (storage == nullptr)
    {
        throw Exception("miss table: " + std::to_string(table_id));
    }
    applyAlterTableImpl(table_info, dbInfo->name, storage);
}

void SchemaBuilder::applyDiff(const SchemaDiff & diff)
{

    if (diff.type == SchemaActionCreateSchema)
    {
        applyCreateSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionDropSchema)
    {
        applyDropSchema(diff.schema_id);
        return;
    }

    auto di = getter.getDatabase(diff.schema_id);

    if (di == nullptr)
        throw Exception("miss database: " + std::to_string(diff.schema_id));

    Int64 oldTableID = 0, newTableID = 0;

    switch (diff.type)
    {
        case SchemaActionCreateTable:
        case SchemaActionRecoverTable:
        {
            newTableID = diff.table_id;
            break;
        }
        case SchemaActionDropTable:
        case SchemaActionDropView:
        {
            oldTableID = diff.table_id;
            break;
        }
        case SchemaActionTruncateTable:
        {
            newTableID = diff.table_id;
            oldTableID = diff.old_table_id;
            break;
        }
        case SchemaActionAddColumn:
        case SchemaActionDropColumn:
        case SchemaActionModifyColumn:
        case SchemaActionSetDefaultValue:
        {
            applyAlterTable(di, diff.table_id);
            break;
        }
        case SchemaActionRenameTable:
        {
            applyRenameTable(di, diff.old_schema_id, diff.table_id);
        }
        case SchemaActionAddTablePartition:
        {
            //applyAddPartition(di, diff.table_id);
            break;
        }
        case SchemaActionDropTablePartition:
        {
            //applyDropPartition(di, diff.table_id);
            break;
        }
        default:
        {
            break;
        }
    }

    if (oldTableID)
    {
        applyDropTable(di, oldTableID);
    }

    if (newTableID)
    {
        applyCreateTable(di, newTableID);
    }
}

void SchemaBuilder::applyRenameTable(DBInfoPtr db_info, DatabaseID old_db_id, TableID table_id)
{
    DBInfoPtr old_db_info;
    if (db_info->id == old_db_id)
    {
        old_db_info = db_info;
    }
    else
    {
        auto db = getter.getDatabase(old_db_id);
        if (db == nullptr)
        {
            throw Exception("miss old db id " + std::to_string(old_db_id));
        }
        old_db_info = db;
    }

    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        throw Exception("miss old table id in TiKV " + std::to_string(table_id));
    }

    auto & tmt_context = context.getTMTContext();
    auto storage_to_rename = tmt_context.getStorages().get(table_id).get();
    if (storage_to_rename == nullptr)
    {
        throw Exception("miss old table id in Flash " + std::to_string(table_id));
    }

    applyRenameTableImpl(old_db_info->name, db_info->name, storage_to_rename->getTableName(), table_info->name);
}

void SchemaBuilder::applyRenameTableImpl(const String & old_db, const String & new_db, const String & old_table, const String & new_table)
{
    LOG_INFO(log, "The " + old_db + "." + old_table + " will be renamed to " + new_db + "." + new_table);
    if (old_db == new_db && old_table == new_table)
    {
        return;
    }

    auto rename = std::make_shared<ASTRenameQuery>();

    ASTRenameQuery::Table from;
    from.database = old_db;
    from.table = old_table;

    ASTRenameQuery::Table to;
    to.database = new_db;
    to.table = new_table;

    ASTRenameQuery::Element elem;
    elem.from = from;
    elem.to = to;

    rename->elements.emplace_back(elem);

    InterpreterRenameQuery(rename, context).execute();
}

bool SchemaBuilder::applyCreateSchema(DatabaseID schema_id)
{
    auto db = getter.getDatabase(schema_id);
    if (db->name == "")
    {
        return false;
    }
    applyCreateSchemaImpl(db);
    return true;
}

void SchemaBuilder::applyCreateSchemaImpl(TiDB::DBInfoPtr db_info)
{
    ASTCreateQuery * create_query = new ASTCreateQuery();
    create_query->database = db_info->name;
    create_query->if_not_exists = true;
    ASTPtr ast = ASTPtr(create_query);
    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    databases[db_info->id] = db_info->name;
}

void SchemaBuilder::applyDropSchema(DatabaseID schema_id)
{
    auto database_name = databases[schema_id];
    if (unlikely(database_name == ""))
    {
        LOG_INFO(log, "Syncer wants to drop database: " + std::to_string(schema_id) + " . But database is not found, may has been dropped.");
        return;
    }
    LOG_INFO(log, "Try to drop database: " + database_name);
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    drop_query->if_exists = true;
    // It will drop all tables in this database.
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();

    databases.erase(schema_id);
}

String createTableStmt(const DBInfo & db_info, const TableInfo & table_info)
{
    NamesAndTypes columns;
    std::vector<String> pks;
    for (const auto & column : table_info.columns)
    {
        DataTypePtr type = getDataTypeByColumnInfo(column);
        columns.emplace_back(NameAndTypePair(column.name, type));

        if (column.hasPriKeyFlag())
        {
            pks.emplace_back(column.name);
        }
    }

    if (pks.size() != 1 || !table_info.pk_is_handle)
    {
        columns.emplace_back(NameAndTypePair(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeInt64>()));
        pks.clear();
        pks.emplace_back(MutableSupport::tidb_pk_column_name);
    }

    String stmt;
    WriteBufferFromString stmt_buf(stmt);
    writeString("CREATE TABLE ", stmt_buf);
    writeBackQuotedString(db_info.name, stmt_buf);
    writeString(".", stmt_buf);
    writeBackQuotedString(table_info.name, stmt_buf);
    writeString("(", stmt_buf);
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (i > 0)
            writeString(", ", stmt_buf);
        writeBackQuotedString(columns[i].name, stmt_buf);
        writeString(" ", stmt_buf);
        writeString(columns[i].type->getName(), stmt_buf);
    }
    writeString(") Engine = TxnMergeTree((", stmt_buf);
    for (size_t i = 0; i < pks.size(); i++)
    {
        if (i > 0)
            writeString(", ", stmt_buf);
        writeBackQuotedString(pks[i], stmt_buf);
    }
    writeString("), 8192, '", stmt_buf);
    writeString(table_info.serialize(true), stmt_buf);
    writeString("')", stmt_buf);

    return stmt;
}

void SchemaBuilder::applyCreatePhysicalTableImpl(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info)
{
    String stmt = createTableStmt(db_info, table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->database = db_info.name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

void SchemaBuilder::applyCreateTable(TiDB::DBInfoPtr db_info, Int64 table_id)
{

    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        return;
    }
    applyCreateTableImpl(*db_info, *table_info);
}

void SchemaBuilder::applyCreateTableImpl(const TiDB::DBInfo & db_info, TiDB::TableInfo & table_info)
{
    table_info.schema_version = target_version;
    if (table_info.is_partition_table)
    {
        // create partition table.
        for (auto part_def : table_info.partition.definitions)
        {
            auto new_table_info = table_info.producePartitionTableInfo(part_def.id);
            applyCreatePhysicalTableImpl(db_info, new_table_info);
        }
    }
    else
    {
        applyCreatePhysicalTableImpl(db_info, table_info);
    }
}

void SchemaBuilder::applyDropTableImpl(const String & database_name, const String & table_name)
{
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->table = table_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
}

void SchemaBuilder::applyDropTable(TiDB::DBInfoPtr dbInfo, Int64 table_id)
{
    LOG_INFO(log, "try to drop table id : " + std::to_string(table_id));
    String database_name = dbInfo->name;
    auto & tmt_context = context.getTMTContext();
    auto storage_to_drop = tmt_context.getStorages().get(table_id).get();
    if (storage_to_drop == nullptr)
    {
        LOG_DEBUG(log, "table id " + std::to_string(table_id) + " in db " + database_name + " is not existed.");
        return;
    }
    const auto & table_info = static_cast<StorageMergeTree *>(storage_to_drop)->getTableInfo();
    if (table_info.is_partition_table)
    {
        // drop all partition tables.
        for (auto part_def : table_info.partition.definitions)
        {
            auto new_table_info = table_info.producePartitionTableInfo(part_def.id);
            applyDropTableImpl(database_name, new_table_info.name);
        }
    }
    // and drop logic table.
    applyDropTableImpl(database_name, table_info.name);
}

// Drop Invalid Tables in Every DB
void SchemaBuilder::dropInvalidTables(std::vector<std::pair<TableInfoPtr, DBInfoPtr>> table_dbs) {

    std::set<TableID> table_ids;

    for (auto table_db : table_dbs)
        table_ids.insert(table_db.first->id);

    auto & tmt_context = context.getTMTContext();
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        auto storage = it->second;
        if (table_ids.count(storage->getTableInfo().id) == 0)
        {
            // Drop Table
            const String db_name = storage->getDatabaseName();
            applyDropTableImpl(db_name, storage->getTableName());
            LOG_DEBUG(log, "Table " + db_name + "." + storage->getTableName() + " is dropped during schema all schemas");
        }
    }
}

using TableName = std::pair<String, String>;
using TableNamePair = std::pair<TableName, TableName>;
using TableNameMap = std::map<TableName, TableName>;
using TableNameSet = std::set<TableName>;
constexpr char TmpTableNamePrefix[] = "_tiflash_tmp_";

inline TableName generateTmpTable(const TableName & name) {
    return TableName(name.first, String(TmpTableNamePrefix) + name.second);
}

TableNamePair resolveRename(SchemaBuilder * builder, TableNameMap & map, TableNameMap::iterator it, TableNameSet & visited, TableNameMap::iterator & cycle_it) {
    TableName target_name = it->second;
    TableName origin_name = it->first;
    visited.insert(it->first);
    auto next_it = map.find(target_name);
    if(next_it == map.end()) {
        builder->applyRenameTableImpl(origin_name.first, target_name.first, origin_name.second, target_name.second);
        map.erase(it);
        return TableNamePair();
    } else if (visited.find(target_name) != visited.end()) {
        // There is a cycle.
        auto tmp_name = generateTmpTable(target_name);
        builder->applyRenameTableImpl(target_name.first, tmp_name.first, target_name.second, tmp_name.second);
        builder->applyRenameTableImpl(origin_name.first, target_name.first, origin_name.second, target_name.second);
        map.erase(it);
        return TableNamePair(target_name, tmp_name);
    } else {
        auto pair = resolveRename(builder, map, next_it, visited, cycle_it);
        if (pair.first == origin_name) {
            origin_name = pair.second;
        }
        builder->applyRenameTableImpl(origin_name.first, target_name.first, origin_name.second, target_name.second);
        map.erase(it);
        return pair;
    }
}

void SchemaBuilder::alterAndRenameTables(std::vector<std::pair<TableInfoPtr, DBInfoPtr>> table_dbs) {
    // Rename Table First.
    auto & tmt_context = context.getTMTContext();
    auto storage_map = tmt_context.getStorages().getAllStorage();
    TableNameMap rename_map;
    for (auto table_db : table_dbs) {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage != nullptr) {
            const String old_db = storage->getDatabaseName();
            const String old_table = storage->getTableName();
            const String new_db = table_db.second->name;
            const String new_table = table_db.first->name;
            if (old_db != new_db || old_table != new_table) {
                rename_map[TableName(old_db, old_table)] = TableName(new_db, new_table);
            }
        }
    }

    while(!rename_map.empty()) {
        auto it = rename_map.begin();
        auto tmp_it = rename_map.end();
        TableNameSet visited;
        resolveRename(this, rename_map, it, visited, tmp_it);
    }

    // Then Alter Table
    for (auto table_db : table_dbs) {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage != nullptr) {
            const String db_name = storage->getDatabaseName();
            applyAlterTableImpl(table_db.first, db_name, storage);
        }
    }

}

void SchemaBuilder::createTables(std::vector<std::pair<TableInfoPtr, DBInfoPtr>> table_dbs)
{
    auto & tmt_context = context.getTMTContext();
    for (auto table_db : table_dbs) {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage == nullptr) {
            applyCreateTableImpl(*table_db.second, *table_db.first);
        }
    }
}

void SchemaBuilder::syncAllSchema()
{
    LOG_DEBUG(log, "try load all schemas.");

    std::vector<DBInfoPtr> all_schema = getter.listDBs();

    for (auto db_info : all_schema)
    {
        LOG_DEBUG(log, "Load schema : " + db_info->name);
    }

    std::set<TiDB::DatabaseID> db_ids;
    for (auto db : all_schema)
    {
        db_ids.insert(db->id);
    }

    // Drop invalid databases;
    for (auto it = databases.begin(); it != databases.end(); it++)
    {
        if (db_ids.count(it->first) == 0)
        {
            applyDropSchema(it->first);
        }
    }

    // Collect All Table Info and Create DBs.
    std::vector<std::pair<TableInfoPtr, DBInfoPtr>> all_tables;
    for (auto db : all_schema) {
        auto database_name = databases[db->id];
        if (database_name == "")
        {
            applyCreateSchemaImpl(db);
        }
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto table : tables) {
            all_tables.push_back(std::make_pair(table, db));
        }
    }

    dropInvalidTables(all_tables);
    alterAndRenameTables(all_tables);
    createTables(all_tables);
}

// end namespace
} // namespace DB
