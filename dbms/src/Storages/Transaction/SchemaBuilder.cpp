#include <Debug/MockSchemaGetter.h>
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
#include <Parsers/parseQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/SchemaBuilder-internal.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

using namespace TiDB;

namespace ErrorCodes
{
extern const int DDL_ERROR;
}

inline void setAlterCommandColumn(Logger * log, AlterCommand & command, const ColumnInfo & column_info)
{
    command.column_name = column_info.name;
    command.data_type = getDataTypeByColumnInfo(column_info);
    if (!column_info.origin_default_value.isEmpty())
    {
        LOG_DEBUG(log, "add default value for column: " << column_info.name);
        auto arg0 = std::make_shared<ASTLiteral>(column_info.defaultValueToField());
        auto arg1 = std::make_shared<ASTLiteral>(command.data_type->getName());
        auto args = std::make_shared<ASTExpressionList>();
        args->children.emplace_back(arg0);
        args->children.emplace_back(arg1);
        auto func = std::make_shared<ASTFunction>();
        func->name = "CAST";
        func->arguments = args;
        command.default_expression = func;
    }
}

AlterCommand newRenameColCommand(const String & old_col, const String & new_col, const TableInfo & orig_table_info)
{
    AlterCommand command;
    command.type = AlterCommand::RENAME_COLUMN;
    command.column_name = old_col;
    command.new_column_name = new_col;
    if (auto pk = orig_table_info.getPKHandleColumn())
    {
        if (pk->get().name == old_col)
        {
            auto list = std::make_shared<ASTExpressionList>();
            auto new_pk = std::make_shared<ASTIdentifier>(new_col);
            list->children.push_back(new_pk);
            command.primary_key = list;
        }
    }
    return command;
}

inline std::vector<AlterCommands> detectSchemaChanges(Logger * log, const TableInfo & table_info, const TableInfo & orig_table_info)
{
    std::vector<AlterCommands> result;

    // add drop commands
    {
        AlterCommands drop_commands;

        /// Detect dropped columns.
        for (const auto & orig_column_info : orig_table_info.columns)
        {
            const auto & column_info = std::find_if(table_info.columns.begin(),
                table_info.columns.end(),
                [&](const TiDB::ColumnInfo & column_info_) { return column_info_.id == orig_column_info.id; });

            if (column_info == table_info.columns.end())
            {
                AlterCommand command;
                // Dropped column.
                command.type = AlterCommand::DROP_COLUMN;
                // Drop column with old name.
                command.column_name = orig_column_info.name;
                drop_commands.emplace_back(std::move(command));
            }
        }
        result.push_back(drop_commands);
    }

    {
        using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
        typename Resolver::NameMap rename_map;
        /// rename columns.
        for (const auto & orig_column_info : orig_table_info.columns)
        {
            const auto & column_info
                = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
                      return (column_info_.id == orig_column_info.id && column_info_.name != orig_column_info.name);
                  });

            if (column_info != table_info.columns.end())
            {
                rename_map[orig_column_info.name] = column_info->name;
            }
        }

        typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));
        for (const auto & rename_pair : rename_result)
        {
            AlterCommands rename_commands;
            rename_commands.push_back(newRenameColCommand(rename_pair.first, rename_pair.second, orig_table_info));
            result.push_back(rename_commands);
        }
    }

    // alter commands
    {
        AlterCommands alter_commands;
        /// Detect type changed columns.
        for (const auto & orig_column_info : orig_table_info.columns)
        {
            const auto & column_info
                = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
                      if (column_info_.id == orig_column_info.id && column_info_.name != orig_column_info.name)
                          LOG_INFO(log, "detect column " << orig_column_info.name << " rename to " << column_info_.name);

                      return column_info_.id == orig_column_info.id
                          && (column_info_.tp != orig_column_info.tp || column_info_.hasNotNullFlag() != orig_column_info.hasNotNullFlag());
                  });

            if (column_info != table_info.columns.end())
            {
                AlterCommand command;
                // Type changed column.
                command.type = AlterCommand::MODIFY_COLUMN;
                // Alter column with new column info
                setAlterCommandColumn(log, command, *column_info);
                alter_commands.emplace_back(std::move(command));
            }
        }
        result.push_back(alter_commands);
    }

    {
        AlterCommands add_commands;
        /// Detect new columns.
        for (const auto & column_info : table_info.columns)
        {
            const auto & orig_column_info = std::find_if(orig_table_info.columns.begin(),
                orig_table_info.columns.end(),
                [&](const TiDB::ColumnInfo & orig_column_info_) { return orig_column_info_.id == column_info.id; });

            if (orig_column_info == orig_table_info.columns.end())
            {
                AlterCommand command;
                // New column.
                command.type = AlterCommand::ADD_COLUMN;
                setAlterCommandColumn(log, command, column_info);

                add_commands.emplace_back(std::move(command));
            }
        }

        result.push_back(add_commands);
    }

    return result;
}

template <typename Getter>
void SchemaBuilder<Getter>::applyAlterTableImpl(TableInfoPtr table_info, const String & db_name, StorageMergeTree * storage)
{
    table_info->schema_version = target_version;
    auto orig_table_info = storage->getTableInfo();
    auto commands_vec = detectSchemaChanges(log, *table_info, orig_table_info);

    std::stringstream ss;
    ss << "Detected schema changes: " << db_name << "." << table_info->name << "\n";
    for (const auto & alter_commands : commands_vec)
        for (const auto & command : alter_commands)
        {
            if (command.type == AlterCommand::ADD_COLUMN)
                ss << "ADD COLUMN " << command.column_name << " " << command.data_type->getName() << ", ";
            else if (command.type == AlterCommand::DROP_COLUMN)
                ss << "DROP COLUMN " << command.column_name << ", ";
            else if (command.type == AlterCommand::MODIFY_COLUMN)
                ss << "MODIFY COLUMN " << command.column_name << " " << command.data_type->getName() << ", ";
            else if (command.type == AlterCommand::RENAME_COLUMN)
                ss << "RENAME COLUMN from " << command.column_name << " to " << command.new_column_name << ", ";
        }

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": " << ss.str());

    // Call storage alter to apply schema changes.
    for (const auto & alter_commands : commands_vec)
        storage->alterForTMT(alter_commands, *table_info, db_name, context);

    auto & tmt_context = context.getTMTContext();

    if (table_info->isLogicalPartitionTable())
    {
        // create partition table.
        for (auto part_def : table_info->partition.definitions)
        {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id);
            auto part_storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(part_def.id).get());
            if (part_storage != nullptr)
                for (const auto & alter_commands : commands_vec)
                    part_storage->alterForTMT(alter_commands, new_table_info, db_name, context);
        }
    }

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Schema changes apply done.");
}

template <typename Getter>
void SchemaBuilder<Getter>::applyAlterTable(TiDB::DBInfoPtr dbInfo, Int64 table_id)
{
    auto table_info = getter.getTableInfo(dbInfo->id, table_id);
    auto & tmt_context = context.getTMTContext();
    auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_id).get());
    if (storage == nullptr || table_info == nullptr)
    {
        throw Exception("miss table: " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }
    applyAlterTableImpl(table_info, dbInfo->name, storage);
}

template <typename Getter>
void SchemaBuilder<Getter>::applyDiff(const SchemaDiff & diff)
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
        throw Exception("miss database: " + std::to_string(diff.schema_id), ErrorCodes::DDL_ERROR);

    if (isIgnoreDB(di->name))
    {
        LOG_INFO(log, "ignore schema changes for db: " << di->name);
        return;
    }

    Int64 oldTableID = 0, newTableID = 0;

    switch (diff.type)
    {
        case SchemaActionCreateTable:
        case SchemaActionRecoverTable: {
            newTableID = diff.table_id;
            break;
        }
        case SchemaActionDropTable:
        case SchemaActionDropView: {
            oldTableID = diff.table_id;
            break;
        }
        case SchemaActionTruncateTable: {
            newTableID = diff.table_id;
            oldTableID = diff.old_table_id;
            break;
        }
        case SchemaActionAddColumn:
        case SchemaActionDropColumn:
        case SchemaActionModifyColumn:
        case SchemaActionSetDefaultValue: {
            applyAlterTable(di, diff.table_id);
            break;
        }
        case SchemaActionRenameTable: {
            applyRenameTable(di, diff.old_schema_id, diff.table_id);
            break;
        }
        case SchemaActionAddTablePartition:
        case SchemaActionDropTablePartition: {
            applyAlterPartition(di, diff.table_id);
            break;
        }
        default: {
            LOG_INFO(log, "ignore change type: " << int(diff.type));
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

template <typename Getter>
void SchemaBuilder<Getter>::applyAlterPartition(TiDB::DBInfoPtr db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        throw Exception("miss old table id in TiKV " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id).get();
    if (storage == nullptr)
    {
        throw Exception("miss table in Flash " + table_info->name, ErrorCodes::DDL_ERROR);
    }
    const String & db_name = storage->getDatabaseName();
    const auto & orig_table_info = storage->getTableInfo();
    if (!table_info->isLogicalPartitionTable())
    {
        throw Exception("miss old table id in Flash " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }
    const auto & orig_defs = orig_table_info.partition.definitions;
    const auto & new_defs = table_info->partition.definitions;

    for (auto orig_def : orig_defs)
    {
        auto it = std::find_if(
            new_defs.begin(), new_defs.end(), [&](const PartitionDefinition & new_def) { return new_def.id == orig_def.id; });
        if (it == new_defs.end())
        {
            applyDropTableImpl(db_name, orig_table_info.getPartitionTableName(orig_def.id));
        }
    }

    for (auto new_def : new_defs)
    {
        auto it = std::find_if(
            orig_defs.begin(), orig_defs.end(), [&](const PartitionDefinition & orig_def) { return new_def.id == orig_def.id; });
        if (it == orig_defs.end())
        {
            auto part_table_info = table_info->producePartitionTableInfo(new_def.id);
            applyCreatePhysicalTableImpl(*db_info, part_table_info);
        }
    }
}

std::vector<std::pair<TableInfoPtr, DBInfoPtr>> collectPartitionTables(TableInfoPtr table_info, DBInfoPtr db_info)
{
    std::vector<std::pair<TableInfoPtr, DBInfoPtr>> all_tables;
    // Collect All partition tables.
    for (auto part_def : table_info->partition.definitions)
    {
        auto new_table_info = table_info->producePartitionTableInfo(part_def.id);
        all_tables.push_back(std::make_pair(std::make_shared<TableInfo>(new_table_info), db_info));
    }
    return all_tables;
}

template <typename Getter>
void SchemaBuilder<Getter>::applyRenameTable(DBInfoPtr db_info, DatabaseID old_db_id, TableID table_id)
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

    storage_to_rename->setTableInfo(*table_info);

    if (table_info->isLogicalPartitionTable())
    {
        const auto & table_dbs = collectPartitionTables(table_info, db_info);
        alterAndRenameTables(table_dbs);
        for (auto table_db : table_dbs)
        {
            auto table = table_db.first;
            auto part_storage = tmt_context.getStorages().get(table->id).get();
            if (part_storage != nullptr)
            {
                part_storage->setTableInfo(*table);
            }
        }
    }
}

template <typename Getter>
void SchemaBuilder<Getter>::applyRenameTableImpl(
    const String & old_db, const String & new_db, const String & old_table, const String & new_table)
{
    LOG_INFO(log, "The " << old_db << "." << old_table << " will be renamed to " << new_db << "." << new_table);
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

template <typename Getter>
bool SchemaBuilder<Getter>::applyCreateSchema(DatabaseID schema_id)
{
    auto db = getter.getDatabase(schema_id);
    if (db == nullptr || db->name == "")
    {
        return false;
    }
    applyCreateSchemaImpl(db);
    return true;
}

template <typename Getter>
void SchemaBuilder<Getter>::applyCreateSchemaImpl(TiDB::DBInfoPtr db_info)
{
    if (isIgnoreDB(db_info->name))
    {
        LOG_INFO(log, "ignore schema changes for db: " << db_info->name);
        return;
    }

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

template <typename Getter>
void SchemaBuilder<Getter>::applyDropSchema(DatabaseID schema_id)
{
    auto it = databases.find(schema_id);
    if (unlikely(it == databases.end()))
    {
        LOG_INFO(
            log, "Syncer wants to drop database: " << std::to_string(schema_id) << " . But database is not found, may has been dropped.");
        return;
    }
    applyDropSchemaImpl(it->second);
    databases.erase(schema_id);
}

template <typename Getter>
void SchemaBuilder<Getter>::applyDropSchemaImpl(const String & database_name)
{
    LOG_INFO(log, "Try to drop database: " << database_name);
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    // It will drop all tables in this database.
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
}

String createTableStmt(const DBInfo & db_info, const TableInfo & table_info, Logger * log)
{
    LOG_DEBUG(log,  "create table :" << table_info.serialize());
    NamesAndTypes columns;
    std::vector<String> pks;
    for (const auto & column : table_info.columns)
    {
        LOG_DEBUG(log,  "create column :" + column.name + " type " + std::to_string((int)column.tp) );
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
    writeEscapedString(table_info.serialize(), stmt_buf);
    writeString("')", stmt_buf);

    return stmt;
}

template <typename Getter>
void SchemaBuilder<Getter>::applyCreatePhysicalTableImpl(const TiDB::DBInfo & db_info, TiDB::TableInfo & table_info)
{
    table_info.schema_version = target_version;

    String stmt = createTableStmt(db_info, table_info, log);

    LOG_INFO(log, "try to create table with stmt: " << stmt);

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

template <typename Getter>
void SchemaBuilder<Getter>::applyCreateTable(TiDB::DBInfoPtr db_info, Int64 table_id)
{

    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        return;
    }
    applyCreateTableImpl(*db_info, *table_info);
}

template <typename Getter>
void SchemaBuilder<Getter>::applyCreateTableImpl(const TiDB::DBInfo & db_info, TiDB::TableInfo & table_info)
{
    if (table_info.isLogicalPartitionTable())
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

template <typename Getter>
void SchemaBuilder<Getter>::applyDropTableImpl(const String & database_name, const String & table_name)
{
    LOG_INFO(log, "try to drop table : " << database_name << "." << table_name);
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->table = table_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
}

template <typename Getter>
void SchemaBuilder<Getter>::applyDropTable(TiDB::DBInfoPtr dbInfo, Int64 table_id)
{
    LOG_DEBUG(log, "drop table id :" << std::to_string(table_id));
    String database_name = dbInfo->name;
    auto & tmt_context = context.getTMTContext();
    auto storage_to_drop = tmt_context.getStorages().get(table_id).get();
    if (storage_to_drop == nullptr)
    {
        LOG_DEBUG(log, "table id " << table_id << " in db " << database_name << " is not existed.");
        return;
    }
    const auto & table_info = static_cast<StorageMergeTree *>(storage_to_drop)->getTableInfo();
    if (table_info.isLogicalPartitionTable())
    {
        // drop all partition tables.
        for (auto part_def : table_info.partition.definitions)
        {
            auto new_table_name = table_info.getPartitionTableName(part_def.id);
            applyDropTableImpl(database_name, new_table_name);
        }
    }
    // and drop logic table.
    applyDropTableImpl(database_name, table_info.name);
}

// Drop Invalid Tables in Every DB
template <typename Getter>
void SchemaBuilder<Getter>::dropInvalidTablesAndDBs(
    const std::vector<std::pair<TableInfoPtr, DBInfoPtr>> & table_dbs, const std::set<String> & db_names)
{

    std::set<TableID> table_ids;
    std::vector<std::pair<String, String>> tables_to_drop;
    std::set<String> dbs_to_drop;

    for (auto table_db : table_dbs)
    {
        table_ids.insert(table_db.first->id);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        auto storage = it->second;
        if (table_ids.count(storage->getTableInfo().id) == 0)
        {
            // Drop Table
            const String db_name = storage->getDatabaseName();
            if (isIgnoreDB(db_name))
            {
                continue;
            }
            tables_to_drop.push_back(std::make_pair(db_name, storage->getTableName()));
        }
    }
    for (auto table : tables_to_drop)
    {
        applyDropTableImpl(table.first, table.second);
        LOG_DEBUG(log, "Table " << table.first << "." << table.second << " is dropped during sync all schemas");
    }
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        String db_name = it->first;
        if (isIgnoreDB(db_name))
        {
            continue;
        }
        if (db_names.count(db_name) == 0)
            dbs_to_drop.insert(db_name);
    }
    for (auto db : dbs_to_drop)
    {
        applyDropSchemaImpl(db);
        LOG_DEBUG(log, "DB " << db << " is dropped during sync all schemas");
    }
}

template <typename Getter>
void SchemaBuilder<Getter>::alterAndRenameTables(std::vector<std::pair<TableInfoPtr, DBInfoPtr>> table_dbs)
{
    using Resolver = CyclicRenameResolver<std::pair<String, String>, TmpTableNameGenerator>;
    using TableName = typename Resolver::Name;

    // Rename Table First.
    auto & tmt_context = context.getTMTContext();
    auto storage_map = tmt_context.getStorages().getAllStorage();

    typename Resolver::NameMap rename_map;
    for (auto table_db : table_dbs)
    {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage != nullptr)
        {
            const String old_db = storage->getDatabaseName();
            const String old_table = storage->getTableName();
            const String new_db = table_db.second->name;
            const String new_table = table_db.first->name;
            if (old_db != new_db || old_table != new_table)
            {
                rename_map[TableName(old_db, old_table)] = TableName(new_db, new_table);
            }
        }
    }

    typename Resolver::NamePairs result = Resolver().resolve(std::move(rename_map));
    for (const auto & rename_pair : result)
    {
        applyRenameTableImpl(rename_pair.first.first, rename_pair.second.first, rename_pair.first.second, rename_pair.second.second);
    }

    // Then Alter Table.
    for (auto table_db : table_dbs)
    {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage != nullptr)
        {
            const String db_name = storage->getDatabaseName();
            applyAlterTableImpl(table_db.first, db_name, storage);
        }
    }
}

template <typename Getter>
void SchemaBuilder<Getter>::createTables(std::vector<std::pair<TableInfoPtr, DBInfoPtr>> table_dbs)
{
    auto & tmt_context = context.getTMTContext();
    for (auto table_db : table_dbs)
    {
        auto storage = static_cast<StorageMergeTree *>(tmt_context.getStorages().get(table_db.first->id).get());
        if (storage == nullptr)
        {
            applyCreatePhysicalTableImpl(*table_db.second, *table_db.first);
        }
    }
}

template <typename Getter>
void SchemaBuilder<Getter>::syncAllSchema()
{
    LOG_DEBUG(log, "try load all schemas.");

    std::vector<DBInfoPtr> all_schema = getter.listDBs();

    for (auto it = all_schema.begin(); it != all_schema.end();)
    {
        if (isIgnoreDB((*it)->name))
        {
            LOG_INFO(log, "ignore schema changes for db: " << (*it)->name);
            it = all_schema.erase(it);
        }
        else
        {
            it++;
        }
    }

    for (auto db_info : all_schema)
    {
        LOG_DEBUG(log, "Load schema : " << db_info->name);
    }

    // Collect All Table Info and Create DBs.
    std::vector<std::pair<TableInfoPtr, DBInfoPtr>> all_tables;
    for (auto db : all_schema)
    {
        if (databases.find(db->id) == databases.end())
        {
            applyCreateSchemaImpl(db);
        }
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto table : tables)
        {
            all_tables.push_back(std::make_pair(table, db));
            if (table->isLogicalPartitionTable())
            {
                auto partition_tables = collectPartitionTables(table, db);
                all_tables.insert(all_tables.end(), partition_tables.begin(), partition_tables.end());
            }
        }
    }

    std::set<String> db_names;
    for (auto db : all_schema)
    {
        db_names.insert(db->name);
    }

    dropInvalidTablesAndDBs(all_tables, db_names);
    alterAndRenameTables(all_tables);
    createTables(all_tables);
}

template <typename Getter>
bool SchemaBuilder<Getter>::isIgnoreDB(const String & name)
{
    return context.getTMTContext().getIgnoreDatabases().count(name) > 0;
}

template struct SchemaBuilder<SchemaGetter>;
template struct SchemaBuilder<MockSchemaGetter>;

// end namespace
} // namespace DB
