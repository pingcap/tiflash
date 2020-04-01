#include <Common/TiFlashMetrics.h>
#include <Debug/MockSchemaGetter.h>
#include <Debug/MockSchemaNameMapper.h>
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
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/SchemaBuilder-internal.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/SchemaNameMapper.h>
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
    command.column_id = column_info.id;
    command.data_type = getDataTypeByColumnInfo(column_info);
    if (!column_info.origin_default_value.isEmpty())
    {
        LOG_DEBUG(log, "add default value for column: " << column_info.name);
        ASTPtr arg0;
        // If it's date time types, we should use string literal to generate default value.
        if (column_info.tp == TypeDatetime || column_info.tp == TypeTimestamp || column_info.tp == TypeDate)
            arg0 = std::make_shared<ASTLiteral>(Field(column_info.origin_default_value.convert<String>()));
        else
            arg0 = std::make_shared<ASTLiteral>(column_info.defaultValueToField());
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

AlterCommand newRenameColCommand(const String & old_col, const String & new_col, ColumnID new_col_id, const TableInfo & orig_table_info)
{
    AlterCommand command;
    command.type = AlterCommand::RENAME_COLUMN;
    command.column_name = old_col;
    command.new_column_name = new_col;
    command.column_id = new_col_id;
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

inline std::vector<AlterCommands> detectSchemaChanges(
    Logger * log, Context & context, const TableInfo & table_info, const TableInfo & orig_table_info)
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
                command.column_id = orig_column_info.id;
                drop_commands.emplace_back(std::move(command));
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_drop_column).Increment();
            }
        }
        result.push_back(drop_commands);
    }

    {
        /// rename columns.
        /// Note that if new column data type has changed at the same time, this do not apply data type change.
        /// We will apply another alter command later to apply data type change.
        using Resolver = CyclicRenameResolver<ColumnNameWithID, TmpColNameWithIDGenerator>;
        typename Resolver::NameMap rename_map;
        for (const auto & orig_column_info : orig_table_info.columns)
        {
            const auto & column_info
                = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
                      return (column_info_.id == orig_column_info.id && column_info_.name != orig_column_info.name);
                  });

            if (column_info != table_info.columns.end())
            {
                rename_map[ColumnNameWithID{orig_column_info.name, orig_column_info.id}]
                    = ColumnNameWithID{column_info->name, column_info->id};
            }
        }

        typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));
        for (const auto & rename_pair : rename_result)
        {
            AlterCommands rename_commands;
            rename_commands.push_back(
                newRenameColCommand(rename_pair.first.name, rename_pair.second.name, rename_pair.second.id, orig_table_info));
            result.push_back(rename_commands);
            GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_rename_column).Increment();
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
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_alter_column_tp).Increment();
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
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_add_column).Increment();
            }
        }

        result.push_back(add_commands);
    }

    return result;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterTableOrPartition(DBInfoPtr db_info, TableInfoPtr table_info, ManageableStoragePtr storage)
{
    LOG_INFO(log, "Altering table " << name_mapper.displayCanonicalName(*db_info, *table_info));

    table_info->schema_version = target_version;
    auto orig_table_info = storage->getTableInfo();
    auto commands_vec = detectSchemaChanges(log, context, *table_info, orig_table_info);

    std::stringstream ss;
    ss << "Detected schema changes: " << name_mapper.displayCanonicalName(*db_info, *table_info) << "\n";
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

    for (const auto & alter_commands : commands_vec)
        if (!alter_commands.empty())
            storage->alterFromTiDB(alter_commands, name_mapper.mapDatabaseName(*db_info), *table_info, name_mapper, context);

    LOG_INFO(log, "Altered table " << name_mapper.displayCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterTable(TiDB::DBInfoPtr db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr || table_info == nullptr)
    {
        throw Exception("miss table: " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }

    // Alter logical table first.
    applyAlterTableOrPartition(db_info, table_info, storage);

    if (table_info->isLogicalPartitionTable())
    {
        // Alter physical tables of a partition table.
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage != nullptr)
                applyAlterTableOrPartition(db_info, part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDiff(const SchemaDiff & diff)
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

    TableID old_table_id = 0, new_table_id = 0;

    switch (diff.type)
    {
        case SchemaActionCreateTable:
        case SchemaActionRecoverTable:
        {
            new_table_id = diff.table_id;
            break;
        }
        case SchemaActionDropTable:
        case SchemaActionDropView:
        {
            old_table_id = diff.table_id;
            break;
        }
        case SchemaActionTruncateTable:
        {
            new_table_id = diff.table_id;
            old_table_id = diff.old_table_id;
            break;
        }
        case SchemaActionAddColumn:
        case SchemaActionDropColumn:
        case SchemaActionModifyColumn:
        case SchemaActionSetDefaultValue:
        // Add primary key change primary keys to not null, so it's equal to alter table for tiflash.
        case SchemaActionAddPrimaryKey:
        {
            applyAlterTable(di, diff.table_id);
            break;
        }
        case SchemaActionRenameTable:
        {
            applyRenameTable(di, diff.table_id);
            break;
        }
        case SchemaActionAddTablePartition:
        case SchemaActionDropTablePartition:
        case SchemaActionTruncateTablePartition:
        {
            applyPartitionDiff(di, diff.table_id);
            break;
        }
        default:
        {
            LOG_INFO(log, "ignore change type: " << int(diff.type));
            break;
        }
    }

    if (old_table_id)
    {
        applyDropTable(di, old_table_id);
    }

    if (new_table_id)
    {
        applyCreateTable(di, new_table_id);
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(TiDB::DBInfoPtr db_info, TableID table_id)
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
        throw Exception("miss table in Flash " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }
    const auto & orig_table_info = storage->getTableInfo();
    if (!table_info->isLogicalPartitionTable())
    {
        throw Exception(
            "new table in TiKV not partition table " + name_mapper.displayCanonicalName(*db_info, *table_info), ErrorCodes::DDL_ERROR);
    }
    if (!orig_table_info.isLogicalPartitionTable())
    {
        throw Exception(
            "old table in Flash not partition table " + name_mapper.displayCanonicalName(*db_info, orig_table_info), ErrorCodes::DDL_ERROR);
    }
    const auto & orig_defs = orig_table_info.partition.definitions;
    const auto & new_defs = table_info->partition.definitions;

    for (auto orig_def : orig_defs)
    {
        auto it = std::find_if(
            new_defs.begin(), new_defs.end(), [&](const PartitionDefinition & new_def) { return new_def.id == orig_def.id; });
        if (it == new_defs.end())
        {
            applyDropTableOrPartition(name_mapper.mapDatabaseName(*db_info), orig_def.id);
        }
    }

    for (auto new_def : new_defs)
    {
        auto it = std::find_if(
            orig_defs.begin(), orig_defs.end(), [&](const PartitionDefinition & orig_def) { return new_def.id == orig_def.id; });
        if (it == orig_defs.end())
        {
            auto part_table_info = table_info->producePartitionTableInfo(new_def.id, name_mapper);
            applyCreateTableOrPartition(db_info, *part_table_info);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(DBInfoPtr new_db_info, TableID table_id)
{
    auto new_table_info = getter.getTableInfo(new_db_info->id, table_id);
    if (new_table_info == nullptr)
    {
        throw Exception("miss table id in TiKV " + std::to_string(table_id));
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw Exception("miss table id in Flash " + std::to_string(table_id));
    }
    applyRenameTableOrPartition(new_db_info, *new_table_info, storage);

    if (new_table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage == nullptr)
            {
                throw Exception("miss old table id in Flash " + std::to_string(part_def.id));
            }
            auto part_table_info = new_table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRenameTableOrPartition(new_db_info, *part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTableOrPartition(
    DBInfoPtr new_db_info, const TableInfo & new_table_info, ManageableStoragePtr storage)
{
    auto old_db_name = storage->getDatabaseName();
    const auto & old_table_info = storage->getTableInfo();
    if (old_db_name == name_mapper.mapDatabaseName(*new_db_info) && old_table_info.name == new_table_info.name)
    {
        LOG_DEBUG(log, "Table " << name_mapper.displayCanonicalName(*new_db_info, new_table_info) << " name identical, not renaming.");
        return;
    }

    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_rename_column).Increment();
    LOG_INFO(log,
        "Renaming table " << old_db_name << "." << name_mapper.displayTableName(old_table_info) << " to "
                          << name_mapper.displayCanonicalName(*new_db_info, new_table_info));

    if (old_db_name != name_mapper.mapDatabaseName(*new_db_info))
    {
        /// Renaming table across databases, we issue a RENAME query to move data around.
        // TODO: This whole if branch won't be needed once we flattened the data path.
        auto rename = std::make_shared<ASTRenameQuery>();

        ASTRenameQuery::Table from;
        from.database = old_db_name;
        from.table = name_mapper.mapTableName(old_table_info);

        ASTRenameQuery::Table to;
        to.database = name_mapper.mapDatabaseName(*new_db_info);
        to.table = name_mapper.mapTableName(new_table_info);

        ASTRenameQuery::Element elem;
        elem.from = from;
        elem.to = to;

        rename->elements.emplace_back(elem);

        InterpreterRenameQuery(rename, context).execute();
    }

    /// Update metadata under new database, through calling alterFromTiDB.
    storage->alterFromTiDB(AlterCommands{}, name_mapper.mapDatabaseName(*new_db_info), new_table_info, name_mapper, context);

    LOG_INFO(log,
        "Renamed table " << old_db_name << "." << name_mapper.displayTableName(old_table_info) << " to "
                         << name_mapper.displayCanonicalName(*new_db_info, new_table_info));
}

template <typename Getter, typename NameMapper>
bool SchemaBuilder<Getter, NameMapper>::applyCreateSchema(DatabaseID schema_id)
{
    auto db = getter.getDatabase(schema_id);
    if (db == nullptr || db->name.empty())
    {
        return false;
    }
    applyCreateSchemaByDBInfo(db);
    return true;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateSchemaByDBInfo(TiDB::DBInfoPtr db_info)
{
    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_create_db).Increment();
    LOG_INFO(log, "Creating database " << name_mapper.displayDatabaseName(*db_info));
    ASTCreateQuery * create_query = new ASTCreateQuery();
    create_query->database = name_mapper.mapDatabaseName(*db_info);
    create_query->if_not_exists = true;
    ASTPtr ast = ASTPtr(create_query);
    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    databases[db_info->id] = db_info;
    LOG_INFO(log, "Created database " << name_mapper.displayDatabaseName(*db_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(DatabaseID schema_id)
{
    auto it = databases.find(schema_id);
    if (unlikely(it == databases.end()))
    {
        LOG_INFO(
            log, "Syncer wants to drop database: " << std::to_string(schema_id) << " . But database is not found, may has been dropped.");
        return;
    }
    applyDropSchemaByName(name_mapper.mapDatabaseName(*it->second));
    databases.erase(schema_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchemaByName(const String & schema_name)
{
    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_drop_db).Increment();
    LOG_INFO(log, "Dropping database " << schema_name);
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = schema_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    // It will drop all tables in this database.
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
    LOG_INFO(log, "Dropped database " << schema_name);
}

String createTableStmt(const DBInfo & db_info, const TableInfo & table_info, const SchemaNameMapper & name_mapper, Logger * log)
{
    LOG_DEBUG(log, "Analyzing table info :" << table_info.serialize());
    NamesAndTypes columns;
    std::vector<String> pks;
    for (const auto & column : table_info.columns)
    {
        LOG_DEBUG(log, "Analyzing column :" + column.name + " type " + std::to_string((int)column.tp));
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
    writeBackQuotedString(name_mapper.mapDatabaseName(db_info), stmt_buf);
    writeString(".", stmt_buf);
    writeBackQuotedString(name_mapper.mapTableName(table_info), stmt_buf);
    writeString("(", stmt_buf);
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (i > 0)
            writeString(", ", stmt_buf);
        writeBackQuotedString(columns[i].name, stmt_buf);
        writeString(" ", stmt_buf);
        writeString(columns[i].type->getName(), stmt_buf);
    }

    // storage engine type
    if (table_info.engine_type == TiDB::StorageEngine::TMT)
    {
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
    }
    else if (table_info.engine_type == TiDB::StorageEngine::DT)
    {
        writeString(") Engine = DeltaMerge((", stmt_buf);
        for (size_t i = 0; i < pks.size(); i++)
        {
            if (i > 0)
                writeString(", ", stmt_buf);
            writeBackQuotedString(pks[i], stmt_buf);
        }
        writeString("), '", stmt_buf);
        writeEscapedString(table_info.serialize(), stmt_buf);
        writeString("')", stmt_buf);
    }
    else if (table_info.engine_type == TiDB::StorageEngine::DEBUGGING_MEMORY)
    {
        writeString(") Engine = Debugging('", stmt_buf);
        writeEscapedString(table_info.serialize(), stmt_buf);
        writeString("')", stmt_buf);
    }
    else
    {
        throw Exception("Unknown engine type : " + toString(static_cast<int32_t>(table_info.engine_type)), ErrorCodes::DDL_ERROR);
    }

    return stmt;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateTableOrPartition(DBInfoPtr db_info, TableInfo & table_info)
{
    if (table_info.is_view)
    {
        LOG_INFO(log, "Table " << name_mapper.displayCanonicalName(*db_info, table_info) << " is a view table, ignore it.");
        return;
    }

    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(log, "Creating table " << name_mapper.displayCanonicalName(*db_info, table_info));

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto storage = tmt_context.getStorages().get(table_info.id).get(); storage)
        {
            if (!storage->isTombstone())
                // TODO: Is this an error?
                LOG_WARNING(log,
                    "Trying to create table " << name_mapper.displayCanonicalName(*db_info, table_info)
                                              << " but it already exists and is not tombstoned");

            LOG_DEBUG(log,
                "Recovering table "
                    << "." << name_mapper.displayCanonicalName(*db_info, table_info));
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            storage->alterFromTiDB(commands, name_mapper.mapDatabaseName(*db_info), table_info, name_mapper, context);
            LOG_INFO(log, "Created table " << name_mapper.displayCanonicalName(*db_info, table_info));
            return;
        }
    }

    /// Normal CREATE table.
    table_info.schema_version = target_version;
    if (table_info.engine_type == StorageEngine::UNSPECIFIED)
    {
        auto & tmt_context = context.getTMTContext();
        table_info.engine_type = tmt_context.getEngineType();
    }

    String stmt = createTableStmt(*db_info, table_info, name_mapper, log);

    LOG_INFO(log, "Creating table " << name_mapper.displayCanonicalName(*db_info, table_info) << " with statement: " << stmt);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->if_not_exists = true;
    ast_create_query->database = name_mapper.mapDatabaseName(*db_info);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
    LOG_INFO(log, "Created table " << name_mapper.displayCanonicalName(*db_info, table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateTable(TiDB::DBInfoPtr db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        LOG_DEBUG(log, "Table " << table_id << " not found, may have been dropped.");
        return;
    }

    if (table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyCreateTableOrPartition(db_info, *new_table_info);
        }
    }

    applyCreateTableOrPartition(db_info, *table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTableOrPartition(const String & db_name, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        LOG_DEBUG(log, "table " << table_id << " does not exist.");
        return;
    }
    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_drop_table).Increment();
    LOG_INFO(log, "Tombstoning table " << db_name << "." << name_mapper.displayTableName(storage->getTableInfo()));
    AlterCommands commands;
    {
        AlterCommand command;
        command.type = AlterCommand::TOMBSTONE;
        commands.emplace_back(std::move(command));
    }
    storage->alterFromTiDB(commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(log, "Tombstoned table " << db_name << "." << name_mapper.displayTableName(storage->getTableInfo()));

    //    auto drop_query = std::make_shared<ASTDropQuery>();
    //    drop_query->database = database_name;
    //    drop_query->table = table_name;
    //    drop_query->if_exists = true;
    //    ASTPtr ast_drop_query = drop_query;
    //    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    //    drop_interpreter.execute();
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTable(DBInfoPtr db_info, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id).get();
    if (storage == nullptr)
    {
        LOG_DEBUG(log, "table " << table_id << " does not exist.");
        return;
    }
    const auto & table_info = storage->getTableInfo();
    if (table_info.isLogicalPartitionTable())
    {
        // drop all partition tables.
        for (const auto & part_def : table_info.partition.definitions)
        {
            applyDropTableOrPartition(name_mapper.mapDatabaseName(*db_info), part_def.id);
        }
    }
    // and drop logic table.
    applyDropTableOrPartition(name_mapper.mapDatabaseName(*db_info), table_info.id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::syncAllSchema()
{
    LOG_INFO(log, "Syncing all schemas.");

    auto & tmt_context = context.getTMTContext();

    /// Create all databases.
    std::unordered_set<String> db_set;
    std::vector<DBInfoPtr> all_schemas = getter.listDBs();
    for (const auto & db : all_schemas)
    {
        db_set.emplace(name_mapper.mapDatabaseName(*db));
        if (databases.find(db->id) == databases.end())
        {
            applyCreateSchemaByDBInfo(db);
            LOG_DEBUG(log, "Database " << name_mapper.displayDatabaseName(*db) << " created during sync all schemas");
        }
    }

    /// Load all tables in each database.
    std::unordered_set<TableID> table_set;
    /// Declare lambda for loading table or partition (each corresponding to an individual storage object).
    auto loadTableOrPartition = [&table_set, &tmt_context, this](DBInfoPtr db_info, TableInfoPtr table_info) {
        /// Record for further detecting tables to drop.
        table_set.emplace(table_info->id);
        auto storage = tmt_context.getStorages().get(table_info->id);
        if (storage == nullptr)
        {
            /// Create if not exists.
            applyCreateTableOrPartition(db_info, *table_info);
        }
        else
        {
            /// Rename if needed.
            applyRenameTableOrPartition(db_info, *table_info, storage);
            /// Alter if needed.
            applyAlterTableOrPartition(db_info, table_info, storage);
        }
        LOG_DEBUG(log, "Table " << name_mapper.displayCanonicalName(*db_info, *table_info) << " synced during sync all schemas");
    };
    for (const auto & db : all_schemas)
    {
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto & table : tables)
        {
            loadTableOrPartition(db, table);
            if (table->isLogicalPartitionTable())
            {
                for (const auto & part_def : table->partition.definitions)
                {
                    auto part_table_info = table->producePartitionTableInfo(part_def.id, name_mapper);
                    loadTableOrPartition(db, part_table_info);
                }
            }
        }
        LOG_DEBUG(log, "Database " << name_mapper.displayDatabaseName(*db) << " synced during sync all schemas");
    }

    /// Drop all unmapped tables.
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        if (table_set.count(it->first) == 0)
        {
            applyDropTableOrPartition(it->second->getDatabaseName(), it->first);
            LOG_DEBUG(log,
                "Table " << it->second->getDatabaseName() << "." << name_mapper.displayTableName(it->second->getTableInfo())
                         << " dropped during sync all schemas");
        }
    }

    /// Drop all unmapped dbs.
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        if (db_set.count(it->first) == 0 && it->first != "system")
        {
            applyDropSchemaByName(it->first);
            LOG_DEBUG(log, "DB " << it->first << " dropped during sync all schemas");
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;

// end namespace
} // namespace DB
