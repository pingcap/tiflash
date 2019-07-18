#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseOrdinary.h>
#include <Debug/MockSchemaSyncer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <common/JSON.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using TableInfo = TiDB::TableInfo;
using ColumnInfo = TiDB::ColumnInfo;

String createDatabaseStmt(const TableInfo & table_info) { return "CREATE DATABASE " + table_info.db_name; }

void createDatabase(const TableInfo & table_info, Context & context)
{
    String stmt = createDatabaseStmt(table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = table_info.db_name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

String createTableStmt(const TableInfo & table_info)
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
    writeBackQuotedString(table_info.db_name, stmt_buf);
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

void createTable(const TableInfo & table_info, Context & context)
{
    String stmt = createTableStmt(table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = table_info.db_name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

void dropTable(const std::string & database_name, const std::string & table_name, Context & context)
{
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->table = table_name;
    ASTPtr ast_drop_query = drop_query;
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
}

void renameTable(const std::string & old_db, const std::string & old_tbl, const TableInfo & table_info, Context & context)
{
    auto rename = std::make_shared<ASTRenameQuery>();

    ASTRenameQuery::Table from;
    from.database = old_db;
    from.table = old_tbl;

    ASTRenameQuery::Table to;
    to.database = table_info.db_name;
    to.table = table_info.name;

    ASTRenameQuery::Element elem;
    elem.from = from;
    elem.to = to;

    rename->elements.emplace_back(elem);

    InterpreterRenameQuery(rename, context).execute();
}

AlterCommands detectSchemaChanges(const TableInfo & table_info, const TableInfo & orig_table_info)
{
    AlterCommands alter_commands;

    /// Detect new columns.
    for (const auto & column_info : table_info.columns)
    {
        const auto & orig_column_info = std::find_if(orig_table_info.columns.begin(),
            orig_table_info.columns.end(),
            [&](const ColumnInfo & orig_column_info_) { return orig_column_info_.id == column_info.id; });

        AlterCommand command;
        if (orig_column_info == orig_table_info.columns.end())
        {
            // New column.
            command.type = AlterCommand::ADD_COLUMN;
            command.column_name = column_info.name;
            command.data_type = getDataTypeByColumnInfo(column_info);
            // TODO: support default value.
            // TODO: support after column.
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
        const auto & column_info = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
            return column_info_.id == orig_column_info.id;
        });

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
            command.column_name = orig_column_info.name;
            command.data_type = getDataTypeByColumnInfo(*column_info);
        }

        alter_commands.emplace_back(std::move(command));
    }

    // TODO: Detect rename columns.

    return alter_commands;
}

MockSchemaSyncer::MockSchemaSyncer() : log(&Logger::get("MockSchemaSyncer")) {}

bool MockSchemaSyncer::syncSchemas(Context & context)
{
    std::unordered_map<TableID, MockTiDB::TablePtr> new_tables;
    MockTiDB::instance().traverseTables([&](const auto & table) { new_tables.emplace(table->id(), table); });

    for (auto [id, table] : tables)
    {
        if (new_tables.find(id) == new_tables.end())
            dropTable(table->table_info.db_name, table->table_info.name, context);
    }

    for (auto [id, table] : new_tables)
    {
        std::ignore = id;
        syncTable(context, table);
    }

    tables.swap(new_tables);

    return true;
}

void MockSchemaSyncer::syncTable(Context & context, MockTiDB::TablePtr table)
{
    auto & tmt_context = context.getTMTContext();

    /// Get table schema json.
    auto table_id = table->id();

    String table_info_json = table->table_info.serialize(false);

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Table " << table_id << " info json: " << table_info_json);

    TableInfo table_info(table_info_json, false);

    auto storage = tmt_context.getStorages().get(table_id);

    if (storage == nullptr)
    {
        if (!context.isDatabaseExist(table_info.db_name))
        {
            /// Database not existing, create it.
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Creating database " << table_info.db_name);
            createDatabase(table_info, context);
        }

        auto create_table_internal = [&]() {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Creating table " << table_info.name);
            createTable(table_info, context);
            auto logical_storage = std::static_pointer_cast<StorageMergeTree>(context.getTable(table_info.db_name, table_info.name));
            context.getTMTContext().getStorages().put(logical_storage);

            /// Mangle for partition table.
            bool is_partition_table = table_info.manglePartitionTableIfNeeded(table_id);
            if (is_partition_table && !context.isTableExist(table_info.db_name, table_info.name))
            {
                LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Re-creating table after mangling partition table " << table_info.name);
                createTable(table_info, context);
                auto physical_storage = std::static_pointer_cast<StorageMergeTree>(context.getTable(table_info.db_name, table_info.name));
                context.getTMTContext().getStorages().put(physical_storage);
            }
        };

        if (!context.isTableExist(table_info.db_name, table_info.name))
        {
            /// Table not existing, create it.
            create_table_internal();
        }
        else
        {
            /// Table existing but with a new table ID, meaning this table is either be truncated (table ID changed by TiDB) or dropped-then-recreated.
            /// Drop existing table and re-create.
            LOG_DEBUG(log,
                __PRETTY_FUNCTION__ << ": TMT storage with ID " << table_id << " doesn't exist but table " << table_info.db_name << "."
                                    << table_info.name + " exists.");
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Dropping table " << table_info.db_name << "." << table_info.name);
            // TODO: Partition table?
            dropTable(table_info.db_name, table_info.name, context);
            create_table_internal();
        }

        return;
    }

    // TODO: Check database name change?
    // TODO: Partition table?
    if (storage->getTableName() != table_info.name)
    {
        /// Rename table if needed.
        LOG_DEBUG(log,
            __PRETTY_FUNCTION__ << ": Renaming table " << table_info.db_name << "." << storage->getTableName() << " TO "
                                << table_info.db_name << "." << table_info.name);
        renameTable(table_info.db_name, storage->getTableName(), table_info, context);
    }

    /// Table existing, detect schema changes and apply.
    const TableInfo & orig_table_info = storage->getTableInfo();
    AlterCommands alter_commands = detectSchemaChanges(table_info, orig_table_info);

    std::stringstream ss;
    ss << "Detected schema changes: ";
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
    storage->alterForTMT(alter_commands, table_info, table->table_info.db_name, context);

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Schema changes apply done.");

    // TODO: Apply schema changes to partition tables.
}

} // namespace DB
