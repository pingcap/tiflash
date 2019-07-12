#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/MutableSupport.h>
#include <IO/WriteHelpers.h>

namespace DB {

using TableInfo = TiDB::TableInfo;
using DBInfo = TiDB::DBInfo;
using TableInfoPtr = TiDB::TableInfoPtr;
using DBInfoPtr = TiDB::DBInfoPtr;

inline AlterCommands detectSchemaChanges(const TiDB::TableInfo & table_info, const TiDB::TableInfo & orig_table_info)
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
        const auto & column_info = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const TiDB::ColumnInfo & column_info_) {
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

    return alter_commands;
}

void SchemaBuilder::applyAlterTableImpl(TiDB::TableInfoPtr table_info, StorageMergeTree * storage) {
    auto orig_table_info = storage->getTableInfo();
    auto commands = detectSchemaChanges(*table_info, orig_table_info);

    storage->alterForTMT(commands, *table_info, context);

    if (table_info->is_partition_table) {
        // create partition table.
        for(auto part_def : table_info->partition.definitions) {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id);
            storage->alterForTMT(commands, *new_table_info, context);
        }
    }
}

void SchemaBuilder::applyAlterTable(TiDB::DBInfoPtr dbInfo, Int64 table_id) {

    auto table_info = getter.getTableInfo(dbInfo->id, table_id);
    auto & tmt_context = context.getTMTContext();
    auto storage = static_cast<StorageMergeTree * >(tmt_context.getStorages().get(table_id).get());
    if (storage == nullptr) {
        // TODO throw exception
    }
    applyAlterTableImpl(table_info, storage);
}

void SchemaBuilder::applyDiff(const SchemaDiff & diff) {

    if (diff.type == SchemaActionCreateSchema) {
        applyCreateSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionDropSchema) {
        applyDropSchema(diff.schema_id);
        return;
    }

    auto di = getter.getDatabase(diff.schema_id);

    if (di == nullptr)
        throw Exception("miss database: ", std::to_string(diff.schema_id));

    Int64 oldTableID = 0, newTableID = 0;

    switch (diff.type) {
        case SchemaActionCreateTable:
        case SchemaActionRecoverTable:
        {
            newTableID = diff.table_id;
            break;
        }
        case SchemaActionDropTable:
        case SchemaActionDropView:
        {
            oldTableID = diff.old_table_id;
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
        case SchemaActionRenameTable:
        case SchemaActionSetDefaultValue:
        {
            applyAlterTable(di, diff.table_id);
            break;
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

    if (oldTableID) {
        applyDropTable(di, diff.table_id);
    }

    if (newTableID) {
        applyCreateTable(di, diff.table_id);
    }
}

bool SchemaBuilder::applyCreateSchema(DatabaseID schema_id) {
    auto db = getter.getDatabase(schema_id);
    if (db->name == "") {
        return false;
    }
    applyCreateSchemaImpl(db);
    return true;
}

void SchemaBuilder::applyCreateSchemaImpl(TiDB::DBInfoPtr db_info) {
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

void SchemaBuilder::applyDropSchema(DatabaseID schema_id) {
    auto database_name = databases[schema_id];
    if (database_name == "") {
        return;
    }
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    ASTPtr ast_drop_query = drop_query;
    // It will drop all tables in this database.
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
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

void SchemaBuilder::applyCreatePhysicalTableImpl(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info) {
    String stmt = createTableStmt(*db_info, *table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info->name, 0);

    ASTCreateQuery * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->database = db_info->name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

void SchemaBuilder::applyCreateTable(TiDB::DBInfoPtr db_info, Int64 table_id) {

    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr) {
        // this table is dropped.
        return;
    }
    applyCreateTableImpl(db_info, table_info);
}

void SchemaBuilder::applyCreateTableImpl(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info) {
    if (table_info->is_partition_table) {
        // create partition table.
        for(auto part_def : table_info->partition.definitions) {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id);
            applyCreatePhysicalTableImpl(db_info, new_table_info);
        }
    } else {
        applyCreatePhysicalTableImpl(db_info, table_info);
    }
}

void SchemaBuilder::applyDropTableImpl(const String & database_name, const String & table_name) {
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = database_name;
    drop_query->table = table_name;
    drop_query->if_exists = true;
    ASTPtr ast_drop_query = drop_query;
    InterpreterDropQuery drop_interpreter(ast_drop_query, context);
    drop_interpreter.execute();
}

void SchemaBuilder::applyDropTable(TiDB::DBInfoPtr dbInfo, Int64 table_id) {
    String database_name = dbInfo->name;
    auto & tmt_context = context.getTMTContext();
    const auto & table_info = static_cast<StorageMergeTree * >(tmt_context.getStorages().get(table_id).get())->getTableInfo();
    if (table_info.is_partition_table) {
        // drop all partition tables.
        for(auto part_def : table_info.partition.definitions) {
            auto new_table_info = table_info.producePartitionTableInfo(part_def.id);
            applyDropTableImpl(database_name, new_table_info->name);
        }
    }
    // and drop logic table.
    applyDropTableImpl(database_name, table_info.name);
}

void SchemaBuilder::updateDB(TiDB::DBInfoPtr db_info) {
    auto database_name = databases[db_info->id];
    if (database_name == "") {
        applyCreateSchemaImpl(db_info);
    }
    auto tables = getter.listTables(db_info->id);
    auto & tmt_context = context.getTMTContext();
    for (auto table : tables) {
        auto storage = static_cast<StorageMergeTree * >(tmt_context.getStorages().get(table->id).get());
        if (storage == nullptr) {
            applyCreateTable(db_info, table->id);
        } else {
            applyAlterTableImpl(table, storage);
        }
    }
}

// end namespace
}
