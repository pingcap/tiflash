// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/IDatabase.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageLog.h>
#include <common/logger_useful.h>

#include <boost/range/join.hpp>
#include <memory>
#include <string_view>


namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_ALREADY_EXISTS;
extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
extern const int INCORRECT_QUERY;
extern const int ENGINE_REQUIRED;
extern const int TABLE_METADATA_ALREADY_EXISTS;
extern const int UNKNOWN_DATABASE_ENGINE;
extern const int DUPLICATE_COLUMN;
extern const int READONLY;
extern const int DDL_GUARD_IS_ACTIVE;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_between_create_database_meta_and_directory[];
}


InterpreterCreateQuery::InterpreterCreateQuery(
    const ASTPtr & query_ptr_,
    Context & context_,
    std::string_view log_suffix_)
    : query_ptr(query_ptr_)
    , context(context_)
    , log_suffix(log_suffix_)
{}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    String database_name = create.database;

    if (create.if_not_exists && context.isDatabaseExist(database_name))
        return {};

    String database_engine_name;
    if (!create.storage)
    {
        // Keep default database engine as "Ordinary", need to specify "ENGINE = TiFlash" if needed.
        database_engine_name = "Ordinary"; /// Default database engine.
        auto engine = std::make_shared<ASTFunction>();
        engine->name = database_engine_name;
        auto storage = std::make_shared<ASTStorage>();
        storage->set(storage->engine, engine);
        create.set(create.storage, storage);
    }
    else
    {
        const ASTStorage & storage = *create.storage;
        const ASTFunction & engine = *storage.engine;
        /// Currently, only "TiFlash" database engine support arguments.
        if ((engine.name != "TiFlash" && engine.arguments) || engine.parameters || storage.partition_by
            || storage.order_by || storage.sample_by || storage.settings)
        {
            std::stringstream ostr;
            formatAST(storage, ostr, false, false);
            throw Exception("Unknown database engine: " + ostr.str(), ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        }

        database_engine_name = engine.name;
    }

    String database_name_escaped = escapeForFileName(database_name);

    /// Create directories for storing all tables' metadata sql file for this database.
    String path = context.getPath();
    String metadata_path = path + "metadata/" + database_name_escaped + "/";
    DatabasePtr database = DatabaseFactory::get(database_name, metadata_path, create.storage, context);

    /// Will write file with database metadata, if needed.
    String metadata_file_tmp_path = path + "metadata/" + database_name_escaped + ".sql.tmp";
    String metadata_file_path = path + "metadata/" + database_name_escaped + ".sql";

    bool need_write_metadata = !create.attach;

    if (need_write_metadata)
    {
        create.attach = true;
        create.if_not_exists = false;

        std::ostringstream statement_stream;
        formatAST(create, statement_stream, false);
        statement_stream << '\n';
        String statement = statement_stream.str();

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        auto out = WriteBufferFromWritableFileBuilder::build(
            context.getFileProvider(),
            metadata_file_tmp_path,
            EncryptionPath(metadata_file_tmp_path, ""),
            true,
            nullptr,
            statement.size(),
            O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        context.addDatabase(database_name, database);

        if (need_write_metadata)
            context.getFileProvider()->renameFile(
                metadata_file_tmp_path,
                EncryptionPath(metadata_file_tmp_path, ""),
                metadata_file_path,
                EncryptionPath(metadata_file_path, ""),
                true);

        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_between_create_database_meta_and_directory);
        // meta file (not temporary) of database exists means create database success,
        // we need to create meta directory for it if not exists.
        if (auto db_meta_path = Poco::File(database->getMetadataPath()); !db_meta_path.exists())
        {
            db_meta_path.createDirectory();
        }

        database->loadTables(context, thread_pool, has_force_restore_data_flag);
    }
    catch (...)
    {
        if (context.tryGetDatabase(database_name) != nullptr)
        {
            // We need to detach database from context
            context.detachDatabase(database_name);
        }

        if (need_write_metadata)
        {
            if (auto file = Poco::File(metadata_file_tmp_path); file.exists())
                file.remove();
        }

        throw;
    }

    return {};
}


using ColumnsAndDefaults = std::pair<NamesAndTypesList, ColumnDefaults>;

/// AST to the list of columns with types. Columns of Nested type are expanded into a list of real columns.
static ColumnsAndDefaults parseColumns(const ASTExpressionList & column_list_ast, const Context & context)
{
    /// list of table columns in correct order
    NamesAndTypesList columns{};
    ColumnDefaults defaults{};

    /// Columns requiring type-deduction or default_expression type-check
    std::vector<std::pair<NameAndTypePair *, ASTColumnDeclaration *>> defaulted_columns{};

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    default_expr_list->children.reserve(column_list_ast.children.size());

    for (const auto & ast : column_list_ast.children)
    {
        auto & col_decl = typeid_cast<ASTColumnDeclaration &>(*ast);

        if (col_decl.type)
        {
            columns.emplace_back(col_decl.name, DataTypeFactory::instance().get(col_decl.type));
        }
        else
            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            columns.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());

        /// add column to postprocessing if there is a default_expression specified
        if (col_decl.default_expression)
        {
            defaulted_columns.emplace_back(&columns.back(), &col_decl);

            /** for columns with explicitly-specified type create two expressions:
             *    1. default_expression aliased as column name with _tmp suffix
             *    2. conversion of expression (1) to explicitly-specified type alias as column name */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp";
                const auto * const data_type_ptr = columns.back().type.get();

                default_expr_list->children.emplace_back(setAlias(
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTIdentifier>(tmp_column_name),
                        std::make_shared<ASTLiteral>(Field(data_type_ptr->getName()))),
                    final_column_name));
                default_expr_list->children.emplace_back(
                    setAlias(col_decl.default_expression->clone(), tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }
    }

    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!defaulted_columns.empty())
    {
        const auto actions = ExpressionAnalyzer{default_expr_list, context, {}, columns}.getActions(true);
        const auto block = actions->getSampleBlock();

        for (auto & column : defaulted_columns)
        {
            auto * const name_and_type_ptr = column.first;
            auto * const col_decl_ptr = column.second;

            const auto & column_name = col_decl_ptr->name;
            const auto has_explicit_type = nullptr != col_decl_ptr->type;
            auto & explicit_type = name_and_type_ptr->type;

            /// if column declaration contains explicit type, name_and_type_ptr->type is not null
            if (has_explicit_type)
            {
                const auto & tmp_column = block.getByName(column_name + "_tmp");
                const auto & deduced_type = tmp_column.type;

                /// type mismatch between explicitly specified and deduced type, add conversion for non-array types
                if (!explicit_type->equals(*deduced_type))
                {
                    col_decl_ptr->default_expression = makeASTFunction(
                        "CAST",
                        col_decl_ptr->default_expression,
                        std::make_shared<ASTLiteral>(explicit_type->getName()));

                    col_decl_ptr->children.clear();
                    col_decl_ptr->children.push_back(col_decl_ptr->type);
                    col_decl_ptr->children.push_back(col_decl_ptr->default_expression);
                }
            }
            else
                /// no explicit type, name_and_type_ptr->type is null, set to deduced type
                explicit_type = block.getByName(column_name).type;

            defaults.emplace(
                column_name,
                ColumnDefault{
                    columnDefaultKindFromString(col_decl_ptr->default_specifier),
                    col_decl_ptr->default_expression});
        }
    }

    return {Nested::flatten(columns), defaults};
}


static NamesAndTypesList removeAndReturnColumns(ColumnsAndDefaults & columns_and_defaults, const ColumnDefaultKind kind)
{
    auto & columns = columns_and_defaults.first;
    auto & defaults = columns_and_defaults.second;

    NamesAndTypesList removed{};

    for (auto it = std::begin(columns); it != std::end(columns);)
    {
        const auto jt = defaults.find(it->name);
        if (jt != std::end(defaults) && jt->second.kind == kind)
        {
            removed.push_back(*it);
            it = columns.erase(it);
        }
        else
            ++it;
    }

    return removed;
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column.name;

        StringPtr type_name = std::make_shared<String>(column.type->getName());
        auto * pos = type_name->data();
        auto * const end = pos + type_name->size();

        ParserIdentifierWithOptionalParameters storage_p;
        column_declaration->type = parseQuery(storage_p, pos, end, "data type", 0);
        column_declaration->type->owned_string = type_name;
        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const ColumnsDescription & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns.getAll())
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;

        StringPtr type_name = std::make_shared<String>(column.type->getName());
        auto * pos = type_name->data();
        auto * const end = pos + type_name->size();

        ParserIdentifierWithOptionalParameters storage_p;
        column_declaration->type = parseQuery(storage_p, pos, end, "data type", 0);
        column_declaration->type->owned_string = type_name;

        const auto it = columns.defaults.find(column.name);
        if (it != std::end(columns.defaults))
        {
            column_declaration->default_specifier = toString(it->second.kind);
            column_declaration->default_expression = it->second.expression->clone();
        }

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}


ColumnsDescription InterpreterCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns,
    const Context & context)
{
    ColumnsDescription res;

    auto && columns_and_defaults = parseColumns(columns, context);
    res.materialized = removeAndReturnColumns(columns_and_defaults, ColumnDefaultKind::Materialized);
    res.aliases = removeAndReturnColumns(columns_and_defaults, ColumnDefaultKind::Alias);
    res.ordinary = std::move(columns_and_defaults.first);
    res.defaults = std::move(columns_and_defaults.second);

    if (res.ordinary.size() + res.materialized.size() == 0)
        throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

    return res;
}


ColumnsDescription InterpreterCreateQuery::setColumns(
    ASTCreateQuery & create,
    const Block & as_select_sample,
    const StoragePtr & as_storage) const
{
    ColumnsDescription res;

    if (create.columns)
    {
        res = getColumnsDescription(*create.columns, context);
    }
    else if (!create.as_table.empty())
    {
        res = as_storage->getColumns();
    }
    else if (create.select)
    {
        for (size_t i = 0; i < as_select_sample.columns(); ++i)
            res.ordinary.emplace_back(
                as_select_sample.safeGetByPosition(i).name,
                as_select_sample.safeGetByPosition(i).type);
    }
    else
        throw Exception(
            "Incorrect CREATE query: required list of column descriptions or AS section or SELECT.",
            ErrorCodes::INCORRECT_QUERY);

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    ASTPtr new_columns = formatColumns(res);
    if (create.columns)
        create.replace(create.columns, new_columns);
    else
        create.set(create.columns, new_columns);

    /// Check for duplicates
    std::set<String> all_columns;
    auto check_column_already_exists = [&all_columns](const NameAndTypePair & column_name_and_type) {
        if (!all_columns.emplace(column_name_and_type.name).second)
            throw Exception(
                "Column " + backQuoteIfNeed(column_name_and_type.name) + " already exists",
                ErrorCodes::DUPLICATE_COLUMN);
    };

    for (const auto & elem : res.ordinary)
        check_column_already_exists(elem);
    for (const auto & elem : res.materialized)
        check_column_already_exists(elem);
    for (const auto & elem : res.aliases)
        check_column_already_exists(elem);

    return res;
}

void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.storage)
    {
        return;
    }

    if (!create.as_table.empty())
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = create.as_database.empty() ? context.getCurrentDatabase() : create.as_database;
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = context.getCreateTableQuery(as_database_name, as_table_name);
        const auto & as_create = typeid_cast<const ASTCreateQuery &>(*as_create_ptr);

        create.set(create.storage, as_create.storage->ptr());
    }
}


/**
 * Try to acquire a DDLGuard to execute the "CREATE TABLE" actions.
 *
 * Return the gurad if this thread become the owner to execute "CREATE TABLE".
 * If the thread does not is the owner to execute "CREATE TABLE".
 *   - If the table already exists, and the request specifies IF NOT EXISTS,
 *     then we allow concurrent CREATE queries (which do nothing).
 *   - Otherwise, concurrent queries for creating a table, if the table does not exist,
 *     wait for `timeout_seconds` at max to check whether the table creation is completly
 *     created. If the table has been created within timeout, then do nothing and return.
 *     If timeout happen at last, throw an exception.
 */
std::unique_ptr<DDLGuard> tryGetDDLGuard(
    Context & context,
    const String & database_name,
    const String & table_name,
    bool create_if_not_exists,
    size_t timeout_seconds,
    std::string_view log_suffix)
{
    constexpr int wait_useconds = 50'000;
    const size_t max_retries = timeout_seconds * 1'000'000 / wait_useconds;
    try
    {
        auto guard = context.getDDLGuardIfTableDoesntExist(
            database_name,
            table_name,
            "Table " + database_name + "." + table_name + " is creating or attaching right now");

        if (!guard)
        {
            if (create_if_not_exists)
                return {}; // return a null guard
            else
                throw Exception(
                    "Table " + database_name + "." + table_name + " already exists.",
                    ErrorCodes::TABLE_ALREADY_EXISTS);
        }
        return guard;
    }
    catch (Exception & e)
    {
        // Concurrent queries for creating the same table may run into this branch.
        // We have to wait for the table created completely, then return to use the table.
        // Thus, we choose to do a retry here to wait the table created completed.
        if (e.code() == ErrorCodes::TABLE_ALREADY_EXISTS || e.code() == ErrorCodes::DDL_GUARD_IS_ACTIVE)
        {
            auto log = Logger::get(log_suffix);
            LOG_WARNING(log, "Concurrent create table happens, error_code={} error_msg={}", e.code(), e.message());
            for (size_t i = 0; i < max_retries; ++i)
            {
                // Once we can get the table from `context`, consider the table create has been "completed"
                // and return a null guard
                if (context.isTableExist(database_name, table_name))
                    return {};

                // sleep a while and retry
                LOG_WARNING(
                    log,
                    "Waiting for the completion of concurrent table creation action"
                    ", sleep for {} ms and try again",
                    wait_useconds / 1000);
                usleep(wait_useconds);
            }

            // timeout, throw an exception
            LOG_ERROR(
                log,
                "still failed to wait for the completion of concurrent table creation in InterpreterCreateQuery, "
                "max_retries={} stack_info={}",
                max_retries,
                e.getStackTrace().toString());
            e.rethrow();
        }
        else
        {
            e.addMessage(std::string(log_suffix));
            e.rethrow();
        }
    }
    return {}; // not reachable
}

BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    String database_name = create.database.empty() ? current_database : create.database;
    String table_name = create.table;
    String table_name_escaped = escapeForFileName(table_name);

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns)
    {
        // Table SQL definition is available even if the table is detached
        auto query = context.getCreateTableQuery(database_name, table_name);
        const auto & as_create = typeid_cast<const ASTCreateQuery &>(*query);
        create = as_create; // Copy the saved create query, but use ATTACH instead of CREATE
        create.attach = true;
    }

    if (create.to_database.empty())
        create.to_database = current_database;

    Block as_select_sample;
    if (create.select && (!create.attach || !create.columns))
        as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), context);

    String as_database_name = create.as_database.empty() ? current_database : create.as_database;
    String as_table_name = create.as_table;

    StoragePtr as_storage;
    TableStructureLockHolder as_storage_lock;
    if (!as_table_name.empty())
    {
        as_storage = context.getTable(as_database_name, as_table_name);
        as_storage_lock = as_storage->lockStructureForShare(context.getCurrentQueryId());
    }

    /// Set and retrieve list of columns.
    ColumnsDescription columns = setColumns(create, as_select_sample, as_storage);

    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    {
        std::unique_ptr<DDLGuard> guard;

        DatabasePtr database = context.getDatabase(database_name);
        String data_path = database->getDataPath();

        guard = tryGetDDLGuard(
            context,
            database_name,
            table_name,
            create.if_not_exists,
            /*timeout_seconds=*/5,
            log_suffix);
        if (!guard)
        {
                // Not the owner to create IStorage instance, and the table is created
                // completely, let's return
                return {};
        }

        // Guard is acquired, let's create the IStorage instance
        StoragePtr res = StorageFactory::instance().get(
            create,
            data_path,
            table_name,
            database_name,
            database->getEngineName(),
            context,
            context.getGlobalContext(),
            columns,
            create.attach,
            false);

        // When creating tables, we should strictly follow the following steps:
        // 1. Create a .sql file
        // 2. Register the instance to ManagedStorages
        // 3. Register the instance to IDatabase
        // Once the instance is registered in `ManagedStorages`, we will try to apply DDL alter changes to its .sql files
        // If we do step 2 before step 1, we may run into "can't find .sql file" error when applying DDL jobs.
        // Besides, we make step 3 the final one, to ensure once we pass the check of context.isTableExist(database_name, table_name)`, the table must be created completely.

        database->createTable(context, table_name, query_ptr);

        // register the storage instance into `ManagedStorages`
        res->startup();

        // the table has been created completely
        database->attachTable(table_name, res);
    }

    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.select && !create.attach && create.is_populate)
    {
        auto insert = std::make_shared<ASTInsertQuery>();

        insert->database = database_name;

        insert->table = table_name;
        insert->select = create.select->clone();

        return InterpreterInsertQuery(insert, context, false).execute();
    }

    return {};
}


BlockIO InterpreterCreateQuery::execute()
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_ptr);
    checkAccess(create);
    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
    {
        return createDatabase(create);
    }
    else
        return createTable(create);
}


void InterpreterCreateQuery::checkAccess(const ASTCreateQuery & create)
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return;

    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    if (!readonly)
    {
        return;
    }

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
    {
        throw Exception("Cannot create database in readonly mode", ErrorCodes::READONLY);
    }

    throw Exception("Cannot create table in readonly mode", ErrorCodes::READONLY);
}
} // namespace DB
