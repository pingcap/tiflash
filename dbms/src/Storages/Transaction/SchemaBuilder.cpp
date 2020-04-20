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

#include <boost/algorithm/string/join.hpp>

namespace DB
{

using namespace TiDB;

namespace ErrorCodes
{
extern const int DDL_ERROR;
}

bool isReservedDatabase(Context & context, const String & database_name)
{
    return context.getTMTContext().getIgnoreDatabases().count(database_name) > 0;
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

using ColumnInfos = std::vector<ColumnInfo>;
using ColumnInfosModifier = std::function<void(ColumnInfos & column_infos)>;
using SchemaChange = std::pair<AlterCommands, ColumnInfosModifier>;
using SchemaChanges = std::vector<SchemaChange>;

/// When schema change detected, the modification to original table info must be preserved as well.
/// With the preserved table info modifications, table info changes along with applying alter commands.
/// In other words, table info and storage structure (altered by applied alter commands) are always identical,
/// and intermediate failure won't hide the outstanding alter commands.
inline SchemaChanges detectSchemaChanges(Logger * log, Context & context, const TableInfo & table_info, const TableInfo & orig_table_info)
{
    SchemaChanges result;

    // add drop commands
    {
        AlterCommands drop_commands;
        std::unordered_set<ColumnID> column_ids_to_drop;
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
                column_ids_to_drop.emplace(orig_column_info.id);
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_drop_column).Increment();
            }
        }
        if (!drop_commands.empty())
        {
            result.emplace_back(std::move(drop_commands), [column_ids_to_drop{std::move(column_ids_to_drop)}](ColumnInfos & column_infos) {
                column_infos.erase(std::remove_if(column_infos.begin(),
                                       column_infos.end(),
                                       [&](const auto & column_info) { return column_ids_to_drop.count(column_info.id) > 0; }),
                    column_infos.end());
            });
        }
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
            auto rename_command
                = newRenameColCommand(rename_pair.first.name, rename_pair.second.name, rename_pair.second.id, orig_table_info);
            auto rename_modifier = [column_id = rename_command.column_id, old_name = rename_command.column_name,
                                       new_name = rename_command.new_column_name](ColumnInfos & column_infos) {
                auto it = std::find_if(column_infos.begin(), column_infos.end(), [&](const auto & column_info) {
                    return column_info.id == column_id && column_info.name == old_name;
                });
                if (it != column_infos.end())
                    it->name = new_name;
            };
            rename_commands.emplace_back(std::move(rename_command));
            result.emplace_back(std::move(rename_commands), rename_modifier);
            GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_rename_column).Increment();
        }
    }

    // alter commands
    {
        AlterCommands alter_commands;
        std::unordered_map<ColumnID, ColumnInfo> alter_map;
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
                alter_map.emplace(column_info->id, *column_info);
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_alter_column_tp).Increment();
            }
        }
        if (!alter_commands.empty())
        {
            result.emplace_back(std::move(alter_commands), [alter_map{std::move(alter_map)}](ColumnInfos & column_infos) {
                std::for_each(column_infos.begin(), column_infos.end(), [&](auto & column_info) {
                    if (auto it = alter_map.find(column_info.id); it != alter_map.end())
                        column_info = it->second;
                });
            });
        }
    }

    {
        AlterCommands add_commands;
        std::vector<ColumnInfo> new_column_infos;
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
                new_column_infos.emplace_back(column_info);
                GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_add_column).Increment();
            }
        }
        if (!add_commands.empty())
        {
            result.emplace_back(std::move(add_commands), [new_column_infos{std::move(new_column_infos)}](ColumnInfos & column_infos) {
                std::for_each(new_column_infos.begin(), new_column_infos.end(), [&](auto & new_column_info) {
                    column_infos.emplace_back(std::move(new_column_info));
                });
            });
        }
    }

    return result;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterPhysicalTable(DBInfoPtr db_info, TableInfoPtr table_info, ManageableStoragePtr storage)
{
    LOG_INFO(log, "Altering table " << name_mapper.displayCanonicalName(*db_info, *table_info));

    /// Detect schema changes.
    auto orig_table_info = storage->getTableInfo();
    auto schema_changes = detectSchemaChanges(log, context, *table_info, orig_table_info);
    if (schema_changes.empty())
    {
        LOG_INFO(
            log, "No schema change detected for table " << name_mapper.displayCanonicalName(*db_info, *table_info) << ", not altering");
        return;
    }

    std::stringstream ss;
    ss << "Detected schema changes: " << name_mapper.displayCanonicalName(*db_info, *table_info) << ": ";
    for (const auto & schema_change : schema_changes)
        for (const auto & command : schema_change.first)
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

    /// Update metadata, through calling alterFromTiDB.
    // Using original table info with updated columns instead of using new_table_info directly,
    // so that other changes (RENAME commands) won't be saved.
    // Also, updating schema_version as altering column is structural.
    for (const auto & schema_change : schema_changes)
    {
        /// Update column infos by applying schema change in this step.
        schema_change.second(orig_table_info.columns);
        /// Update schema version aggressively for the sake of correctness.
        orig_table_info.schema_version = target_version;
        storage->alterFromTiDB(schema_change.first, name_mapper.mapDatabaseName(*db_info), orig_table_info, name_mapper, context);
    }

    LOG_INFO(log, "Altered table " << name_mapper.displayCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterTable(DBInfoPtr db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        throw Exception("miss table in TiKV : " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
    {
        throw Exception("miss table in TiFlash : " + name_mapper.displayCanonicalName(*db_info, *table_info), ErrorCodes::DDL_ERROR);
    }

    applyAlterLogicalTable(db_info, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterLogicalTable(DBInfoPtr db_info, TableInfoPtr table_info, ManageableStoragePtr storage)
{
    // Alter logical table first.
    applyAlterPhysicalTable(db_info, table_info, storage);

    if (table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();

        // Alter physical tables of a partition table.
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage == nullptr)
            {
                throw Exception("miss table in TiFlash : " + name_mapper.displayCanonicalName(*db_info, *table_info)
                        + " partition: " + std::to_string(part_def.id),
                    ErrorCodes::DDL_ERROR);
            }
            applyAlterPhysicalTable(db_info, part_table_info, part_storage);
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
    if (!table_info->isLogicalPartitionTable())
    {
        throw Exception(
            "new table in TiKV not partition table " + name_mapper.displayCanonicalName(*db_info, *table_info), ErrorCodes::DDL_ERROR);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
    {
        throw Exception("miss table in TiFlash " + std::to_string(table_id), ErrorCodes::DDL_ERROR);
    }

    applyPartitionDiff(db_info, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(TiDB::DBInfoPtr db_info, TableInfoPtr table_info, ManageableStoragePtr storage)
{
    const auto & orig_table_info = storage->getTableInfo();
    if (!orig_table_info.isLogicalPartitionTable())
    {
        throw Exception("old table in TiFlash not partition table " + name_mapper.displayCanonicalName(*db_info, orig_table_info),
            ErrorCodes::DDL_ERROR);
    }

    const auto & orig_defs = orig_table_info.partition.definitions;
    const auto & new_defs = table_info->partition.definitions;

    std::unordered_set<TableID> orig_part_id_set, new_part_id_set;
    std::vector<String> orig_part_ids, new_part_ids;
    std::for_each(orig_defs.begin(), orig_defs.end(), [&orig_part_id_set, &orig_part_ids](const auto & def) {
        orig_part_id_set.emplace(def.id);
        orig_part_ids.emplace_back(std::to_string(def.id));
    });
    std::for_each(new_defs.begin(), new_defs.end(), [&new_part_id_set, &new_part_ids](const auto & def) {
        new_part_id_set.emplace(def.id);
        new_part_ids.emplace_back(std::to_string(def.id));
    });

    auto orig_part_ids_str = boost::algorithm::join(orig_part_ids, ", ");
    auto new_part_ids_str = boost::algorithm::join(new_part_ids, ", ");

    LOG_INFO(log,
        "Applying partition changes " << name_mapper.displayCanonicalName(*db_info, *table_info) << " old: " << orig_part_ids_str
                                      << " new: " << new_part_ids_str);

    if (orig_part_id_set == new_part_id_set)
    {
        LOG_INFO(log, "No partition changes " << name_mapper.displayCanonicalName(*db_info, *table_info));
        return;
    }

    /// Create new table info based on original table info.
    // Using copy of original table info with updated table name instead of using new_table_info directly,
    // so that other changes (ALTER/RENAME commands) won't be saved.
    // Besides, no need to update schema_version as partition change is not structural.
    auto updated_table_info = orig_table_info;
    updated_table_info.partition = table_info->partition;

    /// Apply changes to physical tables.
    for (auto orig_def : orig_defs)
    {
        if (new_part_id_set.count(orig_def.id) == 0)
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), orig_def.id);
        }
    }
    for (auto new_def : new_defs)
    {
        if (orig_part_id_set.count(new_def.id) == 0)
        {
            auto part_table_info = updated_table_info.producePartitionTableInfo(new_def.id, name_mapper);
            applyCreatePhysicalTable(db_info, part_table_info);
        }
    }

    /// Apply new table info to logical table.
    storage->alterFromTiDB(AlterCommands{}, name_mapper.mapDatabaseName(*db_info), updated_table_info, name_mapper, context);

    LOG_INFO(log, "Applied partition changes " << name_mapper.displayCanonicalName(*db_info, *table_info));
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

    applyRenameLogicalTable(new_db_info, new_table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameLogicalTable(
    DBInfoPtr new_db_info, TableInfoPtr new_table_info, ManageableStoragePtr storage)
{
    applyRenamePhysicalTable(new_db_info, new_table_info, storage);

    if (new_table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage == nullptr)
            {
                throw Exception("miss old table id in Flash " + std::to_string(part_def.id));
            }
            auto part_table_info = new_table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRenamePhysicalTable(new_db_info, part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenamePhysicalTable(
    DBInfoPtr new_db_info, TableInfoPtr new_table_info, ManageableStoragePtr storage)
{
    auto old_db_name = storage->getDatabaseName();
    const auto & old_table_name = storage->getTableInfo().name;
    if (old_db_name == name_mapper.mapDatabaseName(*new_db_info) && old_table_name == new_table_info->name)
    {
        LOG_DEBUG(log, "Table " << name_mapper.displayCanonicalName(*new_db_info, *new_table_info) << " name identical, not renaming.");
        return;
    }

    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_rename_column).Increment();
    LOG_INFO(log,
        "Renaming table " << old_db_name << "." << old_table_name << " to "
                          << name_mapper.displayCanonicalName(*new_db_info, *new_table_info));

    if (old_db_name != name_mapper.mapDatabaseName(*new_db_info)
        || storage->getTableName() != name_mapper.mapTableName(*new_table_info) /* this condition only holds for mock tests */)
    {
        /// Renaming table across databases (or renaming for mock tests), we issue a RENAME query to move data around.
        // TODO: This whole if branch won't be needed once we flattened the data path.
        auto rename = std::make_shared<ASTRenameQuery>();

        ASTRenameQuery::Table from;
        from.database = old_db_name;
        from.table = old_table_name;

        ASTRenameQuery::Table to;
        to.database = name_mapper.mapDatabaseName(*new_db_info);
        to.table = name_mapper.mapTableName(*new_table_info);

        ASTRenameQuery::Element elem;
        elem.from = from;
        elem.to = to;

        rename->elements.emplace_back(elem);

        InterpreterRenameQuery(rename, context).execute();
    }

    /// Update metadata, through calling alterFromTiDB.
    // Using copy of original table info with updated table name instead of using new_table_info directly,
    // so that other changes (ALTER commands) won't be saved.
    // Besides, no need to update schema_version as table name is not structural.
    auto updated_table_info = storage->getTableInfo();
    updated_table_info.name = new_table_info->name;
    storage->alterFromTiDB(AlterCommands{}, name_mapper.mapDatabaseName(*new_db_info), updated_table_info, name_mapper, context);

    LOG_INFO(log,
        "Renamed table " << old_db_name << "." << old_table_name << " to "
                         << name_mapper.displayCanonicalName(*new_db_info, *new_table_info));
}

template <typename Getter, typename NameMapper>
bool SchemaBuilder<Getter, NameMapper>::applyCreateSchema(DatabaseID schema_id)
{
    auto db = getter.getDatabase(schema_id);
    if (db == nullptr)
    {
        return false;
    }
    applyCreateSchema(db);
    return true;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateSchema(TiDB::DBInfoPtr db_info)
{
    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_create_db).Increment();
    LOG_INFO(log, "Creating database " << name_mapper.displayDatabaseName(*db_info));
    auto mapped = name_mapper.mapDatabaseName(*db_info);
    if (isReservedDatabase(context, mapped))
        throw Exception("Database " + name_mapper.displayDatabaseName(*db_info) + " is reserved", ErrorCodes::DDL_ERROR);
    ASTCreateQuery * create_query = new ASTCreateQuery();
    create_query->database = std::move(mapped);
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
    applyDropSchema(name_mapper.mapDatabaseName(*it->second));
    databases.erase(schema_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(const String & schema_name)
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
void SchemaBuilder<Getter, NameMapper>::applyCreatePhysicalTable(DBInfoPtr db_info, TableInfoPtr table_info)
{
    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(log, "Creating table " << name_mapper.displayCanonicalName(*db_info, *table_info));

    /// Update schema version.
    table_info->schema_version = target_version;

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto storage = tmt_context.getStorages().get(table_info->id).get(); storage)
        {
            if (!storage->isTombstone())
            {
                LOG_DEBUG(log,
                    "Trying to create table " << name_mapper.displayCanonicalName(*db_info, *table_info)
                                              << " but it already exists and is not marked as tombstone");
                return;
            }

            LOG_DEBUG(log, "Recovering table " << name_mapper.displayCanonicalName(*db_info, *table_info));
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            storage->alterFromTiDB(commands, name_mapper.mapDatabaseName(*db_info), *table_info, name_mapper, context);
            LOG_INFO(log, "Created table " << name_mapper.displayCanonicalName(*db_info, *table_info));
            return;
        }
    }

    /// Normal CREATE table.
    if (table_info->engine_type == StorageEngine::UNSPECIFIED)
    {
        auto & tmt_context = context.getTMTContext();
        table_info->engine_type = tmt_context.getEngineType();
    }

    String stmt = createTableStmt(*db_info, *table_info, name_mapper, log);

    LOG_INFO(log, "Creating table " << name_mapper.displayCanonicalName(*db_info, *table_info) << " with statement: " << stmt);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info->name, 0);

    ASTCreateQuery * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->if_not_exists = true;
    ast_create_query->database = name_mapper.mapDatabaseName(*db_info);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
    LOG_INFO(log, "Created table " << name_mapper.displayCanonicalName(*db_info, *table_info));
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

    applyCreateLogicalTable(db_info, table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateLogicalTable(TiDB::DBInfoPtr db_info, TableInfoPtr table_info)
{
    if (table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyCreatePhysicalTable(db_info, new_table_info);
        }
    }

    // Create logical table at last, only logical table creation will be treated as "complete".
    // Intermediate failure will hide the logical table creation so that schema syncing when restart will re-create all (despite some physical tables may have created).
    applyCreatePhysicalTable(db_info, table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropPhysicalTable(const String & db_name, TableID table_id)
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
        // We don't try to get a precise time that TiDB drops this table.
        // We use a more relaxing GC strategy:
        // 1. Use current timestamp, which is after TiDB's drop time, to be the tombstone of this table;
        // 2. Use the same GC safe point as TiDB.
        // In such way our table will be GC-ed later than TiDB, which is safe and correct.
        command.tombstone = tmt_context.getPDClient()->getTS();
        commands.emplace_back(std::move(command));
    }
    storage->alterFromTiDB(commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(log, "Tombstoned table " << db_name << "." << name_mapper.displayTableName(storage->getTableInfo()));
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
        for (const auto & part_def : table_info.partition.definitions)
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), part_def.id);
        }
    }

    // Drop logical table at last, only logical table drop will be treated as "complete".
    // Intermediate failure will hide the logical table drop so that schema syncing when restart will re-drop all (despite some physical tables may have dropped).
    applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), table_info.id);
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
            applyCreateSchema(db);
            LOG_DEBUG(log, "Database " << name_mapper.displayDatabaseName(*db) << " created during sync all schemas");
        }
    }

    /// Load all tables in each database.
    std::unordered_set<TableID> table_set;
    for (const auto & db : all_schemas)
    {
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto & table : tables)
        {
            LOG_DEBUG(log, "Table " << name_mapper.displayCanonicalName(*db, *table) << " syncing during sync all schemas");

            /// Ignore view and sequence.
            if (table->is_view || table->is_sequence)
            {
                LOG_INFO(log, "Table " << name_mapper.displayCanonicalName(*db, *table) << " is a view or sequence, ignoring.");
                continue;
            }

            /// Record for further detecting tables to drop.
            table_set.emplace(table->id);
            if (table->isLogicalPartitionTable())
            {
                std::for_each(table->partition.definitions.begin(), table->partition.definitions.end(), [&table_set](const auto & def) {
                    table_set.emplace(def.id);
                });
            }

            auto storage = tmt_context.getStorages().get(table->id);
            if (storage == nullptr)
            {
                /// Create if not exists.
                applyCreateLogicalTable(db, table);
                storage = tmt_context.getStorages().get(table->id);
                if (storage == nullptr)
                {
                    /// This is abnormal as the storage shouldn't be null after creation, the underlying table must already be existing for unknown reason.
                    LOG_WARNING(log,
                        "Table " << name_mapper.displayCanonicalName(*db, *table)
                                 << " not synced because may have been dropped during sync all schemas");
                    continue;
                }
            }
            if (table->isLogicalPartitionTable())
            {
                /// Apply partition diff if needed.
                applyPartitionDiff(db, table, storage);
            }
            /// Rename if needed.
            applyRenameLogicalTable(db, table, storage);
            /// Alter if needed.
            applyAlterLogicalTable(db, table, storage);
            LOG_DEBUG(log, "Table " << name_mapper.displayCanonicalName(*db, *table) << " synced during sync all schemas");
        }
        LOG_DEBUG(log, "Database " << name_mapper.displayDatabaseName(*db) << " synced during sync all schemas");
    }

    /// Drop all unmapped tables.
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        if (table_set.count(it->first) == 0)
        {
            applyDropPhysicalTable(it->second->getDatabaseName(), it->first);
            LOG_DEBUG(log,
                "Table " << it->second->getDatabaseName() << "." << name_mapper.displayTableName(it->second->getTableInfo())
                         << " dropped during sync all schemas");
        }
    }

    /// Drop all unmapped dbs.
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        if (db_set.count(it->first) == 0 && !isReservedDatabase(context, it->first))
        {
            applyDropSchema(it->first);
            LOG_DEBUG(log, "DB " << it->first << " dropped during sync all schemas");
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;

// end namespace
} // namespace DB
