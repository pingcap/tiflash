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
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseTiFlash.h>
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
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>
#include <TiDB/Schema/SchemaBuilder-internal.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/SchemaGetter.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>
#include <tuple>

namespace DB
{
using namespace TiDB;

namespace ErrorCodes
{
extern const int DDL_ERROR;
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char exception_after_step_1_in_exchange_partition[];
extern const char exception_before_step_2_rename_in_exchange_partition[];
extern const char exception_after_step_2_in_exchange_partition[];
extern const char exception_before_step_3_rename_in_exchange_partition[];
extern const char exception_after_step_3_in_exchange_partition[];
extern const char exception_between_schema_change_in_the_same_diff[];
} // namespace FailPoints

bool isReservedDatabase(Context & context, const String & database_name)
{
    return context.getTMTContext().getIgnoreDatabases().count(database_name) > 0;
}


inline void setAlterCommandColumn(const LoggerPtr & log, AlterCommand & command, const ColumnInfo & column_info)
{
    command.column_name = column_info.name;
    command.column_id = column_info.id;
    command.data_type = getDataTypeByColumnInfo(column_info);
    if (!column_info.origin_default_value.isEmpty())
    {
        LOG_DEBUG(log, "add default value for column: {}", column_info.name);
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

using TableInfoModifier = std::function<void(TableInfo & table_info)>;
using SchemaChange = std::pair<AlterCommands, TableInfoModifier>;
using SchemaChanges = std::vector<SchemaChange>;

bool typeDiffers(const TiDB::ColumnInfo & a, const TiDB::ColumnInfo & b)
{
    if (a.tp != b.tp || a.hasNotNullFlag() != b.hasNotNullFlag() || a.hasUnsignedFlag() != b.hasUnsignedFlag())
        return true;
    if (a.tp == TypeEnum || a.tp == TypeSet)
    {
        if (a.elems.size() != b.elems.size())
            return true;
        for (size_t i = 0; i < a.elems.size(); i++)
        {
            if (a.elems[i].first != b.elems[i].first)
                return true;
        }
        return false;
    }
    else if (a.tp == TypeNewDecimal)
    {
        return a.flen != b.flen || a.decimal != b.decimal;
    }
    else if (a.tp == TiDB::TypeDatetime || a.tp == TiDB::TypeDate || a.tp == TiDB::TypeTimestamp || a.tp == TiDB::TypeTime)
    {
        // detect fsp changed in MyDateTime/MyTime/MyDuration (`Datetime`/`TIMESTAMP`/`TIME` in TiDB)
        return a.flen != b.flen || a.decimal != b.decimal;
    }
    return false;
}

/// When schema change detected, the modification to original table info must be preserved as well.
/// With the preserved table info modifications, table info changes along with applying alter commands.
/// In other words, table info and storage structure (altered by applied alter commands) are always identical,
/// and intermediate failure won't hide the outstanding alter commands.
inline SchemaChanges detectSchemaChanges(
    const LoggerPtr & log,
    const TableInfo & table_info,
    const TableInfo & orig_table_info)
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
                GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_column).Increment();
            }
        }
        if (!drop_commands.empty())
        {
            result.emplace_back(std::move(drop_commands), [column_ids_to_drop{std::move(column_ids_to_drop)}](TableInfo & table_info) {
                auto & column_infos = table_info.columns;
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
            auto rename_modifier
                = [column_id = rename_command.column_id, old_name = rename_command.column_name, new_name = rename_command.new_column_name](
                      TableInfo & table_info) {
                      auto & column_infos = table_info.columns;
                      auto it = std::find_if(column_infos.begin(), column_infos.end(), [&](const auto & column_info) {
                          return column_info.id == column_id && column_info.name == old_name;
                      });
                      if (it != column_infos.end())
                          it->name = new_name;
                      if (table_info.is_common_handle)
                      {
                          /// TiDB only saves column name(not column id) in index info, so have to update primary
                          /// index info when rename column
                          auto & index_info = table_info.getPrimaryIndexInfo();
                          for (auto & col : index_info.idx_cols)
                          {
                              if (col.name == old_name)
                              {
                                  col.name = new_name;
                                  break;
                              }
                          }
                      }
                  };
            rename_commands.emplace_back(std::move(rename_command));
            result.emplace_back(std::move(rename_commands), rename_modifier);
            GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_column).Increment();
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
                          LOG_INFO(log, "detect column {} rename to {}", orig_column_info.name, column_info_.name);

                      return column_info_.id == orig_column_info.id && typeDiffers(column_info_, orig_column_info);
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
                GET_METRIC(tiflash_schema_internal_ddl_count, type_alter_column_tp).Increment();
            }
        }
        if (!alter_commands.empty())
        {
            result.emplace_back(std::move(alter_commands), [alter_map{std::move(alter_map)}](TableInfo & table_info) {
                auto & column_infos = table_info.columns;
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
                GET_METRIC(tiflash_schema_internal_ddl_count, type_add_column).Increment();
            }
        }
        if (!add_commands.empty())
        {
            result.emplace_back(std::move(add_commands), [new_column_infos{std::move(new_column_infos)}](TableInfo & table_info) {
                auto & column_infos = table_info.columns;
                std::for_each(new_column_infos.begin(), new_column_infos.end(), [&](auto & new_column_info) {
                    column_infos.emplace_back(std::move(new_column_info));
                });
            });
        }
    }

    return result;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterPhysicalTable(const DBInfoPtr & db_info, const TableInfoPtr & table_info, const ManageableStoragePtr & storage)
{
    LOG_INFO(log, "Altering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));

    /// Detect schema changes.
    auto orig_table_info = storage->getTableInfo();
    auto schema_changes = detectSchemaChanges(log, *table_info, orig_table_info);
    if (schema_changes.empty())
    {
        LOG_INFO(log, "No schema change detected for table {}, not altering", name_mapper.debugCanonicalName(*db_info, *table_info));
        return;
    }

    auto log_str = [&]() {
        FmtBuffer fmt_buf;
        fmt_buf.fmtAppend("Detected schema changes: {}: ", name_mapper.debugCanonicalName(*db_info, *table_info));
        for (const auto & schema_change : schema_changes)
        {
            for (const auto & command : schema_change.first)
            {
                if (command.type == AlterCommand::ADD_COLUMN)
                    fmt_buf.fmtAppend("ADD COLUMN {} {},", command.column_name, command.data_type->getName());
                else if (command.type == AlterCommand::DROP_COLUMN)
                    fmt_buf.fmtAppend("DROP COLUMN {}, ", command.column_name);
                else if (command.type == AlterCommand::MODIFY_COLUMN)
                    fmt_buf.fmtAppend("MODIFY COLUMN {} {}, ", command.column_name, command.data_type->getName());
                else if (command.type == AlterCommand::RENAME_COLUMN)
                    fmt_buf.fmtAppend("RENAME COLUMN from {} to {}, ", command.column_name, command.new_column_name);
            }
        }
        return fmt_buf.toString();
    };
    LOG_INFO(log, log_str());

    /// Update metadata, through calling alterFromTiDB.
    // Using original table info with updated columns instead of using new_table_info directly,
    // so that other changes (RENAME commands) won't be saved.
    // Also, updating schema_version as altering column is structural.
    for (size_t i = 0; i < schema_changes.size(); i++)
    {
        if (i > 0)
        {
            /// If there are multiple schema change in the same diff,
            /// the table schema version will be set to the latest schema version after the first schema change is applied.
            /// Throw exception in the middle of the schema change to mock the case that there is a race between data decoding and applying different schema change.
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_between_schema_change_in_the_same_diff);
        }
        const auto & schema_change = schema_changes[i];
        /// Update column infos by applying schema change in this step.
        schema_change.second(orig_table_info);
        /// Update schema version aggressively for the sake of correctness（for read part).
        /// In read action, we will use table_info.schema_version(storage_version) and TiDBSchemaSyncer.cur_version(global_version) to compare with query_version, to decide whether we can read under this query_version, or we need to make the schema newer.
        /// In our comparison logic, we only serve the query when the query schema version meet the criterion: storage_version <= query_version <= global_version(The more detail info you can refer the comments in DAGStorageInterpreter::getAndLockStorages.)
        /// And when apply multi diffs here, we only update global_version when all diffs have been applied.
        /// So the global_version may be less than the actual "global_version" of the local schema in the process of applying schema changes.
        /// And if we don't update the storage_version ahead of time, we may meet the following case when apply multiple diffs: storage_version <= global_version < actual "global_version".
        /// If we receive a query with the same version as global_version, we can have the following scenario: storage_version <= global_version == query_version < actual "global_version".
        /// And because storage_version <= global_version == query_version meet the criterion of serving the query, the query will be served. But query_version < actual "global_version" indicates that we use a newer schema to server an older query which may cause some inconsistency issue.
        /// So we update storage_version aggressively to prevent the above scenario happens.
        orig_table_info.schema_version = target_version;
        auto alter_lock = storage->lockForAlter(getThreadName());
        storage->alterFromTiDB(
            alter_lock,
            schema_change.first,
            name_mapper.mapDatabaseName(*db_info),
            orig_table_info,
            name_mapper,
            context);
    }

    LOG_INFO(log, "Altered table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterTable(const DBInfoPtr & db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        throw TiFlashException(fmt::format("miss table in TiKV : {}", table_id), Errors::DDL::StaleSchema);
    }
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
    {
        throw TiFlashException(fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*db_info, *table_info)),
                               Errors::DDL::MissingTable);
    }

    applyAlterLogicalTable(db_info, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyAlterLogicalTable(const DBInfoPtr & db_info, const TableInfoPtr & table_info, const ManageableStoragePtr & storage)
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
                throw TiFlashException(fmt::format("miss table in TiFlash : {},  partition: {}.", name_mapper.debugCanonicalName(*db_info, *table_info), part_def.id),
                                       Errors::DDL::MissingTable);
            }
            applyAlterPhysicalTable(db_info, part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDiff(const SchemaDiff & diff)
{
    if (diff.type == SchemaActionType::CreateSchema)
    {
        applyCreateSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionType::DropSchema)
    {
        applyDropSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionType::ActionRecoverSchema)
    {
        applyRecoverSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionType::CreateTables)
    {
        for (auto && opt : diff.affected_opts)
        {
            SchemaDiff new_diff;
            new_diff.type = SchemaActionType::CreateTable;
            new_diff.version = diff.version;
            new_diff.schema_id = opt.schema_id;
            new_diff.table_id = opt.table_id;
            new_diff.old_schema_id = opt.old_schema_id;
            new_diff.old_table_id = opt.old_table_id;
            applyDiff(new_diff);
        }
        return;
    }

    if (diff.type == SchemaActionType::RenameTables)
    {
        for (auto && opt : diff.affected_opts)
        {
            auto db_info = getter.getDatabase(opt.schema_id);
            if (db_info == nullptr)
                throw TiFlashException("miss database: " + std::to_string(diff.schema_id), Errors::DDL::StaleSchema);
            applyRenameTable(db_info, opt.table_id, "RenameTables");
        }
        return;
    }

    auto db_info = getter.getDatabase(diff.schema_id);
    if (db_info == nullptr)
    {
        throw TiFlashException(fmt::format("miss database: {}", diff.schema_id), Errors::DDL::StaleSchema);
    }

    TableID old_table_id = 0, new_table_id = 0;

    switch (diff.type)
    {
    case SchemaActionType::CreateTable:
    case SchemaActionType::RecoverTable:
    {
        new_table_id = diff.table_id;
        break;
    }
    case SchemaActionType::DropTable:
    case SchemaActionType::DropView:
    {
        old_table_id = diff.table_id;
        break;
    }
    case SchemaActionType::TruncateTable:
    {
        new_table_id = diff.table_id;
        old_table_id = diff.old_table_id;
        break;
    }
    case SchemaActionType::AddColumn:
    case SchemaActionType::AddColumns:
    case SchemaActionType::DropColumn:
    case SchemaActionType::DropColumns:
    case SchemaActionType::ModifyColumn:
    case SchemaActionType::SetDefaultValue:
    // Add primary key change primary keys to not null, so it's equal to alter table for tiflash.
    case SchemaActionType::AddPrimaryKey:
    {
        applyAlterTable(db_info, diff.table_id);
        break;
    }
    case SchemaActionType::RenameTable:
    {
        applyRenameTable(db_info, diff.table_id, "RenameTable");
        break;
    }
    case SchemaActionType::AddTablePartition:
    case SchemaActionType::DropTablePartition:
    case SchemaActionType::TruncateTablePartition:
    {
        applyPartitionDiff(db_info, diff.table_id);
        break;
    }
    case SchemaActionType::ExchangeTablePartition:
    {
        applyExchangeTablePartition(diff);
        break;
    }
    case SchemaActionType::SetTiFlashReplica:
    {
        applySetTiFlashReplica(db_info, diff.table_id);
        break;
    }
    default:
    {
        if (diff.type < SchemaActionType::MaxRecognizedType)
        {
            LOG_INFO(log, "Ignore change type: {}", int(diff.type));
        }
        else
        { // >= SchemaActionType::MaxRecognizedType
            LOG_ERROR(log, "Unsupported change type: {}", int(diff.type));
        }

        break;
    }
    }

    if (old_table_id)
    {
        String action = fmt::format("{}", magic_enum::enum_name(diff.type));
        applyDropTable(db_info, old_table_id, action);
    }

    if (new_table_id)
    {
        applyCreateTable(db_info, new_table_id);
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(const TiDB::DBInfoPtr & db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        throw TiFlashException(fmt::format("miss old table id in TiKV when applyPartitionDiff, table_id={}", table_id), Errors::DDL::StaleSchema);
    }
    if (!table_info->isLogicalPartitionTable())
    {
        throw TiFlashException(fmt::format("new table in TiKV not partition table when applyPartitionDiff, {}", name_mapper.debugCanonicalName(*db_info, *table_info)),
                               Errors::DDL::TableTypeNotMatch);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
    {
        throw TiFlashException(fmt::format("miss table in TiFlash when applyPartitionDiff, table_id={}", table_id), Errors::DDL::MissingTable);
    }

    applyPartitionDiffOnLogicalTable(db_info, table_info, storage, /*drop_part_if_not_exist*/ true);
}

template <typename Getter, typename NameMapper>
TiDB::DBInfoPtr SchemaBuilder<Getter, NameMapper>::tryFindDatabaseByPartitionTable(const TiDB::DBInfoPtr & db_info, const String & part_table_name)
{
    bool ok = context.getDatabase(name_mapper.mapDatabaseName(*db_info))->isTableExist(context, part_table_name);
    if (ok)
    {
        return db_info;
    }

    auto local_dbs = context.getDatabases();
    for (const auto & [db_name, local_db] : local_dbs)
    {
        if (!local_db->isTableExist(context, part_table_name))
        {
            continue;
        }
        auto db_tiflash = std::dynamic_pointer_cast<DatabaseTiFlash>(local_db);
        if (!db_tiflash)
        {
            LOG_WARNING(log, "tryFindDatabaseByPartitionTable, find part table in another database, but can not cast to DatabaseTiFlash, db_name={} part_table_name={}", db_name, part_table_name);
            break;
        }
        LOG_INFO(log, "tryFindDatabaseByPartitionTable, find part table in another database, EXCHANGE PARTITION may be executed, db_name={} part_table_name={}", db_name, part_table_name);
        return std::make_shared<TiDB::DBInfo>(db_tiflash->getDatabaseInfo());
    }

    LOG_INFO(log, "tryFindDatabaseByPartitionTable, can not find part table in all database, part_table_name={}", part_table_name);
    return db_info;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiffOnLogicalTable(const TiDB::DBInfoPtr & db_info, const TableInfoPtr & table_info, const ManageableStoragePtr & storage, bool drop_part_if_not_exist)
{
    const auto & orig_table_info = storage->getTableInfo();
    if (!orig_table_info.isLogicalPartitionTable())
    {
        throw TiFlashException(fmt::format("old table in TiFlash not partition table {}", name_mapper.debugCanonicalName(*db_info, orig_table_info)),
                               Errors::DDL::TableTypeNotMatch);
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

    LOG_INFO(log, "Applying partition changes {}, old: {}, new: {}", name_mapper.debugCanonicalName(*db_info, *table_info), orig_part_ids_str, new_part_ids_str);

    if (orig_part_id_set == new_part_id_set)
    {
        LOG_INFO(log, "No partition changes {}", name_mapper.debugCanonicalName(*db_info, *table_info));
        return;
    }

    /// Create new table info based on original table info.
    // Using copy of original table info with updated table name instead of using new_table_info directly,
    // so that other changes (ALTER/RENAME commands) won't be saved.
    // Besides, no need to update schema_version as partition change is not structural.
    auto updated_table_info = orig_table_info;
    updated_table_info.partition = table_info->partition;

    /// Apply changes to physical tables.
    if (drop_part_if_not_exist)
    {
        for (const auto & orig_def : orig_defs)
        {
            if (new_part_id_set.count(orig_def.id) == 0)
            {
                const auto part_table_name = name_mapper.mapTableNameByID(orig_def.id);
                // When `tryLoadSchemaDiffs` fails, we may run into `SchemaBuilder::syncAllSchema` -> `applyPartitionDiffOnLogicalTable` without `applyExchangeTablePartition`
                // The physical table maybe `EXCHANGE` to another database, try to find the partition from all database
                auto part_db_info = tryFindDatabaseByPartitionTable(db_info, part_table_name);
                applyDropPhysicalTable(name_mapper.mapDatabaseName(*part_db_info), orig_def.id, /*must_must_must_update_tombstone*/ false, "exchange partition");
            }
        }
    }

    for (const auto & new_def : new_defs)
    {
        if (orig_part_id_set.count(new_def.id) == 0)
        {
            auto part_table_info = updated_table_info.producePartitionTableInfo(new_def.id, name_mapper);
            const auto part_table_name = name_mapper.mapTableName(*part_table_info);
            // When `tryLoadSchemaDiffs` fails, we may run into `SchemaBuilder::syncAllSchema` -> `applyPartitionDiffOnLogicalTable` without `applyExchangeTablePartition`
            // The physical table maybe `EXCHANGE` from another database, try to find the partition from all database
            auto part_db_info = tryFindDatabaseByPartitionTable(db_info, part_table_name);
            applyCreatePhysicalTable(part_db_info, part_table_info);
        }
    }

    /// Apply new table info to logical table.
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, AlterCommands{}, name_mapper.mapDatabaseName(*db_info), updated_table_info, name_mapper, context);

    LOG_INFO(log, "Applied partition changes {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(const DBInfoPtr & new_db_info, TableID table_id, std::string_view action)
{
    auto new_table_info = getter.getTableInfo(new_db_info->id, table_id);
    if (new_table_info == nullptr)
    {
        throw TiFlashException(fmt::format("miss table id in TiKV when renameTable, table_id={} action={}", table_id, action), Errors::DDL::StaleSchema);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw TiFlashException(fmt::format("miss table id in TiFlash when renameTable, table_id={} action={}", table_id, action), Errors::DDL::MissingTable);
    }

    applyRenameLogicalTable(new_db_info, new_table_info, storage, action);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameLogicalTable(
    const DBInfoPtr & new_db_info,
    const TableInfoPtr & new_table_info,
    const ManageableStoragePtr & storage,
    std::string_view action)
{
    applyRenamePhysicalTable(new_db_info, *new_table_info, storage, action);

    if (new_table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage == nullptr)
            {
                throw Exception( //
                    ErrorCodes::LOGICAL_ERROR,
                    "Storage instance is not exist in TiFlash, the partition is not created yet in this TiFlash instance, "
                    "physical_table_id={} logical_table_id={} action={}",
                    part_def.id,
                    new_table_info->id,
                    action);
            }
            auto part_table_info = new_table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRenamePhysicalTable(new_db_info, *part_table_info, part_storage, action);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenamePhysicalTable(
    const DBInfoPtr & new_db_info,
    const TableInfo & new_table_info,
    const ManageableStoragePtr & storage,
    std::string_view action)
{
    const auto old_mapped_db_name = storage->getDatabaseName();
    const auto new_mapped_db_name = name_mapper.mapDatabaseName(*new_db_info);
    const auto old_display_table_name = name_mapper.displayTableName(storage->getTableInfo());
    const auto new_display_table_name = name_mapper.displayTableName(new_table_info);
    if (old_mapped_db_name == new_mapped_db_name && old_display_table_name == new_display_table_name)
    {
        LOG_DEBUG(log, "Table {} name identical, not renaming, action={}", name_mapper.debugCanonicalName(*new_db_info, new_table_info), action);
        return;
    }

    const auto old_mapped_tbl_name = storage->getTableName();
    GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_column).Increment();
    LOG_INFO(
        log,
        "Renaming table {}.{} (display name: {}) to {}, action={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info),
        action);

    // Note that rename will update table info in table create statement by modifying original table info
    // with "tidb_display.table" instead of using new_table_info directly, so that other changes
    // (ALTER commands) won't be saved. Besides, no need to update schema_version as table name is not structural.
    auto rename = std::make_shared<ASTRenameQuery>();
    ASTRenameQuery::Table from{old_mapped_db_name, old_mapped_tbl_name};
    ASTRenameQuery::Table to{new_mapped_db_name, name_mapper.mapTableName(new_table_info)};
    ASTRenameQuery::Table display{name_mapper.displayDatabaseName(*new_db_info), new_display_table_name};
    ASTRenameQuery::Element elem{.from = std::move(from), .to = std::move(to), .tidb_display = std::move(display)};
    rename->elements.emplace_back(std::move(elem));

    InterpreterRenameQuery(rename, context, getThreadName()).execute();

    LOG_INFO(
        log,
        "Renamed table {}.{} (display name: {}) to {}, action={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info),
        action);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyExchangeTablePartition(const SchemaDiff & diff)
{
    if (diff.old_table_id == diff.table_id && diff.old_schema_id == diff.schema_id)
    {
        // Only internal changes in non-partitioned table, not affecting TiFlash
        LOG_DEBUG(
            log,
            "Table is going to be exchanged, skipping for now. database_id={} table_id={}",
            diff.schema_id,
            diff.table_id);
        return;
    }
    LOG_DEBUG(
        log,
        "Table and partition is exchanged. database_id={} table_id={}, part_db_id={}, part_table_id={} partition_id={}",
        diff.old_schema_id,
        diff.old_table_id,
        diff.affected_opts[0].schema_id,
        diff.affected_opts[0].table_id,
        diff.table_id);

    /// Exchange table partition is used for ddl:
    /// `ALTER TABLE partition_table EXCHANGE PARTITION partition_name WITH TABLE non_partition_table`
    /// It involves three table/partition: partition_table, partition_name and non_partition_table
    /// The table id/schema id for the 3 table/partition are stored in SchemaDiff as follows:
    /// Table_id in diff is the partition id of which will be exchanged,
    /// Schema_id in diff is the non-partition table's schema id
    /// Old_table_id in diff is the non-partition table's table id
    /// Table_id in diff.affected_opts[0] is the table id of the partition table
    /// Schema_id in diff.affected_opts[0] is the schema id of the partition table
    GET_METRIC(tiflash_schema_internal_ddl_count, type_exchange_partition).Increment();
    if (diff.affected_opts.empty())
        throw Exception("Incorrect schema diff, no affected_opts for alter table exchange partition schema diff", ErrorCodes::DDL_ERROR);
    const auto npt_database_id = diff.schema_id;
    const auto pt_database_id = diff.affected_opts[0].schema_id;
    auto npt_db_info = getter.getDatabase(npt_database_id);
    if (npt_db_info == nullptr)
        throw TiFlashException(fmt::format("miss database: {}", diff.schema_id), Errors::DDL::StaleSchema);
    auto pt_db_info = getter.getDatabase(pt_database_id);
    if (pt_db_info == nullptr)
        throw TiFlashException(fmt::format("miss database: {}", diff.affected_opts[0].schema_id), Errors::DDL::StaleSchema);
    const auto npt_table_id = diff.old_table_id;
    const auto pt_partition_id = diff.table_id;
    const auto pt_table_id = diff.affected_opts[0].table_id;

    LOG_INFO(log, "Execute exchange partition begin, npt_table_id={} npt_database_id={} pt_table_id={} pt_partition_id={}  pt_database_id={}", npt_table_id, npt_database_id, pt_table_id, pt_partition_id, pt_database_id);
    /// step 1 change the mete data of partition table
    auto table_info = getter.getTableInfo(pt_db_info->id, pt_table_id); // latest partition table info from TiKV
    if (table_info == nullptr)
        throw TiFlashException(fmt::format("miss table in TiKV : pt_table_id={}", pt_table_id), Errors::DDL::StaleSchema);
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
        throw TiFlashException(
            fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*pt_db_info, *table_info)),
            Errors::DDL::MissingTable);

    // Apply the new partitions to the logical table.
    /// - create the new physical tables according to the new partition definitions
    /// - persist the new table info to disk
    // The latest table info could be the table info after `EXCHANGE PARTITION` is executed
    // on TiDB. So we need to apply and also create the physical tables of new ids appear in
    // the partition list. Because we can not get a table schema by its physical_table_id
    // once it became a partition.
    // But this method will skip dropping partition id that is not exist in the new table_info,
    // because the physical table could be changed into a normal table without dropping.
    applyPartitionDiffOnLogicalTable(pt_db_info, table_info, storage, /*drop_part_if_not_exist*/ false);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_1_in_exchange_partition);

    /// step 2 change non partition table to a partition of the partition table
    storage = tmt_context.getStorages().get(npt_table_id);
    if (storage == nullptr)
        throw TiFlashException(fmt::format("miss table in TiFlash, npt_table_id={} : {}", npt_table_id, name_mapper.debugCanonicalName(*npt_db_info, *table_info)),
                               Errors::DDL::MissingTable);
    auto orig_table_info = storage->getTableInfo();
    orig_table_info.belonging_table_id = pt_table_id;
    orig_table_info.is_partition_table = true;
    /// partition does not have explicit name, so use default name here
    orig_table_info.name = name_mapper.mapTableName(orig_table_info);
    {
        auto alter_lock = storage->lockForAlter(getThreadName());
        storage->alterFromTiDB(
            alter_lock,
            AlterCommands{},
            name_mapper.mapDatabaseName(*npt_db_info),
            orig_table_info,
            name_mapper,
            context);
    }
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_step_2_rename_in_exchange_partition);

    if (npt_db_info->id != pt_db_info->id)
        applyRenamePhysicalTable(pt_db_info, orig_table_info, storage, "exchange partition");
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_2_in_exchange_partition);

    /// step 3 change partition of the partition table to non partition table
    table_info = getter.getTableInfo(npt_db_info->id, pt_partition_id);
    if (table_info == nullptr)
    {
        LOG_WARNING(log, "Execute exchange partition, the table info of partition can not get from TiKV, npt_database_id={} partition_id={}", npt_database_id, pt_partition_id);
        throw TiFlashException(fmt::format("miss partition table in TiKV, may have been dropped, physical_table_id={}", pt_partition_id), Errors::DDL::StaleSchema);
    }
    storage = tmt_context.getStorages().get(pt_partition_id);
    if (storage == nullptr)
        throw TiFlashException(
            fmt::format("miss partition table in TiFlash, physical_table_id={}", pt_partition_id),
            Errors::DDL::MissingTable);
    orig_table_info = storage->getTableInfo();
    orig_table_info.belonging_table_id = DB::InvalidTableID;
    orig_table_info.is_partition_table = false;
    orig_table_info.name = table_info->name;
    {
        auto alter_lock = storage->lockForAlter(getThreadName());
        storage->alterFromTiDB(
            alter_lock,
            AlterCommands{},
            name_mapper.mapDatabaseName(*pt_db_info),
            orig_table_info,
            name_mapper,
            context);
    }
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_step_3_rename_in_exchange_partition);

    if (npt_db_info->id != pt_db_info->id)
        applyRenamePhysicalTable(npt_db_info, orig_table_info, storage, "exchange partition");
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_3_in_exchange_partition);
    LOG_INFO(log, "Execute exchange partition done, npt_table_id={} npt_database_id={} pt_table_id={} pt_partition_id={} pt_database_id={}", npt_table_id, npt_database_id, pt_table_id, pt_partition_id, pt_database_id);
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

static ASTPtr parseCreateStatement(const String & statement)
{
    ParserCreateQuery parser;
    const char * pos = statement.data();
    std::string error_msg;
    auto ast = tryParseQuery(parser,
                             pos,
                             pos + statement.size(),
                             error_msg,
                             /*hilite=*/false,
                             String("in ") + __PRETTY_FUNCTION__,
                             /*allow_multi_statements=*/false,
                             0);
    if (!ast)
        throw Exception(error_msg, ErrorCodes::SYNTAX_ERROR);
    return ast;
}

String createDatabaseStmt(Context & context, const DBInfo & db_info, const SchemaNameMapper & name_mapper)
{
    auto mapped = name_mapper.mapDatabaseName(db_info);
    if (isReservedDatabase(context, mapped))
        throw TiFlashException(fmt::format("Database {} is reserved", name_mapper.debugDatabaseName(db_info)), Errors::DDL::Internal);

    // R"raw(
    // CREATE DATABASE IF NOT EXISTS `db_xx`
    // ENGINE = TiFlash('<json-db-info>', <format-version>)
    // )raw";

    String stmt;
    WriteBufferFromString stmt_buf(stmt);
    writeString("CREATE DATABASE IF NOT EXISTS ", stmt_buf);
    writeBackQuotedString(mapped, stmt_buf);
    writeString(" ENGINE = TiFlash('", stmt_buf);
    writeEscapedString(db_info.serialize(), stmt_buf); // must escaped for json-encoded text
    writeString("', ", stmt_buf);
    writeIntText(DatabaseTiFlash::CURRENT_VERSION, stmt_buf);
    writeString(")", stmt_buf);
    return stmt;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateSchema(const TiDB::DBInfoPtr & db_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_db).Increment();
    LOG_INFO(log, "Creating database {}", name_mapper.debugDatabaseName(*db_info));

    auto statement = createDatabaseStmt(context, *db_info, name_mapper);

    ASTPtr ast = parseCreateStatement(statement);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    databases[db_info->id] = db_info;
    LOG_INFO(log, "Created database {}", name_mapper.debugDatabaseName(*db_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(DatabaseID schema_id)
{
    auto it = databases.find(schema_id);
    if (unlikely(it == databases.end()))
    {
        LOG_INFO(
            log,
            "Syncer wants to drop database [id={}], but database is not found, may has been dropped.",
            schema_id);
        return;
    }
    applyDropSchema(name_mapper.mapDatabaseName(*it->second));
    databases.erase(schema_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(const String & db_name)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_db).Increment();
    LOG_INFO(log, "Tombstone database begin, db_name={}", db_name);
    auto db = context.tryGetDatabase(db_name);
    if (db == nullptr)
    {
        LOG_INFO(log, "Database does not exists, db_name={}", db_name);
        return;
    }

    /// In order not to acquire `drop_lock` on storages, we tombstone the database in
    /// this thread and physically remove data in another thread. So that applying
    /// drop DDL from TiDB won't block or be blocked by read / write to the storages.

    // Instead of getting a precise time that TiDB drops this database, use a more
    // relaxing GC strategy:
    // 1. Use current timestamp, which is after TiDB's drop time, to be the tombstone of this database;
    // 2. Use the same GC safe point as TiDB.
    // In such way our database (and its belonging tables) will be GC-ed later than TiDB, which is safe and correct.
    auto & tmt_context = context.getTMTContext();
    auto tombstone = PDClientHelper::getTSO(tmt_context.getPDClient(), PDClientHelper::get_tso_maxtime);
    db->alterTombstone(context, tombstone, /*new_db_info*/ nullptr); // keep the old db_info

    LOG_INFO(log, "Tombstone database end, db_name={} tombstone={}", db_name, tombstone);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverSchema(DatabaseID database_id)
{
    auto db_info = getter.getDatabase(database_id);
    if (db_info == nullptr)
    {
        LOG_INFO(
            log,
            "Recover database is ignored because database is not exist in TiKV,"
            " database_id={}",
            database_id);
        return;
    }
    LOG_INFO(log, "Recover database begin, database_id={}", database_id);
    auto db_name = name_mapper.mapDatabaseName(*db_info);
    auto db = context.tryGetDatabase(db_name);
    if (!db)
    {
        LOG_ERROR(
            log,
            "Recover database is ignored because instance is not exists, may have been physically dropped, "
            "database_id={}",
            db_name,
            database_id);
        return;
    }

    {
        for (auto table_iter = db->getIterator(context); table_iter->isValid(); table_iter->next())
        {
            auto & storage = table_iter->table();
            auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
            if (!managed_storage)
            {
                LOG_WARNING(log, "Recover database ignore non-manageable storage, name={} engine={}", storage->getTableName(), storage->getName());
                continue;
            }
            LOG_WARNING(log, "Recover database on storage begin, name={}", storage->getTableName());
            auto table_id = managed_storage->getTableInfo().id;
            applyCreateTable(db_info, table_id);
        }
    }

    // Usually `FLASHBACK DATABASE ... TO ...` will rename the database
    db->alterTombstone(context, 0, db_info);
    databases[db_info->id] = db_info;
    LOG_INFO(log, "Recover database end, database_id={}", database_id);
}

std::tuple<NamesAndTypes, Strings>
parseColumnsFromTableInfo(const TiDB::TableInfo & table_info)
{
    NamesAndTypes columns;
    std::vector<String> primary_keys;
    for (const auto & column : table_info.columns)
    {
        DataTypePtr type = getDataTypeByColumnInfo(column);
        columns.emplace_back(column.name, type);
        if (column.hasPriKeyFlag())
        {
            primary_keys.emplace_back(column.name);
        }
    }

    if (!table_info.pk_is_handle)
    {
        // Make primary key as a column, and make the handle column as the primary key.
        if (table_info.is_common_handle)
            columns.emplace_back(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeString>());
        else
            columns.emplace_back(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeInt64>());
        primary_keys.clear();
        primary_keys.emplace_back(MutableSupport::tidb_pk_column_name);
    }

    return std::make_tuple(std::move(columns), std::move(primary_keys));
}

String createTableStmt(
    const DBInfo & db_info,
    const TableInfo & table_info,
    const SchemaNameMapper & name_mapper,
    const LoggerPtr & log)
{
    LOG_DEBUG(log, "Analyzing table info : {}", table_info.serialize());
    auto [columns, pks] = parseColumnsFromTableInfo(table_info);

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
    if (table_info.engine_type == TiDB::StorageEngine::DT)
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
    else
    {
        throw TiFlashException(fmt::format("Unknown engine type : {}", static_cast<int32_t>(table_info.engine_type)), Errors::DDL::Internal);
    }

    return stmt;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreatePhysicalTable(const DBInfoPtr & db_info, const TableInfoPtr & table_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(log, "Creating table {}", name_mapper.debugCanonicalName(*db_info, *table_info));

    /// Update schema version.
    table_info->schema_version = target_version;

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto * storage = tmt_context.getStorages().get(table_info->id).get(); storage)
        {
            if (!storage->isTombstone())
            {
                LOG_DEBUG(log,
                          "Trying to create table {} but it already exists and is not marked as tombstone",
                          name_mapper.debugCanonicalName(*db_info, *table_info));
                return;
            }

            LOG_INFO(log, "Recovering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            auto alter_lock = storage->lockForAlter(getThreadName());
            storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), *table_info, name_mapper, context);
            LOG_INFO(log, "Created table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
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

    LOG_INFO(log, "Creating table {} with statement: {}", name_mapper.debugCanonicalName(*db_info, *table_info), stmt);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info->name, 0);

    auto * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->if_not_exists = true;
    ast_create_query->database = name_mapper.mapDatabaseName(*db_info);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
    LOG_INFO(log, "Created table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateTable(const TiDB::DBInfoPtr & db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        LOG_INFO(log, "Table {} not found, may have been dropped.", table_id);
        return;
    }

    applyCreateLogicalTable(db_info, table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateLogicalTable(const TiDB::DBInfoPtr & db_info, const TableInfoPtr & table_info)
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
void SchemaBuilder<Getter, NameMapper>::applyDropPhysicalTable(const String & db_name, TableID table_id, bool must_update_tombstone, std::string_view action)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        LOG_DEBUG(log, "table does not exist, table_id={} action={}", table_id, action);
        return;
    }

    if (!must_update_tombstone)
    {
        // When must_update_tombstone == false, we can try to skip updating
        // the tombstone_ts if the table is already tombstone.
        if (auto tombstone_ts = storage->getTombstone(); tombstone_ts != 0)
        {
            LOG_INFO(log, "Tombstone table {}.{} has been done before, action={} tombstone={}", db_name, name_mapper.debugTableName(storage->getTableInfo()), action, tombstone_ts);
            return;
        }
    }

    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_table).Increment();
    LOG_INFO(log, "Tombstone table {}.{} begin, action={}", db_name, name_mapper.debugTableName(storage->getTableInfo()), action);
    const auto tombstone_ts = PDClientHelper::getTSO(tmt_context.getPDClient(), PDClientHelper::get_tso_maxtime);
    AlterCommands commands;
    {
        AlterCommand command;
        command.type = AlterCommand::TOMBSTONE;
        // We don't try to get a precise time that TiDB drops this table.
        // We use a more relaxing GC strategy:
        // 1. Use current timestamp, which is after TiDB's drop time, to be the tombstone of this table;
        // 2. Use the same GC safe point as TiDB.
        // In such way our table will be GC-ed later than TiDB, which is safe and correct.
        command.tombstone = tombstone_ts;
        commands.emplace_back(std::move(command));
    }
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(log, "Tombstone table {}.{} end, action={} tombstone={}", db_name, name_mapper.debugTableName(storage->getTableInfo()), action, tombstone_ts);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTable(const DBInfoPtr & db_info, TableID table_id, std::string_view action)
{
    auto & tmt_context = context.getTMTContext();
    auto * storage = tmt_context.getStorages().get(table_id).get();
    if (storage == nullptr)
    {
        LOG_DEBUG(log, "table {} does not exist.", table_id);
        return;
    }
    const auto & table_info = storage->getTableInfo();
    if (table_info.isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info.partition.definitions)
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), part_def.id, /*must_update_tombstone*/ true, action);
        }
    }

    // Drop logical table at last, only logical table drop will be treated as "complete".
    // Intermediate failure will hide the logical table drop so that schema syncing when restart will re-drop all (despite some physical tables may have dropped).
    applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), table_info.id, /*must_update_tombstone*/ true, action);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplica(const TiDB::DBInfoPtr & db_info, TableID table_id)
{
    auto latest_table_info = getter.getTableInfo(db_info->id, table_id);
    if (unlikely(latest_table_info == nullptr))
    {
        throw TiFlashException(fmt::format("miss table in TiKV : {}", table_id), Errors::DDL::StaleSchema);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(latest_table_info->id);
    if (unlikely(storage == nullptr))
    {
        throw TiFlashException(fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*db_info, *latest_table_info)),
                               Errors::DDL::MissingTable);
    }

    applySetTiFlashReplicaOnLogicalTable(db_info, latest_table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplicaOnLogicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage)
{
    applySetTiFlashReplicaOnPhysicalTable(db_info, table_info, storage);

    if (table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();

        for (const auto & part_def : table_info->partition.definitions)
        {
            auto new_part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            auto part_storage = tmt_context.getStorages().get(new_part_table_info->id);
            if (unlikely(part_storage == nullptr))
            {
                throw TiFlashException(fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*db_info, *new_part_table_info)),
                                       Errors::DDL::MissingTable);
            }
            applySetTiFlashReplicaOnPhysicalTable(db_info, new_part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplicaOnPhysicalTable(
    const TiDB::DBInfoPtr & db_info,
    const TiDB::TableInfoPtr & latest_table_info,
    const ManageableStoragePtr & storage)
{
    if (storage->getTableInfo().replica_info.count == latest_table_info->replica_info.count)
        return;

    // Get a copy of old table info and update replica info
    TiDB::TableInfo table_info = storage->getTableInfo();

    LOG_INFO(log, "Updating replica info for {}, replica count from {} to {}", name_mapper.debugCanonicalName(*db_info, table_info), table_info.replica_info.count, latest_table_info->replica_info.count);
    table_info.replica_info = latest_table_info->replica_info;

    AlterCommands commands;
    // Note that update replica info will update table info in table create statement by modifying
    // original table info with new replica info instead of using latest_table_info directly, so that
    // other changes (ALTER commands) won't be saved.
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), table_info, name_mapper, context);
    LOG_INFO(log, "Updated replica info for {}", name_mapper.debugCanonicalName(*db_info, table_info));
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
            LOG_INFO(log, "Database {} created during sync all schemas", name_mapper.debugDatabaseName(*db));
        }
    }

    /// Load all tables in each database.
    std::unordered_set<TableID> table_set;
    for (const auto & db : all_schemas)
    {
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto & table : tables)
        {
            LOG_INFO(log, "Table {} syncing during sync all schemas", name_mapper.debugCanonicalName(*db, *table));

            /// Ignore view and sequence.
            if (table->is_view || table->is_sequence)
            {
                LOG_INFO(log, "Table {} is a view or sequence, ignoring.", name_mapper.debugCanonicalName(*db, *table));
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
                                "Table {} not synced because may have been dropped during sync all schemas",
                                name_mapper.debugCanonicalName(*db, *table));
                    continue;
                }
            }
            if (table->isLogicalPartitionTable())
            {
                /// Apply partition diff if needed.
                applyPartitionDiffOnLogicalTable(db, table, storage, /*drop_part_if_not_exist*/ true);
            }
            /// Rename if needed.
            applyRenameLogicalTable(db, table, storage, "SyncAllSchema");
            /// Update replica info if needed.
            applySetTiFlashReplicaOnLogicalTable(db, table, storage);
            /// Alter if needed.
            applyAlterLogicalTable(db, table, storage);
            LOG_INFO(log, "Table {} synced during sync all schemas", name_mapper.debugCanonicalName(*db, *table));
        }
        LOG_INFO(log, "Database {} synced during sync all schemas", name_mapper.debugDatabaseName(*db));
    }

    /// Drop all unmapped tables.
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        if (table_set.count(it->first) == 0)
        {
            applyDropPhysicalTable(it->second->getDatabaseName(), it->first, /*must_update_tombstone*/ false, "SyncAllSchema");
            LOG_INFO(log, "Table {}.{} dropped during sync all schemas", it->second->getDatabaseName(), name_mapper.debugTableName(it->second->getTableInfo()));
        }
    }

    /// Drop all unmapped dbs.
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        if (db_set.count(it->first) == 0 && !isReservedDatabase(context, it->first))
        {
            applyDropSchema(it->first);
            LOG_INFO(log, "DB {} dropped during sync all schemas", it->first);
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

// product env
template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
// mock test
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;
// unit test
template struct SchemaBuilder<MockSchemaGetter, SchemaNameMapper>;

// end namespace
} // namespace DB
