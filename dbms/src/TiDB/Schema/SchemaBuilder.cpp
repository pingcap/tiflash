// Copyright 2022 PingCAP, Ltd.
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
#include <TiDB/Schema/SchemaBuilder-internal.h>
#include <TiDB/Schema/SchemaBuilder.h>
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


inline void setAlterCommandColumn(Poco::Logger * log, AlterCommand & command, const ColumnInfo & column_info)
{
    command.column_name = column_info.name;
    command.column_id = column_info.id;
    command.data_type = getDataTypeByColumnInfo(column_info);
    if (!column_info.origin_default_value.isEmpty())
    {
        LOG_FMT_DEBUG(log, "add default value for column: {}", column_info.name);
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
    return false;
}

/// When schema change detected, the modification to original table info must be preserved as well.
/// With the preserved table info modifications, table info changes along with applying alter commands.
/// In other words, table info and storage structure (altered by applied alter commands) are always identical,
/// and intermediate failure won't hide the outstanding alter commands.
inline SchemaChanges detectSchemaChanges(
    Poco::Logger * log,
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
                          LOG_FMT_INFO(log, "detect column {} rename to {}", orig_column_info.name, column_info_.name);

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
    LOG_FMT_INFO(log, "Altering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));

    /// Detect schema changes.
    auto orig_table_info = storage->getTableInfo();
    auto schema_changes = detectSchemaChanges(log, *table_info, orig_table_info);
    if (schema_changes.empty())
    {
        LOG_FMT_INFO(log, "No schema change detected for table {}, not altering", name_mapper.debugCanonicalName(*db_info, *table_info));
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
    LOG_DEBUG(log, log_str());

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

    LOG_FMT_INFO(log, "Altered table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
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
            applyRenameTable(db_info, opt.table_id);
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
        applyRenameTable(db_info, diff.table_id);
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
    case SchemaActionType::SetTiFlashMode:
    {
        applySetTiFlashMode(db_info, diff.table_id);
        break;
    }
    default:
    {
        if (diff.type < SchemaActionType::MaxRecognizedType)
        {
            LOG_FMT_INFO(log, "Ignore change type: {}", int(diff.type));
        }
        else
        { // >= SchemaActionType::MaxRecognizedType
            LOG_FMT_ERROR(log, "Unsupported change type: {}", int(diff.type));
        }

        break;
    }
    }

    if (old_table_id)
    {
        applyDropTable(db_info, old_table_id);
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
        throw TiFlashException(fmt::format("miss old table id in TiKV {}", table_id), Errors::DDL::StaleSchema);
    }
    if (!table_info->isLogicalPartitionTable())
    {
        throw TiFlashException(fmt::format("new table in TiKV not partition table {}", name_mapper.debugCanonicalName(*db_info, *table_info)),
                               Errors::DDL::TableTypeNotMatch);
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
    {
        throw TiFlashException(fmt::format("miss table in TiFlash {}", table_id), Errors::DDL::MissingTable);
    }

    applyPartitionDiff(db_info, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(const TiDB::DBInfoPtr & db_info, const TableInfoPtr & table_info, const ManageableStoragePtr & storage)
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

    LOG_FMT_INFO(log, "Applying partition changes {}, old: {}, new: {}", name_mapper.debugCanonicalName(*db_info, *table_info), orig_part_ids_str, new_part_ids_str);

    if (orig_part_id_set == new_part_id_set)
    {
        LOG_FMT_INFO(log, "No partition changes {}", name_mapper.debugCanonicalName(*db_info, *table_info));
        return;
    }

    /// Create new table info based on original table info.
    // Using copy of original table info with updated table name instead of using new_table_info directly,
    // so that other changes (ALTER/RENAME commands) won't be saved.
    // Besides, no need to update schema_version as partition change is not structural.
    auto updated_table_info = orig_table_info;
    updated_table_info.partition = table_info->partition;

    /// Apply changes to physical tables.
    for (const auto & orig_def : orig_defs)
    {
        if (new_part_id_set.count(orig_def.id) == 0)
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), orig_def.id);
        }
    }
    for (const auto & new_def : new_defs)
    {
        if (orig_part_id_set.count(new_def.id) == 0)
        {
            auto part_table_info = updated_table_info.producePartitionTableInfo(new_def.id, name_mapper);
            applyCreatePhysicalTable(db_info, part_table_info);
        }
    }

    /// Apply new table info to logical table.
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, AlterCommands{}, name_mapper.mapDatabaseName(*db_info), updated_table_info, name_mapper, context);

    LOG_FMT_INFO(log, "Applied partition changes {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(const DBInfoPtr & new_db_info, TableID table_id)
{
    auto new_table_info = getter.getTableInfo(new_db_info->id, table_id);
    if (new_table_info == nullptr)
    {
        throw Exception(fmt::format("miss table id in TiKV {}", table_id));
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw Exception(fmt::format("miss table id in Flash {}", table_id));
    }

    applyRenameLogicalTable(new_db_info, new_table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameLogicalTable(
    const DBInfoPtr & new_db_info,
    const TableInfoPtr & new_table_info,
    const ManageableStoragePtr & storage)
{
    applyRenamePhysicalTable(new_db_info, *new_table_info, storage);

    if (new_table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto part_storage = tmt_context.getStorages().get(part_def.id);
            if (part_storage == nullptr)
            {
                throw Exception(fmt::format("miss old table id in Flash {}", part_def.id));
            }
            auto part_table_info = new_table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRenamePhysicalTable(new_db_info, *part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenamePhysicalTable(
    const DBInfoPtr & new_db_info,
    const TableInfo & new_table_info,
    const ManageableStoragePtr & storage)
{
    const auto old_mapped_db_name = storage->getDatabaseName();
    const auto new_mapped_db_name = name_mapper.mapDatabaseName(*new_db_info);
    const auto old_display_table_name = name_mapper.displayTableName(storage->getTableInfo());
    const auto new_display_table_name = name_mapper.displayTableName(new_table_info);
    if (old_mapped_db_name == new_mapped_db_name && old_display_table_name == new_display_table_name)
    {
        LOG_FMT_DEBUG(log, "Table {} name identical, not renaming.", name_mapper.debugCanonicalName(*new_db_info, new_table_info));
        return;
    }

    const auto old_mapped_tbl_name = storage->getTableName();
    GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_column).Increment();
    LOG_FMT_INFO(
        log,
        "Renaming table {}.{} (display name: {}) to {}.",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info));

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

    LOG_FMT_INFO(
        log,
        "Renamed table {}.{} (display name: {}) to {}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyExchangeTablePartition(const SchemaDiff & diff)
{
    /// Exchange table partition is used for ddl:
    /// alter table partition_table exchange partition partition_name with table non_partition_table
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
    auto npt_db_info = getter.getDatabase(diff.schema_id);
    if (npt_db_info == nullptr)
        throw TiFlashException(fmt::format("miss database: {}", diff.schema_id), Errors::DDL::StaleSchema);
    auto pt_db_info = getter.getDatabase(diff.affected_opts[0].schema_id);
    if (pt_db_info == nullptr)
        throw TiFlashException(fmt::format("miss database: {}", diff.affected_opts[0].schema_id), Errors::DDL::StaleSchema);
    auto npt_table_id = diff.old_table_id;
    auto pt_partition_id = diff.table_id;
    auto pt_table_info = diff.affected_opts[0].table_id;
    /// step 1 change the mete data of partition table
    auto table_info = getter.getTableInfo(pt_db_info->id, pt_table_info);
    if (table_info == nullptr)
        throw TiFlashException(fmt::format("miss table in TiKV : {}", pt_table_info), Errors::DDL::StaleSchema);
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
        throw TiFlashException(
            fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*pt_db_info, *table_info)),
            Errors::DDL::MissingTable);

    LOG_FMT_INFO(log, "Exchange partition for table {}", name_mapper.debugCanonicalName(*pt_db_info, *table_info));
    auto orig_table_info = storage->getTableInfo();
    orig_table_info.partition = table_info->partition;
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
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_1_in_exchange_partition);

    /// step 2 change non partition table to a partition of the partition table
    storage = tmt_context.getStorages().get(npt_table_id);
    if (storage == nullptr)
        throw TiFlashException(fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*npt_db_info, *table_info)),
                               Errors::DDL::MissingTable);
    orig_table_info = storage->getTableInfo();
    orig_table_info.belonging_table_id = pt_table_info;
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
        applyRenamePhysicalTable(pt_db_info, orig_table_info, storage);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_2_in_exchange_partition);

    /// step 3 change partition of the partition table to non partition table
    table_info = getter.getTableInfo(npt_db_info->id, pt_partition_id);
    if (table_info == nullptr)
        throw TiFlashException(fmt::format("miss table in TiKV : {}", pt_partition_id), Errors::DDL::StaleSchema);
    storage = tmt_context.getStorages().get(table_info->id);
    if (storage == nullptr)
        throw TiFlashException(
            fmt::format("miss table in TiFlash : {}", name_mapper.debugCanonicalName(*pt_db_info, *table_info)),
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
        applyRenamePhysicalTable(npt_db_info, orig_table_info, storage);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_step_3_in_exchange_partition);
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
    LOG_FMT_INFO(log, "Creating database {}", name_mapper.debugDatabaseName(*db_info));

    auto statement = createDatabaseStmt(context, *db_info, name_mapper);

    ASTPtr ast = parseCreateStatement(statement);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    databases[db_info->id] = db_info;
    LOG_FMT_INFO(log, "Created database {}", name_mapper.debugDatabaseName(*db_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(DatabaseID schema_id)
{
    auto it = databases.find(schema_id);
    if (unlikely(it == databases.end()))
    {
        LOG_FMT_INFO(
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
    LOG_FMT_INFO(log, "Tombstoning database {}", db_name);
    auto db = context.tryGetDatabase(db_name);
    if (db == nullptr)
    {
        LOG_FMT_INFO(log, "Database {} does not exists", db_name);
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
    auto tombstone = tmt_context.getPDClient()->getTS();
    db->alterTombstone(context, tombstone);

    LOG_FMT_INFO(log, "Tombstoned database {}", db_name);
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
    Poco::Logger * log)
{
    LOG_FMT_DEBUG(log, "Analyzing table info : {}", table_info.serialize());
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
    LOG_FMT_INFO(log, "Creating table {}", name_mapper.debugCanonicalName(*db_info, *table_info));

    /// Update schema version.
    table_info->schema_version = target_version;

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto * storage = tmt_context.getStorages().get(table_info->id).get(); storage)
        {
            if (!storage->isTombstone())
            {
                LOG_FMT_DEBUG(log,
                              "Trying to create table {} but it already exists and is not marked as tombstone",
                              name_mapper.debugCanonicalName(*db_info, *table_info));
                return;
            }

            LOG_FMT_DEBUG(log, "Recovering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            auto alter_lock = storage->lockForAlter(getThreadName());
            storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), *table_info, name_mapper, context);
            LOG_FMT_INFO(log, "Created table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
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

    LOG_FMT_INFO(log, "Creating table {} with statement: {}", name_mapper.debugCanonicalName(*db_info, *table_info), stmt);

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
    LOG_FMT_INFO(log, "Created table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateTable(const TiDB::DBInfoPtr & db_info, TableID table_id)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        LOG_FMT_DEBUG(log, "Table {} not found, may have been dropped.", table_id);
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
void SchemaBuilder<Getter, NameMapper>::applyDropPhysicalTable(const String & db_name, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(table_id);
    if (storage == nullptr)
    {
        LOG_FMT_DEBUG(log, "table {} does not exist.", table_id);
        return;
    }
    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_table).Increment();
    LOG_FMT_INFO(log, "Tombstoning table {}.{}", db_name, name_mapper.debugTableName(storage->getTableInfo()));
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
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_FMT_INFO(log, "Tombstoned table {}.{}", db_name, name_mapper.debugTableName(storage->getTableInfo()));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTable(const DBInfoPtr & db_info, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto * storage = tmt_context.getStorages().get(table_id).get();
    if (storage == nullptr)
    {
        LOG_FMT_DEBUG(log, "table {} does not exist.", table_id);
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
    table_info.replica_info = latest_table_info->replica_info;

    AlterCommands commands;
    LOG_FMT_INFO(log, "Updating replica info for {}", name_mapper.debugCanonicalName(*db_info, table_info));
    // Note that update replica info will update table info in table create statement by modifying
    // original table info with new replica info instead of using latest_table_info directly, so that
    // other changes (ALTER commands) won't be saved.
    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), table_info, name_mapper, context);
    LOG_FMT_INFO(log, "Updated replica info for {}", name_mapper.debugCanonicalName(*db_info, table_info));
}


template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashMode(const TiDB::DBInfoPtr & db_info, TableID table_id)
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

    applySetTiFlashModeOnLogicalTable(db_info, latest_table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashModeOnLogicalTable(
    const TiDB::DBInfoPtr & db_info,
    const TiDB::TableInfoPtr & table_info,
    const ManageableStoragePtr & storage)
{
    applySetTiFlashModeOnPhysicalTable(db_info, table_info, storage);

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
            applySetTiFlashModeOnPhysicalTable(db_info, new_part_table_info, part_storage);
        }
    }
}


template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashModeOnPhysicalTable(
    const TiDB::DBInfoPtr & db_info,
    const TiDB::TableInfoPtr & latest_table_info,
    const ManageableStoragePtr & storage)
{
    if (storage->getTableInfo().tiflash_mode == latest_table_info->tiflash_mode)
        return;

    TiDB::TableInfo table_info = storage->getTableInfo();
    table_info.tiflash_mode = latest_table_info->tiflash_mode;
    AlterCommands commands;

    LOG_FMT_INFO(log, "Updating tiflash mode for {} to {}", name_mapper.debugCanonicalName(*db_info, table_info), TiFlashModeToString(table_info.tiflash_mode));

    auto alter_lock = storage->lockForAlter(getThreadName());
    storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), table_info, name_mapper, context);
    LOG_FMT_INFO(log, "Updated tiflash mode for {} to {}", name_mapper.debugCanonicalName(*db_info, table_info), TiFlashModeToString(table_info.tiflash_mode));
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
            LOG_FMT_DEBUG(log, "Database {} created during sync all schemas", name_mapper.debugDatabaseName(*db));
        }
    }

    /// Load all tables in each database.
    std::unordered_set<TableID> table_set;
    for (const auto & db : all_schemas)
    {
        std::vector<TableInfoPtr> tables = getter.listTables(db->id);
        for (auto & table : tables)
        {
            LOG_FMT_DEBUG(log, "Table {} syncing during sync all schemas", name_mapper.debugCanonicalName(*db, *table));

            /// Ignore view and sequence.
            if (table->is_view || table->is_sequence)
            {
                LOG_FMT_INFO(log, "Table {} is a view or sequence, ignoring.", name_mapper.debugCanonicalName(*db, *table));
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
                    LOG_FMT_WARNING(log,
                                    "Table {} not synced because may have been dropped during sync all schemas",
                                    name_mapper.debugCanonicalName(*db, *table));
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
            /// Update replica info if needed.
            applySetTiFlashReplicaOnLogicalTable(db, table, storage);
            /// Update tiflash mode if needed.
            applySetTiFlashModeOnLogicalTable(db, table, storage);
            /// Alter if needed.
            applyAlterLogicalTable(db, table, storage);
            LOG_FMT_DEBUG(log, "Table {} synced during sync all schemas", name_mapper.debugCanonicalName(*db, *table));
        }
        LOG_FMT_DEBUG(log, "Database {} synced during sync all schemas", name_mapper.debugDatabaseName(*db));
    }

    /// Drop all unmapped tables.
    auto storage_map = tmt_context.getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        if (table_set.count(it->first) == 0)
        {
            applyDropPhysicalTable(it->second->getDatabaseName(), it->first);
            LOG_FMT_DEBUG(log, "Table {}.{} dropped during sync all schemas", it->second->getDatabaseName(), name_mapper.debugTableName(it->second->getTableInfo()));
        }
    }

    /// Drop all unmapped dbs.
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        if (db_set.count(it->first) == 0 && !isReservedDatabase(context, it->first))
        {
            applyDropSchema(it->first);
            LOG_FMT_DEBUG(log, "DB {} dropped during sync all schemas", it->first);
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;

// end namespace
} // namespace DB
