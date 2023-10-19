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
#include <Interpreters/Context.h>
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
#include <Storages/KVStore/TMTContext.h>
#include <Storages/MutableSupport.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/SchemaBuilder-internal.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>
#include <magic_enum.hpp>
#include <mutex>
#include <tuple>

namespace DB
{
using namespace TiDB;
namespace ErrorCodes
{
extern const int DDL_ERROR;
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

bool isReservedDatabase(Context & context, const String & database_name)
{
    return context.getTMTContext().getIgnoreDatabases().count(database_name) > 0;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateTable(DatabaseID database_id, TableID table_id)
{
    auto table_info = getter.getTableInfo(database_id, table_id);
    if (table_info == nullptr) // the database maybe dropped
    {
        LOG_DEBUG(log, "table is not exist in TiKV, may have been dropped, table_id={}", table_id);
        return;
    }

    table_id_map.emplaceTableID(table_id, database_id);
    LOG_DEBUG(log, "register table to table_id_map, database_id={} table_id={}", database_id, table_id);

    if (table_info->isLogicalPartitionTable())
    {
        // If table is partition table, we will create the logical table here.
        // Because we get the table_info, so we can ensure new_db_info will not be nullptr.
        auto new_db_info = getter.getDatabase(database_id);
        applyCreatePhysicalTable(new_db_info, table_info);

        for (const auto & part_def : table_info->partition.definitions)
        {
            LOG_DEBUG(
                log,
                "register table to table_id_map for partition table, logical_table_id={} physical_table_id={}",
                table_id,
                part_def.id);
            table_id_map.emplacePartitionTableID(part_def.id, table_id);
        }
    }
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
    /// Table_id in diff is the partition id of which will be exchanged,
    /// Schema_id in diff is the non-partition table's schema id
    /// Old_table_id in diff is the non-partition table's table id
    /// Table_id in diff.affected_opts[0] is the table id of the partition table
    /// Schema_id in diff.affected_opts[0] is the schema id of the partition table
    table_id_map.eraseTableIDOrLogError(diff.old_table_id);
    table_id_map.emplaceTableID(diff.table_id, diff.schema_id);
    table_id_map.erasePartitionTableIDOrLogError(diff.table_id);
    table_id_map.emplacePartitionTableID(diff.old_table_id, diff.affected_opts[0].table_id);

    if (diff.schema_id != diff.affected_opts[0].schema_id)
    {
        // rename old_table_id(non-partition table)
        {
            auto [new_db_info, new_table_info]
                = getter.getDatabaseAndTableInfo(diff.affected_opts[0].schema_id, diff.affected_opts[0].table_id);
            if (new_table_info == nullptr)
            {
                LOG_ERROR(log, "table is not exist in TiKV, table_id={}", diff.affected_opts[0].table_id);
                return;
            }

            auto & tmt_context = context.getTMTContext();
            auto storage = tmt_context.getStorages().get(keyspace_id, diff.old_table_id);
            if (storage == nullptr)
            {
                LOG_ERROR(log, "table is not exist in TiFlash, table_id={}", diff.old_table_id);
                return;
            }

            auto part_table_info = new_table_info->producePartitionTableInfo(diff.old_table_id, name_mapper);
            applyRenamePhysicalTable(new_db_info, *part_table_info, storage);
        }

        // rename table_id(the exchanged partition table)
        {
            auto [new_db_info, new_table_info] = getter.getDatabaseAndTableInfo(diff.schema_id, diff.table_id);
            if (new_table_info == nullptr)
            {
                LOG_ERROR(log, "table is not exist in TiKV, table_id={}", diff.table_id);
                return;
            }

            auto & tmt_context = context.getTMTContext();
            auto storage = tmt_context.getStorages().get(keyspace_id, diff.table_id);
            if (storage == nullptr)
            {
                LOG_ERROR(log, "table is not exist in TiFlash, table_id={}", diff.old_table_id);
                return;
            }

            applyRenamePhysicalTable(new_db_info, *new_table_info, storage);
        }
    }

    GET_METRIC(tiflash_schema_internal_ddl_count, type_exchange_partition).Increment();
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDiff(const SchemaDiff & diff)
{
    switch (diff.type)
    {
    case SchemaActionType::CreateSchema:
    {
        applyCreateSchema(diff.schema_id);
        break;
    }
    case SchemaActionType::DropSchema:
    {
        applyDropSchema(diff.schema_id);
        break;
    }
    case SchemaActionType::CreateTables:
    {
        /// Because we can't ensure set tiflash replica always be finished earlier than insert actions,
        /// so we have to update table_id_map when create table.
        /// and the table will not be created physically here.
        for (auto && opt : diff.affected_opts)
            applyCreateTable(opt.schema_id, opt.table_id);
        break;
    }
    case SchemaActionType::RenameTables:
    {
        for (auto && opt : diff.affected_opts)
            applyRenameTable(opt.schema_id, opt.table_id);
        break;
    }
    case SchemaActionType::CreateTable:
    {
        /// Because we can't ensure set tiflash replica is earlier than insert,
        /// so we have to update table_id_map when create table.
        /// the table will not be created physically here.
        applyCreateTable(diff.schema_id, diff.table_id);
        break;
    }
    case SchemaActionType::RecoverTable:
    {
        applyRecoverTable(diff.schema_id, diff.table_id);
        break;
    }
    case SchemaActionType::DropTable:
    case SchemaActionType::DropView:
    {
        applyDropTable(diff.schema_id, diff.table_id);
        break;
    }
    case SchemaActionType::TruncateTable:
    {
        applyCreateTable(diff.schema_id, diff.table_id);
        applyDropTable(diff.schema_id, diff.old_table_id);
        break;
    }
    case SchemaActionType::RenameTable:
    {
        applyRenameTable(diff.schema_id, diff.table_id);
        break;
    }
    case SchemaActionType::AddTablePartition:
    case SchemaActionType::DropTablePartition:
    case SchemaActionType::TruncateTablePartition:
    case SchemaActionType::ActionReorganizePartition:
    {
        applyPartitionDiff(diff.schema_id, diff.table_id);
        break;
    }
    case SchemaActionType::ActionAlterTablePartitioning:
    case SchemaActionType::ActionRemovePartitioning:
    {
        if (diff.table_id == diff.old_table_id)
        {
            /// Only internal additions of new partitions
            applyPartitionDiff(diff.schema_id, diff.table_id);
        }
        else
        {
            /// The new non-partitioned table will have a new id
            applyDropTable(diff.schema_id, diff.old_table_id);
            applyCreateTable(diff.schema_id, diff.table_id);
        }
        break;
    }
    case SchemaActionType::ExchangeTablePartition:
    {
        applyExchangeTablePartition(diff);
        break;
    }
    case SchemaActionType::SetTiFlashReplica:
    case SchemaActionType::UpdateTiFlashReplicaStatus:
    {
        applySetTiFlashReplica(diff.schema_id, diff.table_id);
        break;
    }
    default:
    {
        if (diff.type < SchemaActionType::MaxRecognizedType)
        {
            LOG_INFO(log, "Ignore change type: {}", magic_enum::enum_name(diff.type));
        }
        else
        {
            // >= SchemaActionType::MaxRecognizedType
            // log down the Int8 value directly
            LOG_ERROR(log, "Unsupported change type: {}", static_cast<Int8>(diff.type));
        }

        break;
    }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplica(DatabaseID database_id, TableID table_id)
{
    auto [db_info, table_info] = getter.getDatabaseAndTableInfo(database_id, table_id);
    if (unlikely(table_info == nullptr))
    {
        LOG_ERROR(log, "table is not exist in TiKV, table_id={}", table_id);
        return;
    }

    if (table_info->replica_info.count == 0)
    {
        // if set 0, drop table in TiFlash
        auto & tmt_context = context.getTMTContext();
        auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
        if (unlikely(storage == nullptr))
        {
            LOG_ERROR(log, "table is not exist in TiFlash, table_id={}", table_id);
            return;
        }

        applyDropTable(db_info->id, table_id);
    }
    else
    {
        // if set not 0, we first check whether the storage exists, and then check the replica_count and available
        auto & tmt_context = context.getTMTContext();
        auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
        if (storage != nullptr)
        {
            if (storage->getTombstone() == 0)
            {
                auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
                auto storage_replica_info = managed_storage->getTableInfo().replica_info;
                if (storage_replica_info.count == table_info->replica_info.count
                    && storage_replica_info.available == table_info->replica_info.available)
                {
                    return;
                }
                else
                {
                    if (table_info->isLogicalPartitionTable())
                    {
                        for (const auto & part_def : table_info->partition.definitions)
                        {
                            auto new_part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
                            auto part_storage = tmt_context.getStorages().get(keyspace_id, new_part_table_info->id);
                            if (part_storage != nullptr)
                            {
                                auto alter_lock = part_storage->lockForAlter(getThreadNameAndID());
                                part_storage->alterSchemaChange(
                                    alter_lock,
                                    *new_part_table_info,
                                    name_mapper.mapDatabaseName(db_info->id, keyspace_id),
                                    name_mapper.mapTableName(*new_part_table_info),
                                    context);
                            }
                            else
                                table_id_map.emplacePartitionTableID(part_def.id, table_id);
                        }
                    }
                    auto alter_lock = storage->lockForAlter(getThreadNameAndID());
                    storage->alterSchemaChange(
                        alter_lock,
                        *table_info,
                        name_mapper.mapDatabaseName(db_info->id, keyspace_id),
                        name_mapper.mapTableName(*table_info),
                        context);
                }
                return;
            }
            else
            {
                applyRecoverTable(db_info->id, table_id);
            }
        }
        else
        {
            if (!table_id_map.tableIDInDatabaseIdMap(table_id))
            {
                applyCreateTable(db_info->id, table_id);
            }
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(DatabaseID database_id, TableID table_id)
{
    auto [db_info, table_info] = getter.getDatabaseAndTableInfo(database_id, table_id);
    if (table_info == nullptr)
    {
        LOG_ERROR(log, "table is not exist in TiKV, table_id={}", table_id);
        return;
    }
    if (!table_info->isLogicalPartitionTable())
    {
        LOG_ERROR(
            log,
            "new table in TiKV not partition table {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(*db_info, *table_info),
            db_info->id,
            table_info->id);
        return;
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
    if (storage == nullptr)
    {
        LOG_ERROR(log, "table is not exist in TiFlash, table_id={}", table_id);
        return;
    }

    applyPartitionDiff(db_info, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(
    const TiDB::DBInfoPtr & db_info,
    const TableInfoPtr & table_info,
    const ManageableStoragePtr & storage)
{
    const auto & orig_table_info = storage->getTableInfo();
    if (!orig_table_info.isLogicalPartitionTable())
    {
        LOG_ERROR(
            log,
            "old table in TiFlash not partition table {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(*db_info, orig_table_info),
            db_info->id,
            orig_table_info.id);
        return;
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

    LOG_INFO(
        log,
        "Applying partition changes {} with database_id={}, table_id={}, old: {}, new: {}",
        name_mapper.debugCanonicalName(*db_info, *table_info),
        db_info->id,
        table_info->id,
        orig_part_ids_str,
        new_part_ids_str);

    if (orig_part_id_set == new_part_id_set)
    {
        LOG_INFO(
            log,
            "No partition changes {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(*db_info, *table_info),
            db_info->id,
            table_info->id);
        return;
    }

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
            table_id_map.emplacePartitionTableID(new_def.id, updated_table_info.id);
        }
    }

    auto alter_lock = storage->lockForAlter(getThreadNameAndID());
    storage->alterSchemaChange(
        alter_lock,
        updated_table_info,
        name_mapper.mapDatabaseName(db_info->id, keyspace_id),
        name_mapper.mapTableName(updated_table_info),
        context);

    GET_METRIC(tiflash_schema_internal_ddl_count, type_apply_partition).Increment();
    LOG_INFO(
        log,
        "Applied partition changes {} with database_id={}, table_id={}",
        name_mapper.debugCanonicalName(*db_info, *table_info),
        db_info->id,
        table_info->id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(DatabaseID database_id, TableID table_id)
{
    auto [new_db_info, new_table_info] = getter.getDatabaseAndTableInfo(database_id, table_id);
    if (new_table_info == nullptr)
    {
        LOG_ERROR(log, "table is not exist in TiKV, table_id={}", table_id);
        return;
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_ERROR(log, "table is not exist in TiFlash, table_id={}", table_id);
        return;
    }

    applyRenameLogicalTable(new_db_info, new_table_info, storage);

    table_id_map.emplaceTableID(table_id, database_id);
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
            auto part_storage = tmt_context.getStorages().get(keyspace_id, part_def.id);
            if (part_storage == nullptr)
            {
                LOG_ERROR(log, "table is not exist in TiFlash, physical_table_id={}", part_def.id);
                return;
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
        LOG_DEBUG(
            log,
            "Table {} name identical, not renaming. database_id={} table_id={}",
            name_mapper.debugCanonicalName(*new_db_info, new_table_info),
            new_db_info->id,
            new_table_info.id);
        return;
    }

    const auto old_mapped_tbl_name = storage->getTableName();
    GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_table).Increment();
    LOG_INFO(
        log,
        "Renaming table {}.{} (display name: {}) to {} with database_id={}, table_id={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info),
        new_db_info->id,
        new_table_info.id);

    // Note that rename will update table info in table create statement by modifying original table info
    // with "tidb_display.table" instead of using new_table_info directly, so that other changes
    // (ALTER commands) won't be saved. Besides, no need to update schema_version as table name is not structural.
    auto rename = std::make_shared<ASTRenameQuery>();
    ASTRenameQuery::Table from{old_mapped_db_name, old_mapped_tbl_name};
    ASTRenameQuery::Table to{new_mapped_db_name, name_mapper.mapTableName(new_table_info)};
    ASTRenameQuery::Table display{name_mapper.displayDatabaseName(*new_db_info), new_display_table_name};
    ASTRenameQuery::Element elem{.from = std::move(from), .to = std::move(to), .tidb_display = std::move(display)};
    rename->elements.emplace_back(std::move(elem));

    InterpreterRenameQuery(rename, context, getThreadNameAndID()).execute();

    LOG_INFO(
        log,
        "Renamed table {}.{} (display name: {}) to {} with database_id={}, table_id={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info),
        new_db_info->id,
        new_table_info.id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverTable(DatabaseID database_id, TiDB::TableID table_id)
{
    auto [db_info, table_info] = getter.getDatabaseAndTableInfo(database_id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        LOG_DEBUG(log, "table is not exist in TiKV, may have been dropped, table_id={}", table_id);
        return;
    }

    if (table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto new_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRecoverPhysicalTable(db_info, new_table_info);
        }
    }

    applyRecoverPhysicalTable(db_info, table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverPhysicalTable(
    const TiDB::DBInfoPtr & db_info,
    const TiDB::TableInfoPtr & table_info)
{
    auto & tmt_context = context.getTMTContext();
    if (auto * storage = tmt_context.getStorages().get(keyspace_id, table_info->id).get(); storage)
    {
        if (!storage->isTombstone())
        {
            LOG_DEBUG(
                log,
                "Trying to recover table {} but it already exists and is not marked as tombstone, database_id={} "
                "table_id={}",
                name_mapper.debugCanonicalName(*db_info, *table_info),
                db_info->id,
                table_info->id);
            return;
        }

        LOG_DEBUG(
            log,
            "Recovering table {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(*db_info, *table_info),
            db_info->id,
            table_info->id);
        AlterCommands commands;
        {
            AlterCommand command;
            command.type = AlterCommand::RECOVER;
            commands.emplace_back(std::move(command));
        }
        auto alter_lock = storage->lockForAlter(getThreadNameAndID());
        storage->updateTombstone(
            alter_lock,
            commands,
            name_mapper.mapDatabaseName(*db_info),
            *table_info,
            name_mapper,
            context);
        LOG_INFO(
            log,
            "Created table {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(*db_info, *table_info),
            db_info->id,
            table_info->id);
        return;
    }
}

static ASTPtr parseCreateStatement(const String & statement)
{
    ParserCreateQuery parser;
    const char * pos = statement.data();
    std::string error_msg;
    auto ast = tryParseQuery(
        parser,
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
    auto mapped_db_name = name_mapper.mapDatabaseName(db_info);
    if (isReservedDatabase(context, mapped_db_name))
        throw TiFlashException(
            fmt::format("Database {} is reserved, database_id={}", name_mapper.debugDatabaseName(db_info), db_info.id),
            Errors::DDL::Internal);

    // R"raw(
    // CREATE DATABASE IF NOT EXISTS `db_xx`
    // ENGINE = TiFlash('<json-db-info>', <format-version>)
    // )raw";

    String stmt;
    WriteBufferFromString stmt_buf(stmt);
    writeString("CREATE DATABASE IF NOT EXISTS ", stmt_buf);
    writeBackQuotedString(mapped_db_name, stmt_buf);
    writeString(" ENGINE = TiFlash('", stmt_buf);
    writeEscapedString(db_info.serialize(), stmt_buf); // must escaped for json-encoded text
    writeString("', ", stmt_buf);
    writeIntText(DatabaseTiFlash::CURRENT_VERSION, stmt_buf);
    writeString(")", stmt_buf);
    return stmt;
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
void SchemaBuilder<Getter, NameMapper>::applyCreateSchema(const TiDB::DBInfoPtr & db_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_db).Increment();
    LOG_INFO(log, "Creating database {} with database_id={}", name_mapper.debugDatabaseName(*db_info), db_info->id);

    auto statement = createDatabaseStmt(context, *db_info, name_mapper);

    ASTPtr ast = parseCreateStatement(statement);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    {
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_databases);
        databases.emplace(db_info->id, db_info);
    }

    LOG_INFO(log, "Created database {} with database_id={}", name_mapper.debugDatabaseName(*db_info), db_info->id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(DatabaseID schema_id)
{
    TiDB::DBInfoPtr db_info;
    {
        std::shared_lock<std::shared_mutex> shared_lock(shared_mutex_for_databases);
        auto it = databases.find(schema_id);
        if (unlikely(it == databases.end()))
        {
            LOG_INFO(log, "Try to drop database but not found, may has been dropped, database_id={}", schema_id);
            return;
        }
        db_info = it->second;
    }

    {
        //TODO: it seems may need a lot time, maybe we can do it in a background thread
        auto table_ids = table_id_map.findTablesByDatabaseID(schema_id);
        for (auto table_id : table_ids)
            applyDropTable(schema_id, table_id);
    }

    applyDropSchema(name_mapper.mapDatabaseName(*db_info));

    {
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_databases);
        databases.erase(schema_id);
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(const String & db_name)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_db).Increment();
    LOG_INFO(log, "Tombstoning database {}", db_name);
    auto db = context.tryGetDatabase(db_name);
    if (db == nullptr)
    {
        LOG_INFO(log, "Database {} does not exists", db_name);
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

    LOG_INFO(log, "Tombstoned database {}", db_name);
}

std::tuple<NamesAndTypes, Strings> parseColumnsFromTableInfo(const TiDB::TableInfo & table_info)
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
        throw TiFlashException(
            fmt::format("Unknown engine type : {}", static_cast<int32_t>(table_info.engine_type)),
            Errors::DDL::Internal);
    }

    return stmt;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreatePhysicalTable(
    const TiDB::DBInfoPtr & db_info,
    const TableInfoPtr & table_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(
        log,
        "Creating table {} with database_id={}, table_id={}",
        name_mapper.debugCanonicalName(*db_info, *table_info),
        db_info->id,
        table_info->id);

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto * storage = tmt_context.getStorages().get(keyspace_id, table_info->id).get(); storage)
        {
            if (!storage->isTombstone())
            {
                LOG_DEBUG(
                    log,
                    "Trying to create table {}, but it already exists and is not marked as tombstone, database_id={} "
                    "table_id={}",
                    name_mapper.debugCanonicalName(*db_info, *table_info),
                    db_info->id,
                    table_info->id);
                return;
            }

            LOG_DEBUG(
                log,
                "Recovering table {} with database_id={}, table_id={}",
                name_mapper.debugCanonicalName(*db_info, *table_info),
                db_info->id,
                table_info->id);
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            auto alter_lock = storage->lockForAlter(getThreadNameAndID());
            storage->updateTombstone(
                alter_lock,
                commands,
                name_mapper.mapDatabaseName(*db_info),
                *table_info,
                name_mapper,
                context);
            LOG_INFO(
                log,
                "Created table {}, database_id={} table_id={}",
                name_mapper.debugCanonicalName(*db_info, *table_info),
                db_info->id,
                table_info->id);
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

    LOG_INFO(
        log,
        "Creating table {} (database_id={} table_id={}) with statement: {}",
        name_mapper.debugCanonicalName(*db_info, *table_info),
        db_info->id,
        table_info->id,
        stmt);

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
    LOG_INFO(
        log,
        "Created table {}, database_id={} table_id={}",
        name_mapper.debugCanonicalName(*db_info, *table_info),
        db_info->id,
        table_info->id);
}


template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropPhysicalTable(const String & db_name, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_DEBUG(log, "table {} does not exist.", table_id);
        return;
    }
    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_table).Increment();
    LOG_INFO(
        log,
        "Tombstoning table {}.{}, table_id={}",
        db_name,
        name_mapper.debugTableName(storage->getTableInfo()),
        table_id);

    // TODO:try to optimize alterCommands
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
    auto alter_lock = storage->lockForAlter(getThreadNameAndID());
    storage->updateTombstone(alter_lock, commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(
        log,
        "Tombstoned table {}.{}, table_id={}",
        db_name,
        name_mapper.debugTableName(storage->getTableInfo()),
        table_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTable(DatabaseID database_id, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto * storage = tmt_context.getStorages().get(keyspace_id, table_id).get();
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
            applyDropPhysicalTable(name_mapper.mapDatabaseName(database_id, keyspace_id), part_def.id);
        }
    }

    // Drop logical table at last, only logical table drop will be treated as "complete".
    // Intermediate failure will hide the logical table drop so that schema syncing when restart will re-drop all (despite some physical tables may have dropped).
    applyDropPhysicalTable(name_mapper.mapDatabaseName(database_id, keyspace_id), table_info.id);
}

/// syncAllSchema will be called when a new keyspace is created or we meet diff->regenerate_schema_map = true.
/// Thus, we should not assume all the map is empty during syncAllSchema.
template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::syncAllSchema()
{
    LOG_INFO(log, "Syncing all schemas.");

    /// Create all databases.
    std::vector<DBInfoPtr> all_schemas = getter.listDBs();

    std::unordered_set<String> created_db_set;

    //We can't use too large default_num_threads, otherwise, the lock grabbing time will be too much.
    size_t default_num_threads = std::max(4UL, std::thread::hardware_concurrency());
    auto sync_all_schema_thread_pool
        = ThreadPool(default_num_threads, default_num_threads / 2, default_num_threads * 2);
    auto sync_all_schema_wait_group = sync_all_schema_thread_pool.waitGroup();

    std::mutex created_db_set_mutex;
    for (const auto & db : all_schemas)
    {
        auto task = [this, db, &created_db_set, &created_db_set_mutex] {
            {
                std::shared_lock<std::shared_mutex> shared_lock(shared_mutex_for_databases);
                if (databases.find(db->id) == databases.end())
                {
                    shared_lock.unlock();
                    applyCreateSchema(db);
                    {
                        std::unique_lock<std::mutex> created_db_set_lock(created_db_set_mutex);
                        created_db_set.emplace(name_mapper.mapDatabaseName(*db));
                    }

                    LOG_DEBUG(
                        log,
                        "Database {} created during sync all schemas, database_id={}",
                        name_mapper.debugDatabaseName(*db),
                        db->id);
                }
            }

            std::vector<TableInfoPtr> tables = getter.listTables(db->id);
            for (auto & table : tables)
            {
                LOG_INFO(
                    log,
                    "Table {} syncing during sync all schemas, database_id={} table_id={}",
                    name_mapper.debugCanonicalName(*db, *table),
                    db->id,
                    table->id);

                /// Ignore view and sequence.
                if (table->is_view || table->is_sequence)
                {
                    LOG_INFO(
                        log,
                        "Table {} is a view or sequence, ignoring. database_id={} table_id={}",
                        name_mapper.debugCanonicalName(*db, *table),
                        db->id,
                        table->id);
                    continue;
                }

                table_id_map.emplaceTableID(table->id, db->id);
                LOG_DEBUG(log, "register table to table_id_map, database_id={} table_id={}", db->id, table->id);

                applyCreatePhysicalTable(db, table);
                if (table->isLogicalPartitionTable())
                {
                    for (const auto & part_def : table->partition.definitions)
                    {
                        LOG_DEBUG(
                            log,
                            "register table to table_id_map for partition table, logical_table_id={} "
                            "physical_table_id={}",
                            table->id,
                            part_def.id);
                        table_id_map.emplacePartitionTableID(part_def.id, table->id);
                    }
                }
            }
        };
        sync_all_schema_wait_group->schedule(task);
    }
    sync_all_schema_wait_group->wait();

    // TODO:can be removed if we don't save the .sql
    /// Drop all unmapped tables.
    auto storage_map = context.getTMTContext().getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        const auto & table_info = it->second->getTableInfo();
        if (table_info.keyspace_id != keyspace_id)
            continue;

        if (!table_id_map.tableIDInTwoMaps(table_info.id))
        {
            applyDropPhysicalTable(it->second->getDatabaseName(), table_info.id);
            LOG_DEBUG(
                log,
                "Table {}.{} dropped during sync all schemas, table_id={}",
                it->second->getDatabaseName(),
                name_mapper.debugTableName(table_info),
                table_info.id);
        }
    }

    /// Drop all unmapped dbs.
    const auto & dbs = context.getDatabases();
    for (auto it = dbs.begin(); it != dbs.end(); it++)
    {
        auto db_keyspace_id = SchemaNameMapper::getMappedNameKeyspaceID(it->first);
        if (db_keyspace_id != keyspace_id)
        {
            continue;
        }
        if (created_db_set.count(it->first) == 0 && !isReservedDatabase(context, it->first))
        {
            applyDropSchema(it->first);
            LOG_DEBUG(log, "DB {} dropped during sync all schemas", it->first);
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyTable(
    DatabaseID database_id,
    TableID logical_table_id,
    TableID physical_table_id)
{
    auto table_info = getter.getTableInfo(database_id, logical_table_id);
    if (table_info == nullptr)
    {
        LOG_ERROR(log, "table is not exist in TiKV, database_id={} logical_table_id={}", database_id, logical_table_id);
        return;
    }

    if (logical_table_id != physical_table_id)
    {
        if (!table_info->isLogicalPartitionTable())
        {
            LOG_ERROR(
                log,
                "new table in TiKV is not partition table {}, database_id={} table_id={}",
                name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
                database_id,
                table_info->id);
            return;
        }
        try
        {
            table_info = table_info->producePartitionTableInfo(physical_table_id, name_mapper);
        }
        catch (const Exception & e)
        {
            /// when we do a ddl and insert, then we do reorganize partition.
            /// Besides, reorganize reach tiflash before insert, so when insert,
            /// the old partition_id is not exist, so we just ignore it.
            LOG_WARNING(
                log,
                "producePartitionTableInfo meet exception : {} \n stack is {}",
                e.displayText(),
                e.getStackTrace().toString());
            return;
        }
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, physical_table_id);
    if (storage == nullptr)
    {
        auto db_info = getter.getDatabase(database_id);
        if (db_info == nullptr)
        {
            LOG_ERROR(log, "database is not exist in TiKV, database_id={}", database_id);
            return;
        }

        applyCreatePhysicalTable(db_info, table_info);
    }
    else
    {
        LOG_INFO(
            log,
            "Altering table {}, database_id={} table_id={}",
            name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
            database_id,
            table_info->id);
        GET_METRIC(tiflash_schema_internal_ddl_count, type_modify_column).Increment();
        auto alter_lock = storage->lockForAlter(getThreadNameAndID());

        storage->alterSchemaChange(
            alter_lock,
            *table_info,
            name_mapper.mapDatabaseName(database_id, keyspace_id),
            name_mapper.mapTableName(*table_info),
            context);
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::dropAllSchema()
{
    LOG_INFO(log, "Dropping all schemas.");

    auto & tmt_context = context.getTMTContext();

    /// Drop all tables.
    const auto storage_map = tmt_context.getStorages().getAllStorage();
    for (const auto & storage : storage_map)
    {
        auto table_info = storage.second->getTableInfo();
        if (table_info.keyspace_id != keyspace_id)
        {
            continue;
        }
        applyDropPhysicalTable(storage.second->getDatabaseName(), table_info.id);
        LOG_DEBUG(
            log,
            "Table {}.{} dropped during drop all schemas, table_id={}",
            storage.second->getDatabaseName(),
            name_mapper.debugTableName(table_info),
            table_info.id);
    }

    /// Drop all dbs.
    const auto & dbs = context.getDatabases();
    for (const auto & db : dbs)
    {
        auto db_keyspace_id = SchemaNameMapper::getMappedNameKeyspaceID(db.first);
        if (db_keyspace_id != keyspace_id)
        {
            continue;
        }
        applyDropSchema(db.first);
        LOG_DEBUG(log, "DB {} dropped during drop all schemas", db.first);
    }

    LOG_INFO(log, "Dropped all schemas.");
}

// product env
template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
// mock test
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;
// unit test
template struct SchemaBuilder<MockSchemaGetter, SchemaNameMapper>;
// end namespace
} // namespace DB
