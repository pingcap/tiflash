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

#include <Common/Exception.h>
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
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

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
    TableInfoPtr table_info;
    bool get_by_mvcc = false;
    std::tie(table_info, get_by_mvcc) = getter.getTableInfoAndCheckMvcc(database_id, table_id);
    if (table_info == nullptr)
    {
        LOG_INFO(
            log,
            "table is not exist in TiKV, may have been dropped, applyCreateTable is ignored, database_id={} "
            "table_id={}",
            database_id,
            table_id);
        return;
    }

    table_id_map.emplaceTableID(table_id, database_id);
    LOG_DEBUG(log, "register table to table_id_map, database_id={} table_id={}", database_id, table_id);

    // non partition table, done
    if (!table_info->isLogicalPartitionTable())
    {
        return;
    }

    // If table is partition table, we will create the Storage instance for the logical table
    // here (and store the table info to local).
    // Because `applyPartitionDiffOnLogicalTable` need the logical table for comparing
    // the latest partitioning and the local partitioning in table info to apply the changes.
    applyCreateStorageInstance(database_id, table_info, get_by_mvcc);

    // Register the partition_id -> logical_table_id mapping
    for (const auto & part_def : table_info->partition.definitions)
    {
        LOG_DEBUG(
            log,
            "register table to table_id_map for partition table, database_id={} logical_table_id={} "
            "physical_table_id={}",
            database_id,
            table_id,
            part_def.id);
        table_id_map.emplacePartitionTableID(part_def.id, table_id);
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

    if (diff.affected_opts.empty())
    {
        throw TiFlashException(
            Errors::DDL::Internal,
            "Invalid exchange partition schema diff without affected_opts, affected_opts_size={}",
            diff.affected_opts.size());
    }

    /// `ALTER TABLE partition_table EXCHANGE PARTITION partition_name WITH TABLE non_partition_table`
    /// Table_id in diff is the partition id of which will be exchanged
    /// Schema_id in diff is the non-partition table's schema id
    /// Old_table_id in diff is the non-partition table's table id
    /// Table_id in diff.affected_opts[0] is the table id of the partition table
    /// Schema_id in diff.affected_opts[0] is the schema id of the partition table
    const auto non_partition_database_id = diff.old_schema_id;
    const auto non_partition_table_id = diff.old_table_id;
    const auto partition_database_id = diff.affected_opts[0].schema_id;
    const auto partition_logical_table_id = diff.affected_opts[0].table_id;
    const auto partition_physical_table_id = diff.table_id;
    LOG_INFO(
        log,
        "Execute exchange partition begin. database_id={} table_id={} part_database_id={} part_logical_table_id={}"
        " physical_table_id={}",
        non_partition_database_id,
        non_partition_table_id,
        partition_database_id,
        partition_logical_table_id,
        partition_physical_table_id);
    GET_METRIC(tiflash_schema_internal_ddl_count, type_exchange_partition).Increment();

    table_id_map.exchangeTablePartition(
        non_partition_database_id,
        non_partition_table_id,
        partition_database_id,
        partition_logical_table_id,
        partition_physical_table_id);

    if (non_partition_database_id != partition_database_id)
    {
        // Rename old non-partition table belonging new database. Now it should be belong to
        // the database of partition table.
        auto & tmt_context = context.getTMTContext();
        do
        {
            // skip if the instance is not created
            auto storage = tmt_context.getStorages().get(keyspace_id, non_partition_table_id);
            if (storage == nullptr)
            {
                LOG_INFO(
                    log,
                    "ExchangeTablePartition: non_partition_table instance is not created in TiFlash"
                    ", rename is ignored, table_id={}",
                    non_partition_table_id);
                break;
            }

            auto new_table_info = getter.getTableInfo(partition_database_id, partition_logical_table_id);
            if (unlikely(new_table_info == nullptr))
            {
                LOG_INFO(
                    log,
                    "ExchangeTablePartition: part_logical_table table is not exist in TiKV, rename is ignored,"
                    " table_id={}",
                    partition_logical_table_id);
                break;
            }

            String new_db_display_name = tryGetDatabaseDisplayNameFromLocal(partition_database_id);
            auto part_table_info = new_table_info->producePartitionTableInfo(non_partition_table_id, name_mapper);
            applyRenamePhysicalTable(partition_database_id, new_db_display_name, *part_table_info, storage);
        } while (false);

        // Rename the exchanged partition table belonging new database. Now it should belong to
        // the database of non-partition table
        do
        {
            // skip if the instance is not created
            auto storage = tmt_context.getStorages().get(keyspace_id, partition_physical_table_id);
            if (storage == nullptr)
            {
                LOG_INFO(
                    log,
                    "ExchangeTablePartition: partition_physical_table instance is not created in TiFlash"
                    ", rename is ignored, table_id={}",
                    partition_physical_table_id);
                break;
            }

            auto new_table_info = getter.getTableInfo(non_partition_database_id, partition_physical_table_id);
            if (unlikely(new_table_info == nullptr))
            {
                LOG_INFO(
                    log,
                    "ExchangeTablePartition: partition_physical_table is not exist in TiKV, rename is ignored,"
                    " table_id={}",
                    partition_physical_table_id);
                break;
            }

            String new_db_display_name = tryGetDatabaseDisplayNameFromLocal(non_partition_database_id);
            applyRenamePhysicalTable(non_partition_database_id, new_db_display_name, *new_table_info, storage);
        } while (false);
    }

    LOG_INFO(
        log,
        "Execute exchange partition end. database_id={} table_id={} part_database_id={} part_logical_table_id={}"
        " physical_table_id={}",
        non_partition_database_id,
        non_partition_table_id,
        partition_database_id,
        partition_logical_table_id,
        partition_physical_table_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDiff(const SchemaDiff & diff)
{
    LOG_TRACE(log, "applyDiff accept type={}", magic_enum::enum_name(diff.type));
    switch (diff.type)
    {
    case SchemaActionType::CreateSchema:
    {
        applyCreateDatabase(diff.schema_id);
        break;
    }
    case SchemaActionType::DropSchema:
    {
        applyDropDatabase(diff.schema_id);
        break;
    }
    case SchemaActionType::ActionRecoverSchema:
    {
        applyRecoverDatabase(diff.schema_id);
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
            // Create the new table.
            // If the new table is a partition table, this will also overwrite
            // the partition id mapping to the new logical table
            applyCreateTable(diff.schema_id, diff.table_id);
            // Drop the old table. if the previous partitions of the old table are
            // not mapping to the old logical table now, they will not be removed.
            applyDropTable(diff.schema_id, diff.old_table_id);
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
            LOG_INFO(log, "Ignore change type: {}, diff_version={}", magic_enum::enum_name(diff.type), diff.version);
        }
        else
        {
            // >= SchemaActionType::MaxRecognizedType
            // log down the Int8 value directly
            LOG_ERROR(log, "Unsupported change type: {}, diff_version={}", fmt::underlying(diff.type), diff.version);
        }

        break;
    }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplica(DatabaseID database_id, TableID table_id)
{
    auto table_info = getter.getTableInfo(database_id, table_id);
    if (unlikely(table_info == nullptr))
    {
        LOG_WARNING(log, "table is not exist in TiKV, applySetTiFlashReplica is ignored, table_id={}", table_id);
        return;
    }

    auto & tmt_context = context.getTMTContext();
    if (table_info->replica_info.count == 0)
    {
        // if set 0, drop table in TiFlash
        auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
        if (unlikely(storage == nullptr))
        {
            LOG_ERROR(
                log,
                "Storage instance is not exist in TiFlash, applySetTiFlashReplica is ignored, table_id={}",
                table_id);
            return;
        }

        applyDropTable(database_id, table_id);
        return;
    }

    assert(table_info->replica_info.count != 0);
    // Replica info is set to non-zero, create the storage if not exists.
    auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
    if (storage == nullptr)
    {
        if (!table_id_map.tableIDInDatabaseIdMap(table_id))
        {
            applyCreateTable(database_id, table_id);
        }
        return;
    }

    // Recover the table if tombstone
    if (storage->isTombstone())
    {
        applyRecoverLogicalTable(database_id, table_info);
        return;
    }

    auto local_logical_storage_table_info = storage->getTableInfo(); // copy
    // Check whether replica_count and available changed
    if (const auto & local_logical_storage_replica_info = local_logical_storage_table_info.replica_info;
        local_logical_storage_replica_info.count == table_info->replica_info.count
        && local_logical_storage_replica_info.available == table_info->replica_info.available)
    {
        return; // nothing changed
    }

    size_t old_replica_count = 0;
    size_t new_replica_count = 0;
    if (table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto new_part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            auto part_storage = tmt_context.getStorages().get(keyspace_id, new_part_table_info->id);
            if (part_storage == nullptr)
            {
                table_id_map.emplacePartitionTableID(part_def.id, table_id);
                continue;
            }
            {
                auto alter_lock = part_storage->lockForAlter(getThreadNameAndID());
                auto local_table_info = part_storage->getTableInfo(); // copy
                old_replica_count = local_table_info.replica_info.count;
                new_replica_count = new_part_table_info->replica_info.count;
                // Only update the replica info, do not change other fields. Or it may
                // lead to other DDL is unexpectedly ignored.
                local_table_info.replica_info = new_part_table_info->replica_info;
                part_storage->alterSchemaChange(
                    alter_lock,
                    local_table_info,
                    name_mapper.mapDatabaseName(database_id, keyspace_id),
                    name_mapper.mapTableName(local_table_info),
                    context);
            }
            LOG_INFO(
                log,
                "Updating replica info, replica count old={} new={} available={}"
                " physical_table_id={} logical_table_id={}",
                old_replica_count,
                new_replica_count,
                table_info->replica_info.available,
                part_def.id,
                table_id);
        }
    }

    {
        auto alter_lock = storage->lockForAlter(getThreadNameAndID());
        old_replica_count = local_logical_storage_table_info.replica_info.count;
        new_replica_count = table_info->replica_info.count;
        // Only update the replica info, do not change other fields. Or it may
        // lead to other DDL is unexpectedly ignored.
        local_logical_storage_table_info.replica_info = table_info->replica_info;
        storage->alterSchemaChange(
            alter_lock,
            local_logical_storage_table_info,
            name_mapper.mapDatabaseName(database_id, keyspace_id),
            name_mapper.mapTableName(local_logical_storage_table_info),
            context);
    }
    LOG_INFO(
        log,
        "Updating replica info, replica count old={} new={} available={}"
        " physical_table_id={} logical_table_id={}",
        old_replica_count,
        new_replica_count,
        table_info->replica_info.available,
        table_id,
        table_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(DatabaseID database_id, TableID table_id)
{
    auto table_info = getter.getTableInfo(database_id, table_id);
    if (unlikely(table_info == nullptr))
    {
        LOG_ERROR(log, "table is not exist in TiKV, applyPartitionDiff is ignored, table_id={}", table_id);
        return;
    }
    if (!table_info->isLogicalPartitionTable())
    {
        LOG_ERROR(
            log,
            "new table in TiKV is not a partition table {}, database_id={} table_id={}",
            name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
            database_id,
            table_info->id);
        return;
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
    if (storage == nullptr)
    {
        LOG_ERROR(
            log,
            "logical_table storage instance is not exist in TiFlash, applyPartitionDiff is ignored, table_id={}",
            table_id);
        return;
    }

    applyPartitionDiffOnLogicalTable(database_id, table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiffOnLogicalTable(
    const DatabaseID database_id,
    const TableInfoPtr & table_info,
    const ManageableStoragePtr & storage)
{
    const auto & local_table_info = storage->getTableInfo();
    // ALTER TABLE t PARTITION BY ... may turn a non-partition table into partition table
    // with some partition ids in `partition.adding_definitions`/`partition.definitions`
    // and `partition.dropping_definitions`. We need to create those partitions.
    if (!local_table_info.isLogicalPartitionTable())
    {
        LOG_INFO(
            log,
            "Altering non-partition table to be a partition table {} with database_id={}, table_id={}",
            name_mapper.debugCanonicalName(local_table_info, database_id, keyspace_id),
            database_id,
            local_table_info.id);
    }

    const auto & local_defs = local_table_info.partition.definitions;
    const auto & new_defs = table_info->partition.definitions;

    std::unordered_set<TableID> local_part_id_set, new_part_id_set;
    std::for_each(local_defs.begin(), local_defs.end(), [&local_part_id_set](const auto & def) {
        local_part_id_set.emplace(def.id);
    });
    std::for_each(new_defs.begin(), new_defs.end(), [&new_part_id_set](const auto & def) {
        new_part_id_set.emplace(def.id);
    });

    LOG_INFO(
        log,
        "Applying partition changes {} with database_id={}, table_id={}, old: {}, new: {}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id,
        local_part_id_set,
        new_part_id_set);

    if (local_part_id_set == new_part_id_set)
    {
        LOG_INFO(
            log,
            "No partition changes, partitions_size={} {} with database_id={}, table_id={}",
            new_part_id_set.size(),
            name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
            database_id,
            table_info->id);
        return;
    }

    // Copy the local table info and update fields on the copy
    auto updated_table_info = local_table_info;
    updated_table_info.is_partition_table = true;
    updated_table_info.belonging_table_id = table_info->belonging_table_id;
    updated_table_info.partition = table_info->partition;

    /// Apply changes to physical tables.
    for (const auto & local_def : local_defs)
    {
        if (!new_part_id_set.contains(local_def.id))
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(database_id, keyspace_id), local_def.id);
        }
    }

    for (const auto & new_def : new_defs)
    {
        if (!local_part_id_set.contains(new_def.id))
        {
            table_id_map.emplacePartitionTableID(new_def.id, updated_table_info.id);
        }
    }

    auto alter_lock = storage->lockForAlter(getThreadNameAndID());
    storage->alterSchemaChange(
        alter_lock,
        updated_table_info,
        name_mapper.mapDatabaseName(database_id, keyspace_id),
        name_mapper.mapTableName(updated_table_info),
        context);

    GET_METRIC(tiflash_schema_internal_ddl_count, type_apply_partition).Increment();
    LOG_INFO(
        log,
        "Applied partition changes {} with database_id={}, table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(DatabaseID database_id, TableID table_id)
{
    // update the table_id_map no matter storage instance is created or not
    table_id_map.emplaceTableID(table_id, database_id);

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_WARNING(
            log,
            "Storage instance is not exist in TiFlash, applyRenameTable is ignored, table_id={}",
            table_id);
        return;
    }

    auto new_table_info = getter.getTableInfo(database_id, table_id);
    if (unlikely(new_table_info == nullptr))
    {
        LOG_ERROR(log, "table is not exist in TiKV, applyRenameTable is ignored, table_id={}", table_id);
        return;
    }

    String new_db_display_name = tryGetDatabaseDisplayNameFromLocal(database_id);
    applyRenameLogicalTable(database_id, new_db_display_name, new_table_info, storage);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameLogicalTable(
    const DatabaseID new_database_id,
    const String & new_database_display_name,
    const TableInfoPtr & new_table_info,
    const ManageableStoragePtr & storage)
{
    applyRenamePhysicalTable(new_database_id, new_database_display_name, *new_table_info, storage);

    if (new_table_info->isLogicalPartitionTable())
    {
        auto & tmt_context = context.getTMTContext();
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto part_storage = tmt_context.getStorages().get(keyspace_id, part_def.id);
            if (part_storage == nullptr)
            {
                LOG_ERROR(
                    log,
                    "Storage instance is not exist in TiFlash, applyRenamePhysicalTable is ignored, "
                    "physical_table_id={} logical_table_id={}",
                    part_def.id,
                    new_table_info->id);
                return;
            }
            auto part_table_info = new_table_info->producePartitionTableInfo(part_def.id, name_mapper);
            applyRenamePhysicalTable(new_database_id, new_database_display_name, *part_table_info, part_storage);
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenamePhysicalTable(
    const DatabaseID new_database_id,
    const String & new_database_display_name,
    const TableInfo & new_table_info,
    const ManageableStoragePtr & storage)
{
    const auto old_mapped_db_name = storage->getDatabaseName();
    const auto new_mapped_db_name = name_mapper.mapDatabaseName(new_database_id, keyspace_id);
    const auto old_display_table_name = name_mapper.displayTableName(storage->getTableInfo());
    const auto new_display_table_name = name_mapper.displayTableName(new_table_info);
    if (old_mapped_db_name == new_mapped_db_name && old_display_table_name == new_display_table_name)
    {
        LOG_DEBUG(
            log,
            "Table {} name identical, not renaming. database_id={} table_id={}",
            name_mapper.debugCanonicalName(new_table_info, new_database_id, keyspace_id),
            new_database_id,
            new_table_info.id);
        return;
    }

    const auto old_mapped_tbl_name = storage->getTableName();
    GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_table).Increment();
    LOG_INFO(
        log,
        "Rename table {}.{} (display name: {}) to {} begin, database_id={} table_id={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(new_table_info, new_database_id, keyspace_id),
        new_database_id,
        new_table_info.id);

    // Note that rename will update table info in table create statement by modifying original table info
    // with "tidb_display.table" instead of using new_table_info directly, so that other changes
    // (ALTER commands) won't be saved. Besides, no need to update schema_version as table name is not structural.
    auto rename = std::make_shared<ASTRenameQuery>();
    ASTRenameQuery::Element elem{
        .from = ASTRenameQuery::Table{old_mapped_db_name, old_mapped_tbl_name},
        .to = ASTRenameQuery::Table{new_mapped_db_name, name_mapper.mapTableName(new_table_info)},
        .tidb_display = ASTRenameQuery::Table{new_database_display_name, new_display_table_name},
    };
    rename->elements.emplace_back(std::move(elem));

    InterpreterRenameQuery(rename, context, getThreadNameAndID()).execute();

    LOG_INFO(
        log,
        "Rename table {}.{} (display name: {}) to {} end, database_id={} table_id={}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(new_table_info, new_database_id, keyspace_id),
        new_database_id,
        new_table_info.id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverTable(DatabaseID database_id, TiDB::TableID table_id)
{
    auto table_info = getter.getTableInfo(database_id, table_id);
    if (unlikely(table_info == nullptr))
    {
        // this table is dropped.
        LOG_INFO(
            log,
            "table is not exist in TiKV, may have been dropped, recover table is ignored, table_id={}",
            table_id);
        return;
    }

    applyRecoverLogicalTable(database_id, table_info);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverLogicalTable(
    const DatabaseID database_id,
    const TiDB::TableInfoPtr & table_info)
{
    assert(table_info != nullptr);
    if (table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info->partition.definitions)
        {
            auto part_table_info = table_info->producePartitionTableInfo(part_def.id, name_mapper);
            tryRecoverPhysicalTable(database_id, part_table_info);
        }
    }

    tryRecoverPhysicalTable(database_id, table_info);
}

// Return true - the Storage instance exists and is recovered (or not tombstone)
//        false - the Storage instance does not exist
template <typename Getter, typename NameMapper>
bool SchemaBuilder<Getter, NameMapper>::tryRecoverPhysicalTable(
    const DatabaseID database_id,
    const TiDB::TableInfoPtr & table_info)
{
    assert(table_info != nullptr);
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
    if (storage == nullptr)
    {
        LOG_INFO(
            log,
            "Storage instance does not exist, tryRecoverPhysicalTable is ignored, table_id={}",
            table_info->id);
        return false;
    }

    if (!storage->isTombstone())
    {
        LOG_INFO(
            log,
            "Trying to recover table {} but it is not marked as tombstone, skip, database_id={} table_id={}",
            name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
            database_id,
            table_info->id);
        return true;
    }

    LOG_INFO(
        log,
        "Create table {} by recover begin, database_id={} table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
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
        name_mapper.mapDatabaseName(database_id, keyspace_id),
        *table_info,
        name_mapper,
        context);
    LOG_INFO(
        log,
        "Create table {} by recover end, database_id={} table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id);
    return true;
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
bool SchemaBuilder<Getter, NameMapper>::applyCreateDatabase(DatabaseID database_id)
{
    auto db_info = getter.getDatabase(database_id);
    if (unlikely(db_info == nullptr))
    {
        LOG_INFO(
            log,
            "Create database is ignored because database is not exist in TiKV,"
            " database_id={}",
            database_id);
        return false;
    }
    applyCreateDatabaseByInfo(db_info);
    return true;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateDatabaseByInfo(const TiDB::DBInfoPtr & db_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_db).Increment();
    LOG_INFO(log, "Create database {} begin, database_id={}", name_mapper.debugDatabaseName(*db_info), db_info->id);

    auto statement = createDatabaseStmt(context, *db_info, name_mapper);

    ASTPtr ast = parseCreateStatement(statement);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();

    databases.addDatabaseInfo(db_info);

    LOG_INFO(log, "Create database {} end, database_id={}", name_mapper.debugDatabaseName(*db_info), db_info->id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRecoverDatabase(DatabaseID database_id)
{
    auto db_info = getter.getDatabase(database_id);
    if (unlikely(db_info == nullptr))
    {
        LOG_INFO(
            log,
            "Recover database is ignored because database is not exist in TiKV,"
            " database_id={}",
            database_id);
        return;
    }
    LOG_INFO(log, "Recover database begin, database_id={}", database_id);
    auto db_name = name_mapper.mapDatabaseName(database_id, keyspace_id);
    auto db = context.tryGetDatabase(db_name);
    if (unlikely(!db))
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
        //TODO: it seems may need a lot time, maybe we can do it in a background thread
        auto table_ids = table_id_map.findTablesByDatabaseID(database_id);
        for (auto table_id : table_ids)
        {
            auto table_info = getter.getTableInfo(database_id, table_id);
            applyRecoverLogicalTable(database_id, table_info);
        }
    }

    // Usually `FLASHBACK DATABASE ... TO ...` will rename the database
    db->alterTombstone(context, 0, db_info);
    databases.addDatabaseInfo(db_info); // add back database info cache
    LOG_INFO(log, "Recover database end, database_id={}", database_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropDatabase(DatabaseID database_id)
{
    // The DDL in TiFlash comes after user executed `DROP DATABASE ...`. So the meta key could
    // have already been deleted. In order to handle this situation, we should not fetch the
    // `DatabaseInfo` from TiKV.
    {
        //TODO: it seems may need a lot time, maybe we can do it in a background thread
        auto table_ids = table_id_map.findTablesByDatabaseID(database_id);
        for (auto table_id : table_ids)
            applyDropTable(database_id, table_id);
    }

    applyDropDatabaseByName(name_mapper.mapDatabaseName(database_id, keyspace_id));

    databases.eraseDBInfo(database_id);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropDatabaseByName(const String & db_name)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_db).Increment();
    LOG_INFO(log, "Tombstone database begin, db_name={}", db_name);
    auto db = context.tryGetDatabase(db_name);
    if (db == nullptr)
    {
        LOG_INFO(log, "Database does not exist, db_name={}", db_name);
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
    db->alterTombstone(context, tombstone, /*new_db_info*/ nullptr); // keep the old db_info

    LOG_INFO(log, "Tombstone database end, db_name={} tombstone={}", db_name, tombstone);
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
    const KeyspaceID keyspace_id,
    const DatabaseID database_id,
    const TableInfo & table_info,
    const SchemaNameMapper & name_mapper,
    const UInt64 tombstone,
    const LoggerPtr & log)
{
    LOG_DEBUG(log, "Analyzing table info : {}", table_info.serialize());
    auto [columns, pks] = parseColumnsFromTableInfo(table_info);

    String stmt;
    WriteBufferFromString stmt_buf(stmt);
    writeString("CREATE TABLE ", stmt_buf);
    writeBackQuotedString(name_mapper.mapDatabaseName(database_id, keyspace_id), stmt_buf);
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
        writeString(fmt::format("', {})", tombstone), stmt_buf);
    }
    else
    {
        throw TiFlashException(
            Errors::DDL::Internal,
            "Unknown engine type : {}",
            fmt::underlying(table_info.engine_type));
    }

    return stmt;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyCreateStorageInstance(
    const DatabaseID database_id,
    const TableInfoPtr & table_info,
    bool is_tombstone)
{
    assert(table_info != nullptr);

    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(
        log,
        "Create table {} begin, database_id={}, table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id);

    /// Try to recover the existing storage instance
    if (tryRecoverPhysicalTable(database_id, table_info))
    {
        return;
    }
    // Else the storage instance does not exist, create it.
    /// Normal CREATE table.
    if (table_info->engine_type == StorageEngine::UNSPECIFIED)
    {
        auto & tmt_context = context.getTMTContext();
        table_info->engine_type = tmt_context.getEngineType();
    }

    // We need to create a Storage instance to handle its raft log and snapshot when it
    // is "dropped" but not physically removed in TiDB. To handle it porperly, we get a
    // tso from PD to create the table. The tso must be newer than what "DROP TABLE" DDL
    // is executed. So when the gc-safepoint is larger than tombstone_ts, the table can
    // be safe to physically drop on TiFlash.
    UInt64 tombstone_ts = 0;
    if (is_tombstone)
    {
        tombstone_ts = context.getTMTContext().getPDClient()->getTS();
    }

    String stmt = createTableStmt(keyspace_id, database_id, *table_info, name_mapper, tombstone_ts, log);

    LOG_INFO(
        log,
        "Create table {} (database_id={} table_id={}) with statement: {}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id,
        stmt);

    // If "CREATE DATABASE" is executed in TiFlash after user has executed "DROP DATABASE"
    // in TiDB, then TiFlash may not create the IDatabase instance. Make sure we can access
    // to the IDatabase when creating IStorage.
    const auto database_mapped_name = name_mapper.mapDatabaseName(database_id, keyspace_id);
    if (!context.isDatabaseExist(database_mapped_name))
    {
        LOG_WARNING(
            log,
            "database instance is not exist (applyCreateStorageInstance), may has been dropped, create a database with "
            "fake DatabaseInfo for it, database_id={} database_name={}",
            database_id,
            database_mapped_name);
        // The database is dropped in TiKV and we can not fetch it. Generate a fake
        // DatabaseInfo for it. It is OK because the DatabaseInfo will be updated
        // when the database is `FLASHBACK`.
        TiDB::DBInfoPtr database_info = std::make_shared<TiDB::DBInfo>();
        database_info->id = database_id;
        database_info->keyspace_id = keyspace_id;
        database_info->name = database_mapped_name; // use the mapped name because we done known the actual name
        database_info->charset = "utf8mb4"; // default value
        database_info->collate = "utf8mb4_bin"; // default value
        database_info->state = TiDB::StateNone; // special state
        applyCreateDatabaseByInfo(database_info);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info->name, 0);

    auto * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
    ast_create_query->attach = true;
    ast_create_query->if_not_exists = true;
    ast_create_query->database = database_mapped_name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
    LOG_INFO(
        log,
        "Creat table {} end, database_id={} table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id);
}


template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropPhysicalTable(const String & db_name, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_INFO(log, "Storage instance does not exist, applyDropPhysicalTable is ignored, table_id={}", table_id);
        return;
    }

    GET_METRIC(tiflash_schema_internal_ddl_count, type_drop_table).Increment();
    LOG_INFO(
        log,
        "Tombstone table {}.{} begin, table_id={}",
        db_name,
        name_mapper.debugTableName(storage->getTableInfo()),
        table_id);

    const UInt64 tombstone_ts = tmt_context.getPDClient()->getTS();
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
        command.tombstone = tombstone_ts;
        commands.emplace_back(std::move(command));
    }
    auto alter_lock = storage->lockForAlter(getThreadNameAndID());
    storage->updateTombstone(alter_lock, commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(
        log,
        "Tombstone table {}.{} end, table_id={} tombstone={}",
        db_name,
        name_mapper.debugTableName(storage->getTableInfo()),
        table_id,
        tombstone_ts);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropTable(DatabaseID database_id, TableID table_id)
{
    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_INFO(log, "Storage instance does not exist, applyDropTable is ignored, table_id={}", table_id);
        return;
    }
    const auto & table_info = storage->getTableInfo();
    if (table_info.isLogicalPartitionTable())
    {
        for (const auto & part_def : table_info.partition.definitions)
        {
            if (TableID latest_logical_table_id = table_id_map.findTableIDInPartitionMap(part_def.id);
                latest_logical_table_id == -1 || latest_logical_table_id != table_info.id)
            {
                // The partition is managed by another logical table now (caused by `alter table X partition by ...`),
                // skip dropping this partition when dropping the old logical table
                LOG_INFO(
                    log,
                    "The partition is not managed by current logical table, skip, partition_table_id={} "
                    "new_logical_table_id={} current_logical_table_id={}",
                    part_def.id,
                    latest_logical_table_id,
                    table_info.id);
                continue;
            }

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
    LOG_INFO(log, "Sync all schemas begin");

    /// Create all databases.
    std::vector<DBInfoPtr> all_db_info = getter.listDBs();

    //We can't use too large default_num_threads, otherwise, the lock grabbing time will be too much.
    size_t default_num_threads = std::max(4UL, std::thread::hardware_concurrency());
    auto sync_all_schema_thread_pool
        = ThreadPool(default_num_threads, default_num_threads / 2, default_num_threads * 2);
    auto sync_all_schema_wait_group = sync_all_schema_thread_pool.waitGroup();

    std::mutex created_db_set_mutex;
    std::unordered_set<String> created_db_set;
    for (const auto & db_info : all_db_info)
    {
        auto task = [this, db_info, &created_db_set, &created_db_set_mutex] {
            do
            {
                if (databases.exists(db_info->id))
                {
                    break;
                }
                applyCreateDatabaseByInfo(db_info);
                {
                    std::unique_lock<std::mutex> created_db_set_lock(created_db_set_mutex);
                    created_db_set.emplace(name_mapper.mapDatabaseName(*db_info));
                }

                LOG_INFO(
                    log,
                    "Database {} created during sync all schemas, database_id={}",
                    name_mapper.debugDatabaseName(*db_info),
                    db_info->id);
            } while (false); // Ensure database existing

            std::vector<TableInfoPtr> tables = getter.listTables(db_info->id);
            for (auto & table_info : tables)
            {
                LOG_INFO(
                    log,
                    "Table {} syncing during sync all schemas, database_id={} table_id={}",
                    name_mapper.debugCanonicalName(*db_info, *table_info),
                    db_info->id,
                    table_info->id);

                /// Ignore view and sequence.
                if (table_info->is_view || table_info->is_sequence)
                {
                    LOG_INFO(
                        log,
                        "Table {} is a view or sequence, ignoring. database_id={} table_id={}",
                        name_mapper.debugCanonicalName(*db_info, *table_info),
                        db_info->id,
                        table_info->id);
                    continue;
                }

                table_id_map.emplaceTableID(table_info->id, db_info->id);
                LOG_DEBUG(
                    log,
                    "register table to table_id_map, database_id={} table_id={}",
                    db_info->id,
                    table_info->id);

                // `SchemaGetter::listTables` only return non-tombstone tables.
                // So `syncAllSchema` will not create tombstone tables. But if there are new rows/new snapshot
                // sent to TiFlash, TiFlash can create the instance by `applyTable` with force==true in the
                // related process.
                applyCreateStorageInstance(db_info->id, table_info, false);
                if (table_info->isLogicalPartitionTable())
                {
                    for (const auto & part_def : table_info->partition.definitions)
                    {
                        LOG_DEBUG(
                            log,
                            "register table to table_id_map for partition table, logical_table_id={} "
                            "physical_table_id={}",
                            table_info->id,
                            part_def.id);
                        table_id_map.emplacePartitionTableID(part_def.id, table_info->id);
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
            LOG_INFO(
                log,
                "Table {}.{} dropped during sync all schemas, table_id={}",
                it->second->getDatabaseName(),
                name_mapper.debugTableName(table_info),
                table_info.id);
        }
    }

    /// Drop all unmapped databases
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
            applyDropDatabaseByName(it->first);
            LOG_INFO(log, "Database {} dropped during sync all schemas", it->first);
        }
    }

    LOG_INFO(log, "Sync all schemas end");
}

/**
 * Update the schema of given `physical_table_id`.
 * This function ensure only the lock of `physical_table_id` is involved.
 *
 * Param `database_id`, `logical_table_id` is to key to fetch the latest table info. If
 * something wrong when generating the table info of `physical_table_id`, it means the
 * TableID mapping is not up-to-date. This function will return false and the caller
 * should update the TableID mapping then retry.
 * If the caller ensure the TableID mapping is up-to-date, then it should call with
 * `force == true`
 */
template <typename Getter, typename NameMapper>
bool SchemaBuilder<Getter, NameMapper>::applyTable(
    DatabaseID database_id,
    TableID logical_table_id,
    TableID physical_table_id,
    bool force)
{
    // When `force==false`, we get table info without mvcc. So we can detect that whether
    // the table has been renamed to another database or dropped.
    // If the table has been renamed to another database, it is dangerous to use the
    // old table info from the old database because some new columns may have been
    // added to the new table.
    // For the reason above, if we can not get table info without mvcc, this function
    // will return false and the caller should update the table_id_map then retry.
    //
    // When `force==true`, the caller ensure the TableID mapping is up-to-date, so we
    // need to get table info with mvcc. It can return the table info even if a table is
    // dropped but not physically removed by TiDB/TiKV gc_safepoint.
    // It is need for TiFlash correctly decoding the data and get ready for `RECOVER TABLE`
    // and `RECOVER DATABASE`.
    TableInfoPtr table_info;
    bool get_by_mvcc = false;
    if (!force)
    {
        table_info = getter.getTableInfo(database_id, logical_table_id, /*try_mvcc*/ false);
    }
    else
    {
        std::tie(table_info, get_by_mvcc) = getter.getTableInfoAndCheckMvcc(database_id, logical_table_id);
    }
    if (table_info == nullptr)
    {
        LOG_WARNING(
            log,
            "table is not exist in TiKV, applyTable need retry, get_by_mvcc={} database_id={} logical_table_id={}",
            get_by_mvcc,
            database_id,
            logical_table_id);
        return false;
    }

    // For physical table of partition table
    if (logical_table_id != physical_table_id)
    {
        if (!table_info->isLogicalPartitionTable())
        {
            LOG_WARNING(
                log,
                "new table info in TiKV is not partition table {}, applyTable need retry, database_id={} table_id={}",
                name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
                database_id,
                table_info->id);
            return false;
        }

        try
        {
            // Try to produce table info by the logical table
            table_info = table_info->producePartitionTableInfo(physical_table_id, name_mapper);
        }
        catch (const Exception & e)
        {
            // The following DDLs could change to mapping:
            //  - ALTER TABLE ... EXCHANGE PARTITION
            //  - ALTER TABLE ... PARTITION BY
            //  - ALTER TABLE ... REMOVE PARTITIONING
            // If the physical_table does not belong to the logical table in the
            // latest table info. It could now become a normal table or belong to another
            // logical table now. The caller should update the table_id_map then retry.
            LOG_WARNING(
                log,
                "producePartitionTableInfo meet exception, applyTable need retry, message={}",
                e.message());
            return false;
        }
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, physical_table_id);
    if (storage == nullptr)
    {
        // The Raft log or snapshot could comes after user executed `DROP DATABASE ...`. In order to
        // handle this situation, we should not fetch the `DatabaseInfo` from TiKV.

        // Create the instance with the latest table info
        // If the table info is get by mvcc, it means the table is actually in "dropped" status
        applyCreateStorageInstance(database_id, table_info, get_by_mvcc);
        return true;
    }

    // Alter the existing instance with the latest table info
    LOG_INFO(
        log,
        "Alter table {} begin, database_id={} table_id={}",
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

    LOG_INFO(
        log,
        "Alter table {} end, database_id={} table_id={}",
        name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id),
        database_id,
        table_info->id);
    return true;
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::dropAllSchema()
{
    LOG_INFO(log, "Drop all schemas begin");

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
        LOG_INFO(
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
        applyDropDatabaseByName(db.first);
        LOG_INFO(log, "Database {} dropped during drop all schemas", db.first);
    }

    LOG_INFO(log, "Drop all schemas end");
}

template <typename Getter, typename NameMapper>
String SchemaBuilder<Getter, NameMapper>::tryGetDatabaseDisplayNameFromLocal(DatabaseID database_id)
{
    // This method is called in the `applyDiff` loop. The `applyDiff` loop should apply the all the DDL operations before
    // the database get dropped, so the database info should be cached in `databases`.
    // But for corner cases that the database is dropped on some unkonwn cases, we just return a display database name
    // according to the keyspace_id and database_id because display name ususally is not critical.
    if (auto new_db_info = databases.getDBInfo(database_id); likely(new_db_info != nullptr))
    {
        return name_mapper.displayDatabaseName(*new_db_info);
    }
    return name_mapper.mapDatabaseName(database_id, keyspace_id);
}

// product env
template struct SchemaBuilder<SchemaGetter, SchemaNameMapper>;
// mock test
template struct SchemaBuilder<MockSchemaGetter, MockSchemaNameMapper>;
// unit test
template struct SchemaBuilder<MockSchemaGetter, SchemaNameMapper>;
// end namespace
} // namespace DB
