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
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <TiDB/Schema/SchemaBuilder-internal.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>
#include <magic_enum.hpp>
#include <mutex>
#include <tuple>

#include "Storages/Transaction/RegionCFDataBase.h"
#include "Storages/Transaction/Types.h"

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
void SchemaBuilder<Getter, NameMapper>::applyDiff(const SchemaDiff & diff)
{
    LOG_INFO(log, "applyDiff: {}, {}, {}", static_cast<int>(diff.type), diff.schema_id, diff.table_id);
    if (diff.type == SchemaActionType::CreateSchema) // create database 就不影响，正常创建
    {
        applyCreateSchema(diff.schema_id);
        return;
    }

    if (diff.type == SchemaActionType::DropSchema) // drop database 就不影响，正常创建
    {
        applyDropSchema(diff.schema_id);
        return;
    }

    // 其实我不用管 createtables？所有的真实 create 都要等到 set tiflash replica 的操作呀
    // 不能不管，因为 create table 一定早于 insert，但是 set tiflash replica 不能保证一定早于 insert，不然会出现 insert 的时候表不存在的情况，并且还拉不到表信息
    if (diff.type == SchemaActionType::CreateTables) // createTables 不实际 apply schema，但是更新 table_id_to_database_id 和 partition_id_with_table_id
    {
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        for (auto && opt : diff.affected_opts)
        {
            auto table_info = getter.getTableInfo(opt.schema_id, opt.table_id);
            // TODO: double check 一下如果没有 table_info 就不 emplace 是否合理
            // 这个应该是合理的，因为可能先 creates 后面 又 drop 了，不过如果后面改并行的时候，就不一定了。
            if (table_info == nullptr)
            {
                // this table is dropped.
                LOG_DEBUG(log, "Table {} not found, may have been dropped.", opt.table_id);
                return;
            }
            table_id_to_database_id.emplace(opt.table_id, opt.schema_id);
            if (table_info->isLogicalPartitionTable())
            {
                // TODO:目前先暴力的直接生成 logical table，后面在看看怎么处理，感觉必须生成空的 logical 表，不然会有别的问题
                // 不过后面如果想删空表的时候需要考虑一下。
                // 另外就是如果是每个 replica 的分区表，这个情况怎么搞。
                auto new_db_info = getter.getDatabase(diff.schema_id);
                if (new_db_info == nullptr)
                {
                    LOG_ERROR(log, "miss database: {}", diff.schema_id);
                    return;
                }

                applyCreatePhysicalTable(new_db_info, table_info);

                for (const auto & part_def : table_info->partition.definitions)
                {
                    if (partition_id_to_logical_id.find(part_def.id) != partition_id_to_logical_id.end())
                    {
                        LOG_ERROR(log, "partition_id_to_logical_id {} already exists", part_def.id);
                        partition_id_to_logical_id[part_def.id] = opt.table_id;
                    }
                    else
                    {
                        partition_id_to_logical_id.emplace(part_def.id, opt.table_id);
                    }
                }
            }
        }
        return;
    }


    if (diff.type == SchemaActionType::RenameTables)
    {
        // 如果先 rename，然后再 syncTableSchema rename 执行完再执行 syncTableSchema 不会影响正确性
        // 如果先 syncTableSchema，再rename，那么 rename 执行完，再执行 syncTableSchema 也不会影响正确性
        // 不过要记得 rename 检测要彻底，可能出现其中一个表已经改了，但是其他的没改的情况
        for (auto && opt : diff.affected_opts)
            applyRenameTable(opt.schema_id, opt.table_id);

        return;
    }

    if (diff.type == SchemaActionType::ActionFlashbackCluster)
    {
        return;
    }

    switch (diff.type)
    {
    case SchemaActionType::CreateTable:
    {
        auto table_info = getter.getTableInfo(diff.schema_id, diff.table_id);
        if (table_info == nullptr)
        {
            // this table is dropped.
            LOG_DEBUG(log, "Table {} not found, may have been dropped.", diff.table_id);
            return;
        }
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);

        LOG_INFO(log, "create table emplace table_id_to_database_id {}.{}", diff.table_id, diff.schema_id);
        table_id_to_database_id.emplace(diff.table_id, diff.schema_id);
        if (table_info->isLogicalPartitionTable())
        {
            // TODO:目前先暴力的直接生成 logical table，后面在看看怎么处理
            auto new_db_info = getter.getDatabase(diff.schema_id);
            if (new_db_info == nullptr)
            {
                LOG_ERROR(log, "miss database: {}", diff.schema_id);
                return;
            }

            applyCreatePhysicalTable(new_db_info, table_info);

            for (const auto & part_def : table_info->partition.definitions)
            {
                if (partition_id_to_logical_id.find(part_def.id) != partition_id_to_logical_id.end())
                {
                    LOG_ERROR(log, "partition_id_to_logical_id {} already exists", part_def.id);
                    partition_id_to_logical_id[part_def.id] = diff.table_id;
                }
                else
                {
                    partition_id_to_logical_id.emplace(part_def.id, diff.table_id);
                }
            }
        }

        LOG_INFO(log, "Finish Create Table");
        break;
    }

    case SchemaActionType::RecoverTable: // recover 不能拖时间，不然就直接失效了....
    {
        // 更新 table_id_to_database_id, 并且执行 recover
        applyRecoverTable(diff.schema_id, diff.table_id);

        auto table_info = getter.getTableInfo(diff.schema_id, diff.table_id);
        if (table_info == nullptr)
        {
            // this table is dropped.
            LOG_DEBUG(log, "Table {} not found, may have been dropped.", diff.table_id);
            return;
        }

        // 感觉不需要补充这个哎，如果没有删掉就 recover 了，那这些都还存在的。如果删了，就不会 recover 了
        // std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        // table_id_to_database_id.emplace(diff.table_id, diff.schema_id);
        // if (table_info->isLogicalPartitionTable())
        // {
        //     for (const auto & part_def : table_info->partition.definitions)
        //     {
        //         partition_id_to_logical_id.emplace(part_def.id, diff.table_id);
        //     }
        // }

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
        auto table_info = getter.getTableInfo(diff.schema_id, diff.table_id);
        if (table_info == nullptr)
        {
            // this table is dropped.
            LOG_DEBUG(log, "Table {} not found, may have been dropped.", diff.table_id);
            return;
        }

        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);

        table_id_to_database_id.emplace(diff.table_id, diff.schema_id);
        if (table_info->isLogicalPartitionTable())
        {
            // TODO:目前先暴力的直接生成 logical table，后面在看看怎么处理
            auto new_db_info = getter.getDatabase(diff.schema_id);
            if (new_db_info == nullptr)
            {
                LOG_ERROR(log, "miss database: {}", diff.schema_id);
                return;
            }

            applyCreatePhysicalTable(new_db_info, table_info);

            for (const auto & part_def : table_info->partition.definitions)
            {
                //partition_id_to_logical_id.emplace(part_def.id, diff.table_id);
                if (partition_id_to_logical_id.find(part_def.id) != partition_id_to_logical_id.end())
                {
                    LOG_ERROR(log, "partition_id_to_logical_id {} already exists", part_def.id);
                    partition_id_to_logical_id[part_def.id] = diff.table_id;
                }
                else
                {
                    partition_id_to_logical_id.emplace(part_def.id, diff.table_id);
                }
            }
        }

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
        auto db_info = getter.getDatabase(diff.schema_id);
        if (db_info == nullptr)
        {
            LOG_ERROR(log, "miss database: {}", diff.schema_id);
            return;
        }
        applyPartitionDiff(db_info, diff.table_id, shared_mutex_for_table_id_map);
        break;
    }
    case SchemaActionType::ExchangeTablePartition:
    {
        /// Table_id in diff is the partition id of which will be exchanged,
        /// Schema_id in diff is the non-partition table's schema id
        /// Old_table_id in diff is the non-partition table's table id
        /// Table_id in diff.affected_opts[0] is the table id of the partition table
        /// Schema_id in diff.affected_opts[0] is the schema id of the partition table

        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        if (table_id_to_database_id.find(diff.old_table_id) != table_id_to_database_id.end())
        {
            table_id_to_database_id.erase(diff.old_table_id);
        }
        else
        {
            LOG_ERROR(log, "table_id {} not in table_id_to_database_id", diff.old_table_id);
        }
        table_id_to_database_id.emplace(diff.table_id, diff.schema_id);

        if (partition_id_to_logical_id.find(diff.table_id) != partition_id_to_logical_id.end())
        {
            partition_id_to_logical_id.erase(diff.table_id);
        }
        else
        {
            LOG_ERROR(log, "table_id {} not in partition_id_to_logical_id", diff.table_id);
        }
        partition_id_to_logical_id.emplace(diff.old_table_id, diff.affected_opts[0].table_id);

        if (diff.schema_id != diff.affected_opts[0].schema_id)
        {
            //applyRenamePhysicalTable(diff.schema_id, diff.old_table_id, diff.affected_opts[0].schema_id); // old_schema, old_table_id, new_schema;
            {
                auto new_db_info = getter.getDatabase(diff.affected_opts[0].schema_id);
                if (new_db_info == nullptr)
                {
                    LOG_ERROR(log, "miss database: {}", diff.affected_opts[0].schema_id);
                    return;
                }

                auto new_table_info = getter.getTableInfo(diff.affected_opts[0].schema_id, diff.affected_opts[0].table_id);
                if (new_table_info == nullptr)
                {
                    LOG_ERROR(log, "miss table in TiKV: {}", diff.affected_opts[0].table_id);
                    return;
                }

                auto & tmt_context = context.getTMTContext();
                auto storage = tmt_context.getStorages().get(keyspace_id, diff.old_table_id);
                if (storage == nullptr)
                {
                    LOG_ERROR(log, "miss table in TiFlash: {}", diff.old_table_id);
                    return;
                }

                auto part_table_info = new_table_info->producePartitionTableInfo(diff.old_table_id, name_mapper);
                applyRenamePhysicalTable(new_db_info, *part_table_info, storage);
            }

            //applyRenamePhysicalTable(diff.affected_opts[0].schema_id, diff.table_id, diff.schema_id);
            {
                auto new_db_info = getter.getDatabase(diff.schema_id);
                if (new_db_info == nullptr)
                {
                    LOG_ERROR(log, "miss database: {}", diff.schema_id);
                    return;
                }

                auto new_table_info = getter.getTableInfo(diff.schema_id, diff.table_id);
                if (new_table_info == nullptr)
                {
                    LOG_ERROR(log, "miss table in TiKV: {}", diff.table_id);
                    return;
                }

                auto & tmt_context = context.getTMTContext();
                auto storage = tmt_context.getStorages().get(keyspace_id, diff.table_id);
                if (storage == nullptr)
                {
                    LOG_ERROR(log, "miss table in TiFlash: {}", diff.old_table_id);
                    return;
                }

                applyRenamePhysicalTable(new_db_info, *new_table_info, storage);
            }
        }

        break;
    }
    case SchemaActionType::SetTiFlashReplica:
    case SchemaActionType::UpdateTiFlashReplicaStatus: // TODO:Double check with PR：https://github.com/pingcap/tiflash/pull/7571
    {
        auto db_info = getter.getDatabase(diff.schema_id);
        if (db_info == nullptr)
        {
            LOG_ERROR(log, "miss database: {}", diff.schema_id);
            return;
        }
        applySetTiFlashReplica(db_info, diff.table_id);
        break;
    }
    default:
    {
        if (diff.type < SchemaActionType::MaxRecognizedType)
        {
            LOG_INFO(log, "Ignore change type: {}", magic_enum::enum_name(diff.type));
        }
        else
        { // >= SchemaActionType::MaxRecognizedType
            LOG_ERROR(log, "Unsupported change type: {}", magic_enum::enum_name(diff.type));
        }

        break;
    }
    }
}


template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applySetTiFlashReplica(const TiDB::DBInfoPtr & db_info, TableID table_id)
{
    auto latest_table_info = getter.getTableInfo(db_info->id, table_id);
    if (unlikely(latest_table_info == nullptr))
    {
        LOG_ERROR(log, "miss old table id in TiKV {}", table_id);
        return;
    }

    if (latest_table_info->replica_info.count == 0)
    {
        // 1. set 0
        auto & tmt_context = context.getTMTContext();
        auto storage = tmt_context.getStorages().get(keyspace_id, latest_table_info->id);
        if (unlikely(storage == nullptr))
        {
            // 这边感觉没了也就没了，不会影响啥？
            LOG_ERROR(log, "miss table in TiFlash {}", table_id);
            return;
        }

        // 直接当作 drop table 来处理
        applyDropTable(db_info->id, table_id);
    }
    else
    {
        // 2. set 非 0
        // 我们其实也不在乎他到底有几个 replica 对吧，有就可以了。并且真的要插入数据了， create table 已经把基础打好了，所以不用处理

        // 但是有一种可能是 create 了，然后 set 0， 然后再 set 1，这样 map 值可能被删了，或者即将被删
        auto & tmt_context = context.getTMTContext();
        auto storage = tmt_context.getStorages().get(keyspace_id, latest_table_info->id);
        if (storage != nullptr)
        {
            // 说明 storage 还在，check 一下他的 tomstone
            if (storage->getTombstone() == 0)
            {
                // 说明没被删，那就不用管了
                return;
            }
            else
            {
                // 删了就走 recover 逻辑
                applyRecoverTable(db_info->id, table_id);
            }
        }
        else
        {
            // 如果 map 里没有，就走 create 逻辑，有的话就不用管了
            // TODO:check 这个合理么
            std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
            if (table_id_to_database_id.find(table_id) == table_id_to_database_id.end())
            {
                // 那就走 create table 的逻辑
                auto table_info = getter.getTableInfo(db_info->id, table_id);
                if (table_info == nullptr)
                {
                    // this table is dropped.
                    LOG_DEBUG(log, "Table {} not found, may have been dropped.", table_id);
                    return;
                }

                LOG_INFO(log, "create table emplace table_id_to_database_id {}.{}", table_id, db_info->id);
                table_id_to_database_id.emplace(table_id, db_info->id);
                if (table_info->isLogicalPartitionTable())
                {
                    auto new_db_info = getter.getDatabase(db_info->id);
                    if (new_db_info == nullptr)
                    {
                        LOG_ERROR(log, "miss database: {}", db_info->id);
                        return;
                    }

                    applyCreatePhysicalTable(new_db_info, table_info);

                    for (const auto & part_def : table_info->partition.definitions)
                    {
                        //partition_id_to_logical_id.emplace(part_def.id, table_id);
                        if (partition_id_to_logical_id.find(part_def.id) != partition_id_to_logical_id.end())
                        {
                            LOG_ERROR(log, "partition_id_to_logical_id {} already exists", part_def.id);
                            partition_id_to_logical_id[part_def.id] = table_id;
                        }
                        else
                        {
                            partition_id_to_logical_id.emplace(part_def.id, table_id);
                        }
                    }
                }
            }
        }
    }
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(const TiDB::DBInfoPtr & db_info, TableID table_id, std::shared_mutex & shared_mutex_for_table_id_map)
{
    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        LOG_ERROR(log, "miss old table id in TiKV {}", table_id);
        return;
    }
    if (!table_info->isLogicalPartitionTable())
    {
        LOG_ERROR(log, "new table in TiKV not partition table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_info->id);
    if (storage == nullptr)
    {
        LOG_ERROR(log, "miss table in TiFlash {}", table_id);
        return;
    }

    applyPartitionDiff(db_info, table_info, storage, shared_mutex_for_table_id_map);
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyPartitionDiff(const TiDB::DBInfoPtr & db_info, const TableInfoPtr & table_info, const ManageableStoragePtr & storage, std::shared_mutex & shared_mutex_for_table_id_map)
{
    // TODO: 这个要不要加锁
    const auto & orig_table_info = storage->getTableInfo();
    if (!orig_table_info.isLogicalPartitionTable())
    {
        LOG_ERROR(log, "old table in TiFlash not partition table {}", name_mapper.debugCanonicalName(*db_info, orig_table_info));
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
    for (const auto & orig_def : orig_defs)
    {
        if (new_part_id_set.count(orig_def.id) == 0)
        {
            applyDropPhysicalTable(name_mapper.mapDatabaseName(*db_info), orig_def.id);
        }
    }

    std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
    for (const auto & new_def : new_defs)
    {
        if (orig_part_id_set.count(new_def.id) == 0)
        {
            auto iter = partition_id_to_logical_id.find(new_def.id);
            if (iter == partition_id_to_logical_id.end())
            {
                partition_id_to_logical_id.emplace(new_def.id, updated_table_info.id);
            }
            else if (iter->second != new_def.id)
            {
                LOG_ERROR(log, "new partition id {} is exist with {}, and updated to {}", new_def.id, iter->second, updated_table_info.id);
                partition_id_to_logical_id.erase(new_def.id);
                partition_id_to_logical_id.emplace(new_def.id, updated_table_info.id);
            }
        }
    }

    auto alter_lock = storage->lockForAlter(getThreadNameAndID()); // 真实的拿 storage 的锁
    storage->updateTableInfo(alter_lock, updated_table_info, context, name_mapper.mapDatabaseName(db_info->id, keyspace_id), name_mapper.mapTableName(updated_table_info));


    /// TODO:需要什么 log 比较合适
    LOG_INFO(log, "Applied partition changes {}", name_mapper.debugCanonicalName(*db_info, *table_info));
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyRenameTable(DatabaseID database_id, TableID table_id)
{
    auto new_db_info = getter.getDatabase(database_id);
    if (new_db_info == nullptr)
    {
        LOG_ERROR(log, "miss database: {}", database_id);
        return;
    }

    auto new_table_info = getter.getTableInfo(database_id, table_id);
    if (new_table_info == nullptr)
    {
        LOG_ERROR(log, "miss table in TiKV: {}", table_id);
        return;
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, table_id);
    if (storage == nullptr)
    {
        LOG_ERROR(log, "miss table in TiFlash: {}", table_id);
        return;
    }

    applyRenameLogicalTable(new_db_info, new_table_info, storage);

    std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
    auto iter = table_id_to_database_id.find(table_id);
    if (iter == table_id_to_database_id.end())
    {
        LOG_ERROR(log, "table_id {} not in table_id_to_database_id", table_id);
    }
    else if (iter->second != database_id)
    {
        table_id_to_database_id.erase(table_id);
        table_id_to_database_id.emplace(table_id, database_id);
    }

    if (new_table_info->isLogicalPartitionTable())
    {
        for (const auto & part_def : new_table_info->partition.definitions)
        {
            auto iter = table_id_to_database_id.find(part_def.id);
            if (iter == table_id_to_database_id.end())
            {
                LOG_ERROR(log, "table_id {} not in table_id_to_database_id", table_id);
            }
            else if (iter->second != database_id)
            {
                table_id_to_database_id.erase(table_id);
                table_id_to_database_id.emplace(table_id, database_id);
            }
        }
    }
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
                LOG_ERROR(log, "miss old table id in TiFlash: {}", part_def.id);
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
        LOG_DEBUG(log, "Table {} name identical, not renaming.", name_mapper.debugCanonicalName(*new_db_info, new_table_info));
        return;
    }

    const auto old_mapped_tbl_name = storage->getTableName();
    GET_METRIC(tiflash_schema_internal_ddl_count, type_rename_column).Increment();
    LOG_INFO(
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

    InterpreterRenameQuery(rename, context, getThreadNameAndID()).execute();

    LOG_INFO(
        log,
        "Renamed table {}.{} (display name: {}) to {}",
        old_mapped_db_name,
        old_mapped_tbl_name,
        old_display_table_name,
        name_mapper.debugCanonicalName(*new_db_info, new_table_info));
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
void SchemaBuilder<Getter, NameMapper>::applyRecoverTable(DatabaseID database_id, TiDB::TableID table_id)
{
    auto db_info = getter.getDatabase(database_id);
    if (db_info == nullptr)
    {
        LOG_ERROR(log, "miss database: {}", database_id);
        return;
    }

    auto table_info = getter.getTableInfo(db_info->id, table_id);
    if (table_info == nullptr)
    {
        // this table is dropped.
        LOG_DEBUG(log, "Table {} not found, may have been dropped.", table_id);
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
void SchemaBuilder<Getter, NameMapper>::applyRecoverPhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info)
{
    auto & tmt_context = context.getTMTContext();
    if (auto * storage = tmt_context.getStorages().get(keyspace_id, table_info->id).get(); storage)
    {
        if (!storage->isTombstone())
        {
            LOG_DEBUG(log,
                      "Trying to recover table {} but it already exists and is not marked as tombstone",
                      name_mapper.debugCanonicalName(*db_info, *table_info));
            return;
        }

        LOG_DEBUG(log, "Recovering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
        AlterCommands commands;
        {
            AlterCommand command;
            command.type = AlterCommand::RECOVER;
            commands.emplace_back(std::move(command));
        }
        auto alter_lock = storage->lockForAlter(getThreadNameAndID());
        // TODO:alterFromTiDB 简化 and rename
        storage->alterFromTiDB(alter_lock, commands, name_mapper.mapDatabaseName(*db_info), *table_info, name_mapper, context);
        LOG_INFO(log, "Created table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
        return;
    }
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

    shared_mutex_for_databases.lock();
    LOG_INFO(log, "emplace databases with db id: {}", db_info->id);
    databases.emplace(db_info->id, db_info);
    shared_mutex_for_databases.unlock();

    LOG_INFO(log, "Created database {}", name_mapper.debugDatabaseName(*db_info));
}

// TODO:要先把没删掉的表给删了
template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyDropSchema(DatabaseID schema_id)
{
    shared_mutex_for_databases.lock_shared();
    auto it = databases.find(schema_id);
    if (unlikely(it == databases.end()))
    {
        LOG_INFO(
            log,
            "Syncer wants to drop database [id={}], but database is not found, may has been dropped.",
            schema_id);
        shared_mutex_for_databases.unlock_shared();
        return;
    }
    shared_mutex_for_databases.unlock_shared();

    // 检查数据库对应的表是否都已经被删除
    // 先用一个非常离谱的手法，后面在看看真的难到要再加一个 map 么
    // TODO:优化这段逻辑，不然耗时太长了。
    shared_mutex_for_table_id_map.lock_shared();
    for (const auto & pair : table_id_to_database_id)
    {
        if (pair.second == schema_id)
        {
            // 还要处理 分区表，因为你也拉不到 tableInfo了，不过这边完全可以扔给后台做
            // alter a add column , insert data, drop database 这个场景要能 cover
            applyDropTable(schema_id, pair.first);

            for (const auto & parition_pair : partition_id_to_logical_id)
            {
                if (parition_pair.second == pair.first)
                {
                    applyDropTable(schema_id, parition_pair.first);
                }
            }
        }
    }
    shared_mutex_for_table_id_map.unlock_shared();

    applyDropSchema(name_mapper.mapDatabaseName(*it->second));
    shared_mutex_for_databases.lock();
    databases.erase(schema_id);
    shared_mutex_for_databases.unlock();
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
void SchemaBuilder<Getter, NameMapper>::applyCreatePhysicalTable(const TiDB::DBInfoPtr & db_info, const TableInfoPtr & table_info)
{
    GET_METRIC(tiflash_schema_internal_ddl_count, type_create_table).Increment();
    LOG_INFO(log, "Creating table {}", name_mapper.debugCanonicalName(*db_info, *table_info));

    /// Check if this is a RECOVER table.
    {
        auto & tmt_context = context.getTMTContext();
        if (auto * storage = tmt_context.getStorages().get(keyspace_id, table_info->id).get(); storage)
        {
            if (!storage->isTombstone())
            {
                LOG_DEBUG(log,
                          "Trying to create table {} but it already exists and is not marked as tombstone",
                          name_mapper.debugCanonicalName(*db_info, *table_info));
                return;
            }

            LOG_DEBUG(log, "Recovering table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
            AlterCommands commands;
            {
                AlterCommand command;
                command.type = AlterCommand::RECOVER;
                commands.emplace_back(std::move(command));
            }
            auto alter_lock = storage->lockForAlter(getThreadNameAndID());
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
    LOG_INFO(log, "Tombstoning table {}.{}", db_name, name_mapper.debugTableName(storage->getTableInfo()));
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
    storage->alterFromTiDB(alter_lock, commands, db_name, storage->getTableInfo(), name_mapper, context);
    LOG_INFO(log, "Tombstoned table {}.{}", db_name, name_mapper.debugTableName(storage->getTableInfo()));
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

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::syncAllSchema()
{
    LOG_INFO(log, "Syncing all schemas.");

    /// Create all databases.
    std::vector<DBInfoPtr> all_schemas = getter.listDBs();

    // TODO:改成并行
    std::unordered_set<String> db_set;
    for (const auto & db : all_schemas)
    {
        shared_mutex_for_databases.lock_shared();
        if (databases.find(db->id) == databases.end())
        {
            shared_mutex_for_databases.unlock_shared();
            // TODO:create database 感觉就是写入 db.sql, 以及把 database 信息写入 context，如果后面不存 .sql，可以再进行简化
            applyCreateSchema(db);
            db_set.emplace(name_mapper.mapDatabaseName(*db));
            LOG_DEBUG(log, "Database {} created during sync all schemas", name_mapper.debugDatabaseName(*db));
        }
        else
        {
            shared_mutex_for_databases.unlock_shared();
        }
    }

    // TODO:改成并行
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

            std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
            table_id_to_database_id.emplace(table->id, db->id);

            if (table->isLogicalPartitionTable())
            {
                for (const auto & part_def : table->partition.definitions)
                {
                    //partition_id_to_logical_id.emplace(part_def.id, table->id);
                    if (partition_id_to_logical_id.find(part_def.id) != partition_id_to_logical_id.end())
                    {
                        LOG_ERROR(log, "partition_id_to_logical_id {} already exists", part_def.id);
                        partition_id_to_logical_id[part_def.id] = table->id;
                    }
                    else
                    {
                        partition_id_to_logical_id.emplace(part_def.id, table->id);
                    }
                }
            }
        }
    }

    // TODO:can be removed if we don't save the .sql
    /// Drop all unmapped tables.
    auto storage_map = context.getTMTContext().getStorages().getAllStorage();
    for (auto it = storage_map.begin(); it != storage_map.end(); it++)
    {
        auto table_info = it->second->getTableInfo();
        if (table_info.keyspace_id != keyspace_id)
        {
            continue;
        }
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        if (table_id_to_database_id.find(table_info.id) == table_id_to_database_id.end() && partition_id_to_logical_id.find(table_info.id) == partition_id_to_logical_id.end())
        {
            applyDropPhysicalTable(it->second->getDatabaseName(), table_info.id);
            LOG_DEBUG(log, "Table {}.{} dropped during sync all schemas", it->second->getDatabaseName(), name_mapper.debugTableName(table_info));
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
        if (db_set.count(it->first) == 0 && !isReservedDatabase(context, it->first))
        {
            applyDropSchema(it->first);
            LOG_DEBUG(log, "DB {} dropped during sync all schemas", it->first);
        }
    }

    LOG_INFO(log, "Loaded all schemas.");
}

template <typename Getter, typename NameMapper>
void SchemaBuilder<Getter, NameMapper>::applyTable(DatabaseID database_id, TableID table_id, TableID partition_table_id)
{
    LOG_INFO(log, "apply table: {}, {}, {}", database_id, table_id, partition_table_id);

    // TODO:这种方案还会出现一个问题就是，频繁的 DDL 后 drop，然后拉不到 对应的 schema，最后的数据没解析下去写入的问题，这次也一定要修掉了。
    auto table_info = getter.getTableInfo(database_id, table_id);
    if (table_info == nullptr)
    {
        // TODO:说明表被删了，需要 fix 一下去拿导数第二次的那个schema
        LOG_ERROR(log, "miss table in TiFlash : {}.{}", database_id, table_id);
        return;
    }

    // 判断一下是分区表还是正常的表，如果是分区表的话，拿到他对应的分区表的 tableInfo
    if (table_id != partition_table_id)
    {
        // 说明是分区表

        // 检查一遍他是 logicalparitionTable
        if (!table_info->isLogicalPartitionTable())
        {
            // LOG_ERROR(log, "new table in TiKV is not partition table {}", name_mapper.debugCanonicalName(*db_info, *table_info));
            LOG_ERROR(log, "new table in TiKV is not partition table {}", name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id));
            return;
        }
        try
        {
            table_info = table_info->producePartitionTableInfo(partition_table_id, name_mapper);
        }
        catch (const Exception & e)
        {
            // TODO:目前唯一会遇到这个问题的在于，先 DDL，insert，然后 organize partition。并且让 organize 先到 tiflash。这样就 insert 到的时候，老的 partition_id 已经不在了，所以生成不了，直接让他不插入应该就可以了。
            LOG_ERROR(log, "producePartitionTableInfo meet exception : {} \n stack is {}", e.displayText(), e.getStackTrace().toString());
            return;
        }
    }

    auto & tmt_context = context.getTMTContext();
    auto storage = tmt_context.getStorages().get(keyspace_id, partition_table_id);
    if (storage == nullptr)
    {
        auto db_info = getter.getDatabase(database_id);
        if (db_info == nullptr)
        {
            LOG_ERROR(log, "miss database: {}", database_id);
            return;
        }

        // empty_input_for_udaf.test 这个测试
        applyCreatePhysicalTable(db_info, table_info);
        // applyTable 入口前 check 过 map，所以肯定是 map里面有对应映射，所以不需要加
        // shared_mutex_for_table_id_map.lock();
        // if (table_id_to_database_id.find(table_id) == table_id_to_database_id.end()){
        //     table_id_to_database_id.emplace(table_id, database_id);
        // }
        // if (table_id != partition_table_id and partition_id_to_logical_id.find(table_id) == partition_id_to_logical_id.end()) {
        //     partition_id_to_logical_id.emplace(partition_table_id, table_id);
        // }
        // shared_mutex_for_table_id_map.unlock();
    }
    else
    {
        // 触发了 syncTableSchema 肯定是 tableInfo 不同了，但是应该还要检查一下
        LOG_INFO(log, "Altering table {}", name_mapper.debugCanonicalName(*table_info, database_id, keyspace_id));

        auto orig_table_info = storage->getTableInfo();

        auto alter_lock = storage->lockForAlter(getThreadNameAndID()); // 真实的拿 storage 的锁
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
        LOG_DEBUG(log, "Table {}.{} dropped during drop all schemas", storage.second->getDatabaseName(), name_mapper.debugTableName(table_info));
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
