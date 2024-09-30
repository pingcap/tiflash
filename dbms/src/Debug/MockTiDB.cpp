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
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/dbgKVStore.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/StringTokenizer.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

using ColumnInfo = TiDB::ColumnInfo;
using IndexInfo = TiDB::IndexInfo;
using TableInfo = TiDB::TableInfo;
using PartitionInfo = TiDB::PartitionInfo;
using PartitionDefinition = TiDB::PartitionDefinition;
using Table = MockTiDB::Table;
using TablePtr = MockTiDB::TablePtr;

Table::Table(
    const String & database_name_,
    DatabaseID database_id_,
    const String & table_name_,
    TableInfo && table_info_)
    : table_info(std::move(table_info_))
    , database_name(database_name_)
    , database_id(database_id_)
    , table_name(table_name_)
    , col_id(table_info.columns.size())
{}

MockTiDB::MockTiDB()
{
    databases["default"] = 0;
}

TablePtr MockTiDB::dropTableByNameImpl(
    Context & context,
    const String & database_name,
    const String & table_name,
    bool drop_regions)
{
    String qualified_name = database_name + "." + table_name;
    auto it_by_name = tables_by_name.find(qualified_name);
    if (it_by_name == tables_by_name.end())
        return nullptr;

    auto table = it_by_name->second;
    dropTableInternal(context, table, drop_regions);

    tables_by_name.erase(it_by_name);
    return table;
}

TablePtr MockTiDB::dropTableByIdImpl(Context & context, const TableID table_id, bool drop_regions)
{
    auto iter = tables_by_id.find(table_id);
    if (iter == tables_by_id.end())
        return nullptr;

    auto table = iter->second;
    dropTableInternal(context, table, drop_regions);

    // erase from `tables_by_name`
    for (auto iter_by_name = tables_by_name.begin(); iter_by_name != tables_by_name.end(); /* empty */)
    {
        if (table != iter_by_name->second)
        {
            ++iter_by_name;
            continue;
        }
        LOG_INFO(Logger::get(), "removing table from MockTiDB, name={} table_id={}", iter_by_name->first, table_id);
        iter_by_name = tables_by_name.erase(iter_by_name);
    }
    return table;
}

TablePtr MockTiDB::dropTableInternal(Context & context, const TablePtr & table, bool drop_regions)
{
    auto & kvstore = context.getTMTContext().getKVStore();
    auto debug_kvstore = RegionBench::DebugKVStore(*kvstore);
    auto & region_table = context.getTMTContext().getRegionTable();

    if (table->isPartitionTable())
    {
        for (const auto & partition : table->table_info.partition.definitions)
        {
            tables_by_id.erase(partition.id);
            if (drop_regions)
            {
                for (auto & e : region_table.getRegionsByTable(NullspaceID, partition.id))
                    debug_kvstore.mockRemoveRegion(e.first, region_table);
                region_table.removeTable(NullspaceID, partition.id);
            }
        }
    }
    tables_by_id.erase(table->id());

    if (drop_regions)
    {
        for (auto & e : region_table.getRegionsByTable(NullspaceID, table->id()))
            debug_kvstore.mockRemoveRegion(e.first, region_table);
        region_table.removeTable(NullspaceID, table->id());
    }

    return table;
}

void MockTiDB::dropDB(Context & context, const String & database_name, bool drop_regions)
{
    std::scoped_lock lock(tables_mutex);

    std::vector<String> table_names;
    std::for_each(tables_by_id.begin(), tables_by_id.end(), [&](const auto & pair) {
        if (pair.second->database_name == database_name)
            table_names.emplace_back(pair.second->table_info.name);
    });

    for (const auto & table_name : table_names)
        dropTableByNameImpl(context, database_name, table_name, drop_regions);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropSchema;
    if (databases.find(database_name) == databases.end())
        diff.schema_id = -1;
    else
        diff.schema_id = databases[database_name];
    diff.version = version;
    version_diff[version] = diff;

    databases.erase(database_name);
}

void MockTiDB::dropTable(Context & context, const String & database_name, const String & table_name, bool drop_regions)
{
    std::scoped_lock lock(tables_mutex);

    auto table = dropTableByNameImpl(context, database_name, table_name, drop_regions);
    if (!table)
        return;

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropTable;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::dropTableById(Context & context, const TableID & table_id, bool drop_regions)
{
    std::scoped_lock lock(tables_mutex);

    auto table = dropTableByIdImpl(context, table_id, drop_regions);
    if (!table)
        return;

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropTable;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

DatabaseID MockTiDB::newDataBase(const String & database_name)
{
    DatabaseID schema_id = 0;

    if (databases.find(database_name) == databases.end())
    {
        if (databases.empty())
        {
            schema_id = 1;
        }
        else
        {
            schema_id = databases.cbegin()->second + 1;
        }
        databases.emplace(database_name, schema_id);
    }

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::CreateSchema;
    diff.schema_id = schema_id;
    diff.version = version;
    version_diff[version] = diff;

    return schema_id;
}

TiDB::TableInfoPtr MockTiDB::parseColumns(
    const String & tbl_name,
    const ColumnsDescription & columns,
    const String & handle_pk_name)
{
    TableInfo table_info;
    table_info.name = tbl_name;
    table_info.pk_is_handle = false;
    table_info.is_common_handle = false;

    bool has_pk = false;
    bool has_non_int_pk = false;
    Poco::StringTokenizer string_tokens(handle_pk_name, ",");
    std::unordered_map<String, size_t> pk_column_pos_map;
    int i = 1;
    for (auto & column : columns.getAllPhysical())
    {
        Field default_value;
        auto it = columns.defaults.find(column.name);
        if (it != columns.defaults.end())
            default_value = getDefaultValue(it->second.expression);
        table_info.columns.emplace_back(reverseGetColumnInfo(column, i++, default_value, true));
        for (const auto & tok : string_tokens)
        {
            // todo support prefix index
            if (tok == column.name)
            {
                has_pk = true;
                if (!column.type->isInteger() && !column.type->isUnsignedInteger())
                    has_non_int_pk = true;
                table_info.columns.back().setPriKeyFlag();
                pk_column_pos_map[tok] = i - 2;
                break;
            }
        }
    }

    if (has_pk)
    {
        if (string_tokens.count() > 1 || has_non_int_pk)
        {
            table_info.is_common_handle = true;
            // construct IndexInfo
            table_info.index_infos.resize(1);
            TiDB::IndexInfo & index_info = table_info.index_infos[0];
            index_info.id = 1;
            index_info.is_primary = true;
            index_info.idx_name = "PRIMARY";
            index_info.is_unique = true;
            index_info.index_type = TiDB::IndexType::BTREE;
            index_info.idx_cols.resize(string_tokens.count());
            for (size_t index = 0; index < string_tokens.count(); index++)
            {
                String & name = string_tokens[index];
                index_info.idx_cols[index].name = name;
                index_info.idx_cols[index].offset = pk_column_pos_map[name];
                index_info.idx_cols[index].length = -1;
            }
        }
        else
            table_info.pk_is_handle = true;
    }

    return std::make_shared<TiDB::TableInfo>(std::move(table_info));
}

TableID MockTiDB::newTable(
    const String & database_name,
    const String & table_name,
    const ColumnsDescription & columns,
    Timestamp tso,
    const String & handle_pk_name)
{
    std::scoped_lock lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    if (tables_by_name.find(qualified_name) != tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    if (databases.find(database_name) == databases.end())
    {
        throw Exception("MockTiDB not found db: " + database_name, ErrorCodes::LOGICAL_ERROR);
    }

    auto table_info = parseColumns(table_name, columns, handle_pk_name);
    table_info->id = table_id_allocator++;
    table_info->update_timestamp = tso;
    return addTable(database_name, std::move(*table_info));
}

std::tuple<TableID, std::vector<TableID>> MockTiDB::newPartitionTable(
    const String & database_name,
    const String & table_name,
    const ColumnsDescription & columns,
    Timestamp tso,
    const String & handle_pk_name,
    const Strings & part_names)
{
    std::scoped_lock lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    if (tables_by_name.find(qualified_name) != tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    if (databases.find(database_name) == databases.end())
    {
        throw Exception("MockTiDB not found db: " + database_name, ErrorCodes::LOGICAL_ERROR);
    }

    std::vector<TableID> physical_table_ids;
    physical_table_ids.reserve(part_names.size());
    auto table_info = parseColumns(table_name, columns, handle_pk_name);
    table_info->id = table_id_allocator++;
    table_info->is_partition_table = true;
    table_info->partition.enable = true;
    for (const auto & part_name : part_names)
    {
        PartitionDefinition part_def;
        part_def.id = table_id_allocator++;
        part_def.name = part_name;
        table_info->partition.definitions.emplace_back(part_def);
        ++table_info->partition.num;
        physical_table_ids.emplace_back(part_def.id);
    }
    table_info->update_timestamp = tso;
    auto logical_table_id = addTable(database_name, std::move(*table_info));
    return {logical_table_id, physical_table_ids};
}

std::vector<TableID> MockTiDB::newTables(
    const String & database_name,
    const std::vector<std::tuple<String, ColumnsDescription, String>> & tables,
    Timestamp tso)
{
    std::scoped_lock lock(tables_mutex);
    std::vector<TableID> table_ids;
    table_ids.reserve(tables.size());
    if (databases.find(database_name) == databases.end())
    {
        throw Exception("MockTiDB not found db: " + database_name, ErrorCodes::LOGICAL_ERROR);
    }

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::CreateTables;
    for (const auto & [table_name, columns, handle_pk_name] : tables)
    {
        String qualified_name = database_name + "." + table_name;
        if (tables_by_name.find(qualified_name) != tables_by_name.end())
        {
            throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
        }

        auto table_info = *parseColumns(table_name, columns, handle_pk_name);
        table_info.id = table_id_allocator++;
        table_info.update_timestamp = tso;

        auto table
            = std::make_shared<Table>(database_name, databases[database_name], table_info.name, std::move(table_info));
        tables_by_id.emplace(table->table_info.id, table);
        tables_by_name.emplace(qualified_name, table);

        AffectedOption opt{};
        opt.schema_id = table->database_id;
        opt.table_id = table->id();
        opt.old_schema_id = table->database_id;
        opt.old_table_id = table->id();
        diff.affected_opts.push_back(std::move(opt));

        table_ids.push_back(table->id());
    }

    if (diff.affected_opts.empty())
        throw Exception("MockTiDB CreateTables should have at lease 1 table", ErrorCodes::LOGICAL_ERROR);

    diff.schema_id = diff.affected_opts[0].schema_id;
    diff.version = version;
    version_diff[version] = diff;

    return table_ids;
}

TableID MockTiDB::addTable(const String & database_name, TiDB::TableInfo && table_info)
{
    auto table
        = std::make_shared<Table>(database_name, databases[database_name], table_info.name, std::move(table_info));
    String qualified_name = database_name + "." + table->table_info.name;
    tables_by_id.emplace(table->table_info.id, table);
    tables_by_name.emplace(qualified_name, table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::CreateTable;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;

    return table->table_info.id;
}

Field getDefaultValue(const ASTPtr & default_value_ast)
{
    const auto * func = typeid_cast<const ASTFunction *>(default_value_ast.get());
    if (func != nullptr)
    {
        const auto * value_ptr = typeid_cast<const ASTLiteral *>(
            typeid_cast<const ASTExpressionList *>(func->arguments.get())->children[0].get());
        return value_ptr->value;
    }
    else if (typeid_cast<const ASTLiteral *>(default_value_ast.get()) != nullptr)
        return typeid_cast<const ASTLiteral *>(default_value_ast.get())->value;
    return Field();
}

TableID MockTiDB::newPartition(
    TableID belong_logical_table,
    const String & partition_name,
    Timestamp tso,
    bool is_add_part)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr logical_table = getTableByID(belong_logical_table);
    TableID partition_id = table_id_allocator++; // allocate automatically

    return newPartitionImpl(logical_table, partition_id, partition_name, tso, is_add_part);
}

TableID MockTiDB::newPartition(
    const String & database_name,
    const String & table_name,
    TableID partition_id,
    Timestamp tso,
    bool is_add_part)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr logical_table = getTableByNameInternal(database_name, table_name);
    return newPartitionImpl(logical_table, partition_id, toString(partition_id), tso, is_add_part);
}

TableID MockTiDB::newPartitionImpl(
    const TablePtr & logical_table,
    TableID partition_id,
    const String & partition_name,
    Timestamp tso,
    bool is_add_part)
{
    TableInfo & table_info = logical_table->table_info;
    RUNTIME_CHECK_MSG(
        !logical_table->existPartitionID(partition_id),
        "Mock TiDB table {}.{} already has partition {}, table_info={}",
        logical_table->database_name,
        logical_table->table_name,
        partition_id,
        table_info.serialize());

    table_info.is_partition_table = true;
    table_info.partition.enable = true;
    table_info.partition.num++;
    PartitionDefinition partition_def;
    partition_def.id = partition_id;
    partition_def.name = partition_name;
    table_info.partition.definitions.emplace_back(partition_def);
    table_info.update_timestamp = tso;

    if (is_add_part)
    {
        version++;

        SchemaDiff diff;
        diff.type = SchemaActionType::AddTablePartition;
        diff.schema_id = logical_table->database_id;
        diff.table_id = logical_table->id();
        diff.version = version;
        version_diff[version] = diff;
    }
    return partition_id;
}

void MockTiDB::dropPartition(const String & database_name, const String & table_name, TableID partition_id)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    TableInfo & table_info = table->table_info;

    const auto & part_def = find_if(
        table_info.partition.definitions.begin(),
        table_info.partition.definitions.end(),
        [&partition_id](PartitionDefinition & part_def) { return part_def.id == partition_id; });
    if (part_def == table_info.partition.definitions.end())
        throw Exception(
            "Mock TiDB table " + database_name + "." + table_name + " already drop partition "
                + std::to_string(partition_id),
            ErrorCodes::LOGICAL_ERROR);

    table_info.partition.num--;
    table_info.partition.definitions.erase(part_def);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropTablePartition;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

IndexInfo reverseGetIndexInfo(
    IndexID id,
    const NameAndTypePair & column,
    Int32 offset,
    TiDB::VectorIndexDefinitionPtr vector_index)
{
    IndexInfo index_info;
    index_info.id = id;
    index_info.state = TiDB::StatePublic;

    std::vector<TiDB::IndexColumnInfo> idx_cols;
    Poco::JSON::Object::Ptr idx_col_json = new Poco::JSON::Object();
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", column.name);
    name_json->set("L", column.name);
    idx_col_json->set("name", name_json);
    idx_col_json->set("length", -1);
    idx_col_json->set("offset", offset);
    TiDB::IndexColumnInfo idx_col(idx_col_json);
    index_info.idx_cols.push_back(idx_col);
    index_info.vector_index = vector_index;
    index_info.index_type = TiDB::toIndexType(vector_index->kind);

    return index_info;
}

void MockTiDB::addVectorIndexToTable(
    const String & database_name,
    const String & table_name,
    const IndexID index_id,
    const NameAndTypePair & column_name,
    Int32 offset,
    TiDB::VectorIndexDefinitionPtr vector_index)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & indexes = table->table_info.index_infos;
    if (std::find_if(indexes.begin(), indexes.end(), [&](const IndexInfo & index_) { return index_.id == index_id; })
        != indexes.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Index {} already exists in TiDB table {}",
            index_id,
            qualified_name);
    IndexInfo index_info = reverseGetIndexInfo(index_id, column_name, offset, vector_index);
    indexes.push_back(index_info);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::ActionAddVectorIndex;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::dropVectorIndexFromTable(const String & database_name, const String & table_name, IndexID index_id)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;

    auto & indexes = table->table_info.index_infos;
    auto it
        = std::find_if(indexes.begin(), indexes.end(), [&](const IndexInfo & index_) { return index_.id == index_id; });
    RUNTIME_CHECK_MSG(it != indexes.end(), "Index {} does not exist in TiDB table {}", index_id, qualified_name);
    indexes.erase(it);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropIndex;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::addColumnToTable(
    const String & database_name,
    const String & table_name,
    const NameAndTypePair & column,
    const Field & default_value)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    if (std::find_if(
            columns.begin(),
            columns.end(),
            [&](const ColumnInfo & column_) { return column_.name == column.name; })
        != columns.end())
        throw Exception(
            "Column " + column.name + " already exists in TiDB table " + qualified_name,
            ErrorCodes::LOGICAL_ERROR);

    ColumnInfo column_info = reverseGetColumnInfo(column, table->allocColumnID(), default_value, true);
    columns.emplace_back(column_info);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::AddColumn;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::dropColumnFromTable(const String & database_name, const String & table_name, const String & column_name)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) {
        return column_.name == column_name;
    });
    if (it == columns.end())
        throw Exception(
            "Column " + column_name + " does not exist in TiDB table  " + qualified_name,
            ErrorCodes::LOGICAL_ERROR);

    columns.erase(it);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionType::DropColumn;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::modifyColumnInTable(
    const String & database_name,
    const String & table_name,
    const NameAndTypePair & column)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) {
        return column_.name == column.name;
    });
    if (it == columns.end())
        throw Exception(
            "Column " + column.name + " does not exist in TiDB table  " + qualified_name,
            ErrorCodes::LOGICAL_ERROR);

    ColumnInfo column_info = reverseGetColumnInfo(column, 0, Field(), true);
    if (it->hasUnsignedFlag() != column_info.hasUnsignedFlag())
        throw Exception("Modify column " + column.name + " UNSIGNED flag is not allowed", ErrorCodes::LOGICAL_ERROR);
    if (it->tp == column_info.tp && it->hasNotNullFlag() == column_info.hasNotNullFlag())
        throw Exception("Column " + column.name + " type not changed", ErrorCodes::LOGICAL_ERROR);

    it->tp = column_info.tp;
    it->flag = column_info.flag;

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::ModifyColumn;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameColumnInTable(
    const String & database_name,
    const String & table_name,
    const String & old_column_name,
    const String & new_column_name)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) {
        return column_.name == old_column_name;
    });
    if (it == columns.end())
        throw Exception(
            "Column " + old_column_name + " does not exist in TiDB table  " + qualified_name,
            ErrorCodes::LOGICAL_ERROR);

    if (columns.end() != std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) {
            return column_.name == new_column_name;
        }))
        throw Exception(
            "Column " + new_column_name + " exists in TiDB table  " + qualified_name,
            ErrorCodes::LOGICAL_ERROR);

    it->name = new_column_name;

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::ModifyColumn;
    diff.schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameTable(const String & database_name, const String & table_name, const String & new_table_name)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    String new_qualified_name = database_name + "." + new_table_name;

    TableInfo new_table_info = table->table_info;
    new_table_info.name = new_table_name;
    auto new_table
        = std::make_shared<Table>(database_name, table->database_id, new_table_name, std::move(new_table_info));

    tables_by_id[new_table->table_info.id] = new_table;
    tables_by_name.erase(qualified_name);
    tables_by_name.emplace(new_qualified_name, new_table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::RenameTable;
    diff.schema_id = table->database_id;
    diff.old_schema_id = table->database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameTableTo(
    const String & database_name,
    const String & table_name,
    const String & new_database_name,
    const String & new_table_name)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    String new_qualified_name = new_database_name + "." + new_table_name;

    const auto old_database_id = table->database_id;
    const auto new_database = databases.find(new_database_name);
    RUNTIME_CHECK_MSG(
        new_database != databases.end(),
        "new_database is not exist in MockTiDB, new_database_name={}",
        new_database_name);
    table->database_id = new_database->second; // set new_database_id

    TableInfo new_table_info = table->table_info;
    new_table_info.name = new_table_name;
    auto new_table
        = std::make_shared<Table>(new_database_name, table->database_id, new_table_name, std::move(new_table_info));

    tables_by_id[new_table->table_info.id] = new_table;
    tables_by_name.erase(qualified_name);
    tables_by_name.emplace(new_qualified_name, new_table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::RenameTable;
    diff.schema_id = table->database_id;
    diff.old_schema_id = old_database_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameTables(const std::vector<std::tuple<std::string, std::string, std::string>> & table_name_map)
{
    std::scoped_lock lock(tables_mutex);
    version++;
    SchemaDiff diff;
    for (const auto & [database_name, table_name, new_table_name] : table_name_map)
    {
        TablePtr table = getTableByNameInternal(database_name, table_name);
        String qualified_name = database_name + "." + table_name;
        String new_qualified_name = database_name + "." + new_table_name;

        TableInfo new_table_info = table->table_info;
        new_table_info.name = new_table_name;
        auto new_table
            = std::make_shared<Table>(database_name, table->database_id, new_table_name, std::move(new_table_info));

        tables_by_id[new_table->table_info.id] = new_table;
        tables_by_name.erase(qualified_name);
        tables_by_name.emplace(new_qualified_name, new_table);

        AffectedOption opt{};
        opt.schema_id = table->database_id;
        opt.table_id = new_table->id();
        opt.old_schema_id = table->database_id;
        opt.old_table_id = table->id();
        diff.affected_opts.push_back(std::move(opt));
    }

    if (diff.affected_opts.empty())
        throw Exception("renameTables should have at least 1 affected_opts", ErrorCodes::LOGICAL_ERROR);

    diff.type = SchemaActionType::RenameTables;
    diff.schema_id = diff.affected_opts[0].schema_id;
    diff.old_schema_id = diff.affected_opts[0].schema_id;
    diff.table_id = diff.affected_opts[0].table_id;
    diff.old_table_id = diff.affected_opts[0].old_table_id;
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::truncateTable(const String & database_name, const String & table_name)
{
    std::scoped_lock lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);

    TableID old_table_id = table->table_info.id;
    table->table_info.id = table_id_allocator++;

    tables_by_id.erase(old_table_id);
    tables_by_id.emplace(table->id(), table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionType::TruncateTable;
    diff.schema_id = table->database_id;
    diff.old_table_id = old_table_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

Int64 MockTiDB::regenerateSchemaMap()
{
    std::scoped_lock lock(tables_mutex);

    SchemaDiff diff;
    diff.type = SchemaActionType::None;
    diff.regenerate_schema_map = true;
    diff.version = version + 1;
    version++;
    version_diff[version] = diff;
    return version;
}

TablePtr MockTiDB::getTableByName(const String & database_name, const String & table_name)
{
    std::scoped_lock lock(tables_mutex);

    return getTableByNameInternal(database_name, table_name);
}

TablePtr MockTiDB::getTableByNameInternal(const String & database_name, const String & table_name)
{
    String qualified_name = database_name + "." + table_name;
    auto it = tables_by_name.find(qualified_name);
    if (it == tables_by_name.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Mock TiDB table {} does not exists", qualified_name);
    }

    return it->second;
}

TablePtr MockTiDB::getTableByID(TableID table_id)
{
    if (auto it = tables_by_id.find(table_id); it != tables_by_id.end())
        return it->second;
    throw Exception(fmt::format("Mock TiDB table does not exists, table_id={}", table_id), ErrorCodes::UNKNOWN_TABLE);
}

TiDB::TableInfoPtr MockTiDB::getTableInfoByID(TableID table_id)
{
    auto it = tables_by_id.find(table_id);
    if (it == tables_by_id.end())
    {
        return nullptr;
    }
    return std::make_shared<TiDB::TableInfo>(TiDB::TableInfo(it->second->table_info));
}

TiDB::DBInfoPtr MockTiDB::getDBInfoByID(DatabaseID db_id)
{
    for (const auto & database : databases)
    {
        if (database.second == db_id)
        {
            TiDB::DBInfoPtr db_ptr = std::make_shared<TiDB::DBInfo>(TiDB::DBInfo());
            db_ptr->id = db_id;
            db_ptr->name = database.first;
            return db_ptr;
        }
    }
    // If the database has been dropped in TiKV, TiFlash get a nullptr
    return nullptr;
}

std::pair<bool, DatabaseID> MockTiDB::getDBIDByName(const String & database_name)
{
    for (const auto & database : databases)
    {
        if (database.first == database_name)
        {
            return std::make_pair(true, database.second);
        }
    }
    return std::make_pair(false, -1);
}

std::optional<SchemaDiff> MockTiDB::getSchemaDiff(Int64 version_)
{
    if (!version_diff.contains(version_))
        return std::nullopt;
    return version_diff[version_];
}

bool MockTiDB::checkSchemaDiffExists(Int64 version)
{
    return version_diff.find(version) != version_diff.end();
}

} // namespace DB
