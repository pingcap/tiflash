#include <Debug/MockTiDB.h>

#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

using ColumnInfo = TiDB::ColumnInfo;
using TableInfo = TiDB::TableInfo;
using PartitionInfo = TiDB::PartitionInfo;
using PartitionDefinition = TiDB::PartitionDefinition;
using Table = MockTiDB::Table;
using TablePtr = MockTiDB::TablePtr;

Table::Table(const String & database_name_, const String & table_name_, TableInfo && table_info_)
    : table_info(std::move(table_info_)), database_name(database_name_), table_name(table_name_), col_id(table_info_.columns.size())
{}

MockTiDB::MockTiDB() { databases["default"] = 0; }

TablePtr MockTiDB::dropTableInternal(Context & context, const String & database_name, const String & table_name, bool drop_regions)
{
    String qualified_name = database_name + "." + table_name;
    auto it_by_name = tables_by_name.find(qualified_name);
    if (it_by_name == tables_by_name.end())
        return nullptr;

    auto & kvstore = context.getTMTContext().getKVStore();
    auto & region_table = context.getTMTContext().getRegionTable();

    auto table = it_by_name->second;
    if (table->isPartitionTable())
    {
        for (const auto & partition : table->table_info.partition.definitions)
        {
            tables_by_id.erase(partition.id);
            if (drop_regions)
            {
                for (auto & e : region_table.getRegionsByTable(partition.id))
                    kvstore->removeRegion(e.first, &region_table, kvstore->genTaskLock());
                region_table.removeTable(partition.id);
            }
        }
    }
    tables_by_id.erase(table->id());

    tables_by_name.erase(it_by_name);

    if (drop_regions)
    {
        for (auto & e : region_table.getRegionsByTable(table->id()))
            kvstore->removeRegion(e.first, &region_table, kvstore->genTaskLock());
        region_table.removeTable(table->id());
    }

    return table;
}

void MockTiDB::dropDB(Context & context, const String & database_name, bool drop_regions)
{
    std::lock_guard lock(tables_mutex);

    std::vector<String> table_names;
    std::for_each(tables_by_id.begin(), tables_by_id.end(), [&](const auto & pair) {
        if (pair.second->table_info.db_name == database_name)
            table_names.emplace_back(pair.second->table_info.name);
    });

    for (const auto & table_name : table_names)
        dropTableInternal(context, database_name, table_name, drop_regions);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionDropSchema;
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
    std::lock_guard lock(tables_mutex);

    auto table = dropTableInternal(context, database_name, table_name, drop_regions);
    if (!table)
        return;

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionDropTable;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

template <typename T>
bool tryGetDecimalType(const IDataType * nested_type, ColumnInfo & column_info)
{
    using TypeDec = DataTypeDecimal<T>;
    if (checkDataType<TypeDec>(nested_type))
    {
        auto decimal_type = checkAndGetDataType<TypeDec>(nested_type);
        column_info.flen = decimal_type->getPrec();
        column_info.decimal = decimal_type->getScale();
        column_info.tp = TiDB::TypeNewDecimal;
        return true;
    }
    return false;
}

ColumnInfo getColumnInfoFromColumn(const NameAndTypePair & column, ColumnID id, const Field & default_value)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.name = column.name;
    const IDataType * nested_type = column.type.get();
    if (!column.type->isNullable())
    {
        column_info.setNotNullFlag();
    }
    else
    {
        auto nullable_type = checkAndGetDataType<DataTypeNullable>(nested_type);
        nested_type = nullable_type->getNestedType().get();
    }
    if (nested_type->isUnsignedInteger())
    {
        column_info.setUnsignedFlag();
    }
    else if (tryGetDecimalType<Decimal32>(nested_type, column_info))
    {
    }
    else if (tryGetDecimalType<Decimal64>(nested_type, column_info))
    {
    }
    else if (tryGetDecimalType<Decimal128>(nested_type, column_info))
    {
    }
    else if (tryGetDecimalType<Decimal256>(nested_type, column_info))
    {
    }

#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w)                       \
    if (checkDataType<DataType##ct>(nested_type)) \
        column_info.tp = TiDB::Type##tt;          \
    else
    COLUMN_TYPES(M)
#undef M
    if (checkDataType<DataTypeUInt8>(nested_type))
        column_info.tp = TiDB::TypeTiny;
    else if (checkDataType<DataTypeUInt16>(nested_type))
        column_info.tp = TiDB::TypeShort;
    else if (checkDataType<DataTypeUInt32>(nested_type))
        column_info.tp = TiDB::TypeLong;

    if (auto type = checkAndGetDataType<DataTypeMyDateTime>(nested_type))
        column_info.decimal = type->getFraction();
    // UInt64 is hijacked by the macro expansion, we check it again.
    if (checkDataType<DataTypeUInt64>(nested_type))
        column_info.tp = TiDB::TypeLongLong;

    // Default value, currently we only support int.
    if (!default_value.isNull())
    {
        // convert any type to string , this is TiDB's style.

        column_info.origin_default_value = applyVisitor(FieldVisitorToString(), default_value);
    }
    else
    {
        column_info.setNoDefaultValueFlag();
    }

    return column_info;
}

DatabaseID MockTiDB::newDataBase(const String & database_name)
{
    DatabaseID schema_id = 0;

    if (databases.find(database_name) == databases.end())
    {
        schema_id = databases.size() + 1;
        databases.emplace(database_name, schema_id);
    }

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionCreateSchema;
    diff.schema_id = schema_id;
    diff.version = version;
    version_diff[version] = diff;

    return schema_id;
}

TableID MockTiDB::newTable(
    const String & database_name, const String & table_name, const ColumnsDescription & columns, Timestamp tso, String engine_type)
{
    std::lock_guard lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    if (tables_by_name.find(qualified_name) != tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    if (databases.find(database_name) == databases.end())
    {
        throw Exception("MockTiDB not found db: " + database_name, ErrorCodes::LOGICAL_ERROR);
    }

    TableInfo table_info;
    table_info.db_id = databases[database_name];
    table_info.db_name = database_name;
    table_info.id = table_id_allocator++;
    table_info.name = table_name;

    int i = 0;
    for (auto & column : columns.getAllPhysical())
    {
        table_info.columns.emplace_back(getColumnInfoFromColumn(column, i++, Field()));
    }

    table_info.pk_is_handle = false;
    table_info.comment = "Mocked.";
    table_info.update_timestamp = tso;

    // set storage engine type
    std::transform(engine_type.begin(), engine_type.end(), engine_type.begin(), [](unsigned char c) { return std::tolower(c); });
    if (engine_type == "tmt")
        table_info.engine_type = TiDB::StorageEngine::TMT;
    else if (engine_type == "dm")
        table_info.engine_type = TiDB::StorageEngine::DM;
    else if (engine_type == "buggy")
        table_info.engine_type = TiDB::StorageEngine::DEBUGGING_MEMORY;
    else
        throw Exception("Unknown engine type : " + engine_type + ", must be 'tmt' or 'dm'", ErrorCodes::BAD_ARGUMENTS);

    auto table = std::make_shared<Table>(database_name, table_name, std::move(table_info));
    tables_by_id.emplace(table->table_info.id, table);
    tables_by_name.emplace(qualified_name, table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionCreateTable;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;

    return table->table_info.id;
}

void MockTiDB::newPartition(const String & database_name, const String & table_name, TableID partition_id, Timestamp tso, bool is_add_part)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    TableInfo & table_info = table->table_info;

    const auto & part_def = find_if(table_info.partition.definitions.begin(), table_info.partition.definitions.end(),
        [&partition_id](PartitionDefinition & part_def) { return part_def.id == partition_id; });
    if (part_def != table_info.partition.definitions.end())
        throw Exception("Mock TiDB table " + database_name + "." + table_name + " already has partition " + std::to_string(partition_id),
            ErrorCodes::LOGICAL_ERROR);

    table_info.is_partition_table = true;
    table_info.partition.enable = true;
    table_info.partition.num++;
    PartitionDefinition partition_def;
    partition_def.id = partition_id;
    partition_def.name = std::to_string(partition_id);
    table_info.partition.definitions.emplace_back(partition_def);
    table_info.update_timestamp = tso;

    if (is_add_part)
    {
        version++;

        SchemaDiff diff;
        diff.type = SchemaActionAddTablePartition;
        diff.schema_id = table->table_info.db_id;
        diff.table_id = table->id();
        diff.version = version;
        version_diff[version] = diff;
    }
}

void MockTiDB::dropPartition(const String & database_name, const String & table_name, TableID partition_id)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    TableInfo & table_info = table->table_info;

    const auto & part_def = find_if(table_info.partition.definitions.begin(), table_info.partition.definitions.end(),
        [&partition_id](PartitionDefinition & part_def) { return part_def.id == partition_id; });
    if (part_def == table_info.partition.definitions.end())
        throw Exception("Mock TiDB table " + database_name + "." + table_name + " already drop partition " + std::to_string(partition_id),
            ErrorCodes::LOGICAL_ERROR);

    table_info.partition.num--;
    table_info.partition.definitions.erase(part_def);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionDropTablePartition;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::addColumnToTable(
    const String & database_name, const String & table_name, const NameAndTypePair & column, const Field & default_value)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    if (std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == column.name; })
        != columns.end())
        throw Exception("Column " + column.name + " already exists in TiDB table " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    ColumnInfo column_info = getColumnInfoFromColumn(column, table->allocColumnID(), default_value);
    columns.emplace_back(column_info);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionAddColumn;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::dropColumnFromTable(const String & database_name, const String & table_name, const String & column_name)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == column_name; });
    if (it == columns.end())
        throw Exception("Column " + column_name + " does not exist in TiDB table  " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    columns.erase(it);

    version++;

    SchemaDiff diff;
    diff.type = SchemaActionDropColumn;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::modifyColumnInTable(const String & database_name, const String & table_name, const NameAndTypePair & column)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == column.name; });
    if (it == columns.end())
        throw Exception("Column " + column.name + " does not exist in TiDB table  " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    ColumnInfo column_info = getColumnInfoFromColumn(column, 0, Field());
    if (it->hasUnsignedFlag() != column_info.hasUnsignedFlag())
        throw Exception("Modify column " + column.name + " UNSIGNED flag is not allowed", ErrorCodes::LOGICAL_ERROR);
    if (it->tp == column_info.tp && it->hasNotNullFlag() == column_info.hasNotNullFlag())
        throw Exception("Column " + column.name + " type not changed", ErrorCodes::LOGICAL_ERROR);

    it->tp = column_info.tp;
    it->flag = column_info.flag;

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionModifyColumn;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameColumnInTable(
    const String & database_name, const String & table_name, const String & old_column_name, const String & new_column_name)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    auto it = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == old_column_name; });
    if (it == columns.end())
        throw Exception("Column " + old_column_name + " does not exist in TiDB table  " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    if (columns.end()
        != std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == new_column_name; }))
        throw Exception("Column " + new_column_name + " exists in TiDB table  " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    it->name = new_column_name;

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionModifyColumn;
    diff.schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::renameTable(const String & database_name, const String & table_name, const String & new_table_name)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    String new_qualified_name = database_name + "." + new_table_name;

    TableInfo new_table_info = table->table_info;
    new_table_info.name = new_table_name;
    auto new_table = std::make_shared<Table>(database_name, new_table_name, std::move(new_table_info));

    tables_by_id[new_table->table_info.id] = new_table;
    tables_by_name.erase(qualified_name);
    tables_by_name.emplace(new_qualified_name, new_table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionRenameTable;
    diff.schema_id = table->table_info.db_id;
    diff.old_schema_id = table->table_info.db_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

void MockTiDB::truncateTable(const String & database_name, const String & table_name)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);

    TableID old_table_id = table->table_info.id;
    table->table_info.id = table_id_allocator++;

    tables_by_id.erase(old_table_id);
    tables_by_id.emplace(table->id(), table);

    version++;
    SchemaDiff diff;
    diff.type = SchemaActionTruncateTable;
    diff.schema_id = table->table_info.db_id;
    diff.old_table_id = old_table_id;
    diff.table_id = table->id();
    diff.version = version;
    version_diff[version] = diff;
}

TablePtr MockTiDB::getTableByName(const String & database_name, const String & table_name)
{
    std::lock_guard lock(tables_mutex);

    return getTableByNameInternal(database_name, table_name);
}

TablePtr MockTiDB::getTableByNameInternal(const String & database_name, const String & table_name)
{
    String qualified_name = database_name + "." + table_name;
    auto it = tables_by_name.find(qualified_name);
    if (it == tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " does not exists", ErrorCodes::UNKNOWN_TABLE);
    }

    return it->second;
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
    TiDB::DBInfoPtr db_ptr = std::make_shared<TiDB::DBInfo>(TiDB::DBInfo());
    db_ptr->id = db_id;
    for (auto it = databases.begin(); it != databases.end(); it++)
    {
        if (it->second == db_id)
        {
            db_ptr->name = it->first;
            break;
        }
    }
    return db_ptr;
}

SchemaDiff MockTiDB::getSchemaDiff(Int64 version) { return version_diff[version]; }

} // namespace DB
