#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionHelpers.h>

#include <Debug/MockTiDB.h>

namespace DB
{

using ColumnInfo = TiDB::ColumnInfo;
using TableInfo = TiDB::TableInfo;
using PartitionInfo = TiDB::PartitionInfo;
using PartitionDefinition = TiDB::PartitionDefinition;
using Table = MockTiDB::Table;
using TablePtr = MockTiDB::TablePtr;

Table::Table(const String & database_name_, const String & table_name_, TableInfo && table_info_)
    : table_info(std::move(table_info_)), database_name(database_name_), table_name(table_name_)
{}

String MockTiDB::getSchemaJson(TableID table_id)
{
    std::lock_guard lock(tables_mutex);

    auto it = tables_by_id.find(table_id);
    if (it == tables_by_id.end())
    {
        throw Exception("Mock TiDB table with ID " + toString(table_id) + " does not exists", ErrorCodes::UNKNOWN_TABLE);
    }

    return it->second->table_info.serialize(false);
}

void MockTiDB::dropTable(const String & database_name, const String & table_name)
{
    std::lock_guard lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    auto it_by_name = tables_by_name.find(qualified_name);
    if (it_by_name == tables_by_name.end())
        return;

    const auto & table = it_by_name->second;
    if (table->isPartitionTable())
    {
        for (const auto & partition : table->table_info.partition.definitions)
        {
            tables_by_id.erase(partition.id);
        }
    }
    tables_by_id.erase(table->id());


    tables_by_name.erase(it_by_name);
}

ColumnInfo getColumnInfoFromColumn(const NameAndTypePair & column, ColumnID id)
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
    if (column.type->isUnsignedInteger())
    {
        column_info.setUnsignedFlag();
    }
    if (checkDataType<DataTypeDecimal>(nested_type))
    {
        auto decimal_type = checkAndGetDataType<DataTypeDecimal>(nested_type);
        column_info.flen = decimal_type->getPrec();
        column_info.decimal = decimal_type->getScale();
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
    throw DB::Exception("Invalid ?", ErrorCodes::LOGICAL_ERROR);

    return column_info;
}

TableID MockTiDB::newTable(const String & database_name, const String & table_name, const ColumnsDescription & columns)
{
    std::lock_guard lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    if (tables_by_name.find(qualified_name) != tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    TableInfo table_info;

    if (databases.find(database_name) != databases.end())
    {
        databases.emplace(database_name, databases.size());
    }
    table_info.db_id = databases[database_name];
    table_info.db_name = database_name;
    table_info.id = static_cast<TableID>(tables_by_id.size()) + MaxSystemTableID + 1;
    table_info.name = table_name;

    int i = 0;
    for (auto & column : columns.getAllPhysical())
    {
        table_info.columns.emplace_back(getColumnInfoFromColumn(column, i++));
    }

    table_info.pk_is_handle = false;
    table_info.comment = "Mocked.";

    auto table = std::make_shared<Table>(database_name, table_name, std::move(table_info));
    tables_by_id.emplace(table->table_info.id, table);
    tables_by_name.emplace(database_name + "." + table_name, table);

    return table->table_info.id;
}

TableID MockTiDB::newPartition(const String & database_name, const String & table_name, const String & partition_name)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    TableInfo & table_info = table->table_info;

    const auto & part_def = find_if(table_info.partition.definitions.begin(), table_info.partition.definitions.end(),
        [&partition_name](PartitionDefinition & part_def) { return part_def.name == partition_name; });
    if (part_def != table_info.partition.definitions.end())
        throw Exception(
            "Mock TiDB table " + database_name + "." + table_name + " already has partition " + partition_name, ErrorCodes::LOGICAL_ERROR);

    table_info.is_partition_table = true;
    table_info.partition.enable = true;
    table_info.partition.num++;
    TableID partition_id = table_info.id + table_info.partition.num;
    PartitionDefinition partition_def;
    partition_def.id = partition_id;
    partition_def.name = partition_name;
    table_info.partition.definitions.emplace_back(partition_def);

    // Map the same table object with partition ID as key, so mock schema syncer behaves the same as TiDB,
    // i.e. gives the table info by partition ID.
    tables_by_id.emplace(partition_id, table);

    return partition_id;
}

void MockTiDB::addColumnToTable(const String & database_name, const String & table_name, const NameAndTypePair & column)
{
    std::lock_guard lock(tables_mutex);

    TablePtr table = getTableByNameInternal(database_name, table_name);
    String qualified_name = database_name + "." + table_name;
    auto & columns = table->table_info.columns;
    if (std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & column_) { return column_.name == column.name; })
        != columns.end())
        throw Exception("Column " + column.name + " already exists in TiDB table " + qualified_name, ErrorCodes::LOGICAL_ERROR);

    ColumnInfo column_info = getColumnInfoFromColumn(column, columns.back().id + 1);
    columns.emplace_back(column_info);
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

    ColumnInfo column_info = getColumnInfoFromColumn(column, 0);
    if (it->hasUnsignedFlag() != column_info.hasUnsignedFlag())
        throw Exception("Modify column " + column.name + " UNSIGNED flag is not allowed", ErrorCodes::LOGICAL_ERROR);
    if (it->hasNotNullFlag() != column_info.hasNotNullFlag())
        throw Exception("Modify column " + column.name + " NOT NULL flag is not allowed", ErrorCodes::LOGICAL_ERROR);
    if (it->tp == column_info.tp)
        throw Exception("Column " + column.name + " type not changed", ErrorCodes::LOGICAL_ERROR);

    it->tp = column_info.tp;
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

} // namespace DB
