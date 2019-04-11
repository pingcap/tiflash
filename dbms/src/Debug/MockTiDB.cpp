#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

#include <Debug/MockTiDB.h>
#include <Debug/dbgTools.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
}

using ColumnInfo = TiDB::ColumnInfo;
using TableInfo = TiDB::TableInfo;
using Table = MockTiDB::Table;
using TablePtr = MockTiDB::TablePtr;

Table::Table(const String & database_name_, const String & table_name_, TableInfo && table_info_)
    : table_info(std::move(table_info_)), database_name(database_name_), table_name(table_name_)
{
}

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

    for (auto it = tables_by_id.begin(); it != tables_by_id.begin(); ++it)
        if (it->second.get() == it_by_name->second.get())
            it = tables_by_id.erase(it);

    tables_by_name.erase(it_by_name);
}

TableID MockTiDB::newTable(const String & database_name, const String & table_name, const ColumnsDescription & columns, const String & primary_key)
{
    std::lock_guard lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    if (tables_by_name.find(qualified_name) != tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    TableInfo table_info;
    bool pk_is_handle = false;

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
        ColumnInfo column_info;
        column_info.id = (i++);
        column_info.name = column.name;
        const IDataType *nested_type = column.type.get();
        if (column.name == primary_key)
        {
            if (pk_is_handle)
            {
                throw Exception("primary key can only be one column");
            }
            column_info.setPriKeyFlag();
            std::string handle_column_type_name = std::string(column.type->getFamilyName());
            if ((handle_column_type_name == "Int64")
                || (handle_column_type_name == "Int32")
                || (handle_column_type_name == "Int16")
                || (handle_column_type_name == "Int8")
                || (handle_column_type_name == "UInt64")
                || (handle_column_type_name == "UInt32")
                || (handle_column_type_name == "UInt16")
                || (handle_column_type_name == "UInt8"))
            {
                pk_is_handle = true;
            }
        }
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
#define M(tt, v, cf, cfu, ct, ctu) \
        if (checkDataType<DataType##ct>(nested_type) || checkDataType<DataType##ctu>(nested_type)) column_info.tp = TiDB::Type##tt; else
        COLUMN_TYPES(M)
#undef M
        throw DB::Exception("Invalid ?", ErrorCodes::LOGICAL_ERROR);

        table_info.columns.emplace_back(column_info);
    }

    table_info.pk_is_handle = pk_is_handle;

    table_info.comment = "Mocked.";

    auto table = std::make_shared<Table>(database_name, table_name, std::move(table_info));
    tables_by_id.emplace(table->table_info.id, table);
    tables_by_name.emplace(database_name + "." + table_name, table);

    return table->table_info.id;
}

TablePtr MockTiDB::getTableByName(const String & database_name, const String & table_name)
{
    std::lock_guard lock(tables_mutex);

    String qualified_name = database_name + "." + table_name;
    auto it = tables_by_name.find(qualified_name);
    if (it == tables_by_name.end())
    {
        throw Exception("Mock TiDB table " + qualified_name + " does not exists", ErrorCodes::UNKNOWN_TABLE);
    }

    return it->second;
}

}
