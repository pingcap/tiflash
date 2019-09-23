#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/Transaction/TiDB.h>


namespace DB
{
using ColumnInfo = TiDB::ColumnInfo;


DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info);

ColumnInfo getColumnInfoByDataType(const DataTypePtr &type);

} // namespace DB

