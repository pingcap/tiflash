#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/Transaction/TiDB.h>


namespace DB
{
using ColumnInfo = TiDB::ColumnInfo;


DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info);

TiDB::CodecFlag getCodecFlagByDataType(const DataTypePtr & dataTypePtr);

}
