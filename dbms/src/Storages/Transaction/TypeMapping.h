#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <DataTypes/IDataType.h>
#include <Storages/Transaction/TiDB.h>


namespace DB
{
using ColumnInfo = TiDB::ColumnInfo;


DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info);

DataTypePtr getDataTypeByFieldType(const tipb::FieldType & field_type);

TiDB::CodecFlag getCodecFlagByDataType(const DataTypePtr & dataTypePtr);

}
