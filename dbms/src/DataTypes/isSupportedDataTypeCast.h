#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// Is TiDB / TiFlash support casting DataType `from` to `to` in DDL
bool isSupportedDataTypeCast(const DataTypePtr &from, const DataTypePtr &to);

} // namespace DB
