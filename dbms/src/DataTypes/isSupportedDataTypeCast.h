#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

bool isSupportedDataTypeCast(const DataTypePtr &from, const DataTypePtr &to);

} // namespace DB
