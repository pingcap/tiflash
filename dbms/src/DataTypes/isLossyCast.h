#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

bool isLossyCast(const DataTypePtr &from, const DataTypePtr &to);

} // namespace DB
