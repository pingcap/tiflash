#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

enum TMTPKType
{
    INT64,
    UINT64,
    STRING,
    UNSPECIFIED,
};

TMTPKType getTMTPKType(const IDataType & rhs);

} // namespace DB
