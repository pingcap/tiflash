#pragma once

#include <Core/Types.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class Context;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct PriKeyNameNotMatchException
{
    // The primary key name in definition
    const String pri_key;
    // The actual primary key name in TiDB::TableInfo
    const String actual_pri_key;
    PriKeyNameNotMatchException(String && pri_key_, String && actual_pri_key_) : pri_key(pri_key_), actual_pri_key(actual_pri_key_) {}
};

// This function will replace the primary key and update statement in `table_metadata_path`. The correct statement will be return.
String fixCreateStatementWithPriKeyNotMatchException(Context & context, const String old_definition, const String & table_metadata_path,
    const PriKeyNameNotMatchException & ex, Poco::Logger * log);

} // namespace DB
