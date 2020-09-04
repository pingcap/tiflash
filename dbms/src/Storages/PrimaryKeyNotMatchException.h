#pragma once

#include <Core/Types.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class Context;

struct PrimaryKeyNotMatchException
{
    // The primary key name in definition
    const String pri_key;
    // The actual primary key name in TiDB::TableInfo
    const String actual_pri_key;
    PrimaryKeyNotMatchException(const String & pri_key_, const String & actual_pri_key_)
        : pri_key(pri_key_), actual_pri_key(actual_pri_key_)
    {}
};

// This function will replace the primary key and update statement in `table_metadata_path`. The correct statement will be return.
String fixCreateStatementWithPriKeyNotMatchException(Context & context, const String old_definition, const String & table_metadata_path,
    const PrimaryKeyNotMatchException & ex, Poco::Logger * log);

} // namespace DB
