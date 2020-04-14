#pragma once

#include <Databases/IDatabase.h>

namespace DB
{

class ASTStorage;

class DatabaseFactory
{
public:
    static DatabasePtr get(const String & database_name, const String & metadata_path, const ASTStorage * engine_define, Context & context);
};

} // namespace DB
