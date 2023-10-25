// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}

DatabaseMemory::DatabaseMemory(String name_)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , log(&Poco::Logger::get("DatabaseMemory(" + name + ")"))
{}

void DatabaseMemory::loadTables(
    Context & /*context*/,
    ThreadPool * /*thread_pool*/,
    bool /*has_force_restore_data_flag*/)
{
    /// Nothing to load.
}

void DatabaseMemory::createTable(const Context & /*context*/, const String & /*table_name*/, const ASTPtr & /*query*/)
{
    /// Nothing to do.
}

void DatabaseMemory::removeTable(const Context & /*context*/, const String & table_name)
{
    detachTable(table_name);
}

void DatabaseMemory::renameTable(const Context &, const String &, IDatabase &, const String &)
{
    throw Exception("DatabaseMemory: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseMemory::alterTable(const Context &, const String &, const ColumnsDescription &, const ASTModifier &)
{
    throw Exception("DatabaseMemory: alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseMemory::getTableMetadataModificationTime(const Context &, const String &)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseMemory::getCreateTableQuery(const Context &, const String &) const
{
    throw Exception(
        "There is no CREATE TABLE query for DatabaseMemory tables",
        ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery(const Context &) const
{
    throw Exception("There is no CREATE DATABASE query for DatabaseMemory", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

void DatabaseMemory::drop(const Context & /*context*/)
{
    /// Additional actions to delete database are not required.
}

} // namespace DB
