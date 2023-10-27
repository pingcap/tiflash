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

#pragma once

#include <Databases/DatabasesCommon.h>


namespace Poco
{
class Logger;
}


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory : public DatabaseWithOwnTablesBase
{
public:
    explicit DatabaseMemory(String name_);

    String getEngineName() const override { return "Memory"; }

    void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

    void createTable(const Context & context, const String & table_name, const ASTPtr & query) override;

    void removeTable(const Context & context, const String & table_name) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name) override;

    void alterTable(
        const Context & context,
        const String & name,
        const ColumnsDescription & columns,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(const Context & context, const String & table_name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;
    ASTPtr tryGetCreateTableQuery(const Context &, const String &) const override { return nullptr; }

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    void drop(const Context & context) override;

private:
    Poco::Logger * log;
};

} // namespace DB
