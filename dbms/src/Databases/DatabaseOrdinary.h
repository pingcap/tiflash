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


namespace DB
{
/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseWithOwnTablesBase
{
public:
    DatabaseOrdinary(String name_, const String & metadata_path_, const Context & context);

    String getEngineName() const override { return "Ordinary"; }

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
        const ASTModifier & storage_modifier) override;

    time_t getTableMetadataModificationTime(const Context & context, const String & table_name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    String getDataPath() const override;
    String getMetadataPath() const override;
    String getTableMetadataPath(const String & table_name) const override;

    void shutdown() override;
    void drop(const Context & context) override;

private:
    const String metadata_path;
    const String data_path;
    Poco::Logger * log;

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};

} // namespace DB
