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

#include <mutex>
#include <unordered_set>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>


namespace Poco
{
    class Logger;
}


namespace DB
{
class ExternalDictionaries;

/* Database to store StorageDictionary tables
 * automatically creates tables for all dictionaries
 */
class DatabaseDictionary : public IDatabase
{
public:
    DatabaseDictionary(const String & name_, const Context & context);

    String getEngineName() const override
    {
        return "Dictionary";
    }

    void loadTables(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) override;

    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) const override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    bool empty(const Context & context) const override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void attachTable(const String & table_name, const StoragePtr & table) override;
    StoragePtr detachTable(const String & table_name) override;

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

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(
            const Context & context,
            const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    void shutdown() override;
    void drop(const Context & context) override;

private:
    const String name;
    mutable std::mutex mutex;
    const ExternalDictionaries & external_dictionaries;
    std::unordered_set<String> deleted_tables;

    Poco::Logger * log;

    Tables loadTables();

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};

}
