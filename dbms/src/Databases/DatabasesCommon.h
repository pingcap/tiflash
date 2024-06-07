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

#include <Core/Types.h>
#include <Databases/IDatabase.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>


/// General functionality for several different database engines.

namespace DB
{
class Context;


/** Get the row with the table definition based on the CREATE query.
  * It is an ATTACH query that you can execute to create a table from the correspondent database.
  * See the implementation.
  */
String getTableDefinitionFromCreateQuery(const ASTPtr & query);

/** Get the row with the database definition based on the CREATE query.
  * It is an ATTACH query that you can execute to create a database.
  * See the implementation.
  */
String getDatabaseDefinitionFromCreateQuery(const ASTPtr & query);


/** Create a table by its definition, without using InterpreterCreateQuery.
  *  (InterpreterCreateQuery has more complex functionality, and it can not be used if the database has not been created yet)
  * Returns the table name and the table itself.
  * You must subsequently call IStorage::startup method to use the table.
  */
std::pair<String, StoragePtr> createTableFromDefinition(
    const String & definition,
    const String & database_name,
    const String & database_data_path,
    const String & database_engine,
    Context & context,
    bool has_force_restore_data_flag,
    const String & description_for_error_message);

/// Some helper functions for loading database metadata.
namespace DatabaseLoading
{
ASTPtr getQueryFromMetadata(const Context & context, const String & metadata_path, bool throw_on_error = true);

ASTPtr getCreateQueryFromMetadata(
    const Context & context,
    const String & metadata_path,
    const String & database,
    bool throw_on_error);

std::vector<String> listSQLFilenames(const String & meta_dir, Poco::Logger * log);

void cleanupTables(IDatabase & database, const String & db_name, const Tables & tables, Poco::Logger * log);

std::tuple<String, StoragePtr> loadTable(
    Context & context,
    IDatabase & database,
    const String & database_metadata_path,
    const String & database_name,
    const String & database_data_path,
    const String & database_engine,
    const String & file_name,
    bool has_force_restore_data_flag);
} // namespace DatabaseLoading


/// Copies list of tables and iterates through such snapshot.
class DatabaseSnapshotIterator : public IDatabaseIterator
{
private:
    Tables tables;
    Tables::iterator it;

public:
    explicit DatabaseSnapshotIterator(Tables & tables_)
        : tables(tables_)
        , it(tables.begin())
    {}

    explicit DatabaseSnapshotIterator(Tables && tables_)
        : tables(tables_)
        , it(tables.begin())
    {}

    void next() override { ++it; }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->first; }

    StoragePtr & table() const override { return it->second; }
};

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public IDatabase
{
public:
    bool isTableExist(const Context & context, const String & table_name) const override;

    StoragePtr tryGetTable(const Context & context, const String & table_name) const override;

    bool empty(const Context & context) const override;


    void attachTable(const String & table_name, const StoragePtr & table) override;

    StoragePtr detachTable(const String & table_name) override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    void shutdown() override;

     ~DatabaseWithOwnTablesBase() override;

protected:
    String name;

    mutable std::mutex mutex;
    Tables tables;

    explicit DatabaseWithOwnTablesBase(String name_)
        : name(std::move(name_))
    {}
};

} // namespace DB
