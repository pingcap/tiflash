// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/Transaction/Types.h>

#include <map>
#include <memory>
#include <unordered_set>

namespace Poco
{
class Logger;
}

namespace TiDB
{
struct TableInfo;
using TableInfoPtr = std::shared_ptr<TableInfo>;

struct DBInfo;
using DBInfoPtr = std::shared_ptr<DBInfo>;
} // namespace TiDB

namespace DB
{
class Context;
class PathPool;
struct SchemaNameMapper;

class IDAsPathUpgrader
{
public:
    struct DatabaseDiskInfo;

    struct TableDiskInfo
    {
    public:
        String name() const;
        String newName() const;

        TableDiskInfo(String old_name_, TiDB::TableInfoPtr info_, std::shared_ptr<SchemaNameMapper> mapper_)
            : old_name(old_name_)
            , tidb_table_info(std::move(info_))
            , mapper(std::move(mapper_))
        {}

    private:
        String old_name;
        TiDB::TableInfoPtr tidb_table_info;
        std::shared_ptr<SchemaNameMapper> mapper;

    public:
        // "metadata/${db_name}/${tbl_name}.sql"
        String getMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const;
        // "data/${db_name}/${tbl_name}/"
        String getDataDirectory(const String & root_path, const DatabaseDiskInfo & db, bool escape_db = true, bool escape_tbl = true) const;
        // "extra_data/${db_name}/${tbl_name}/"
        String getExtraDirectory(
            const String & root_path,
            const DatabaseDiskInfo & db,
            bool escape_db = true,
            bool escape_tbl = true) const;

        // "metadata/db_${db_id}/t_${id}.sql"
        String getNewMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const;
        // "data/t_${id}/"
        String getNewDataDirectory(const String & root_path, const DatabaseDiskInfo & db) const;
        // "extra_data/t_${id}"
        String getNewExtraDirectory(const String & root_path, const DatabaseDiskInfo & db) const;

        const TiDB::TableInfo & getInfo() const;
    };

    struct DatabaseDiskInfo
    {
    public:
        static constexpr auto TMP_SUFFIX = "_flash_upgrade";

        String engine;
        std::vector<TableDiskInfo> tables;

    private:
        String name;
        std::shared_ptr<SchemaNameMapper> mapper;
        bool moved_to_tmp = false;
        TiDB::DBInfoPtr tidb_db_info = nullptr;

        const TiDB::DBInfo & getInfo() const;

    public:
        DatabaseDiskInfo(String name_, std::shared_ptr<SchemaNameMapper> mapper_)
            : name(std::move(name_))
            , mapper(std::move(mapper_))
        {}

        void setDBInfo(TiDB::DBInfoPtr info_);

        bool hasValidTiDBInfo() const { return tidb_db_info != nullptr; }

        String newName() const;

        String getTiDBSerializeInfo() const;

        // "metadata/${db_name}.sql"
        String getMetaFilePath(const String & root_path) const { return doGetMetaFilePath(root_path, moved_to_tmp); }
        // "metadata/${db_name}/"
        String getMetaDirectory(const String & root_path) const { return doGetMetaDirectory(root_path, moved_to_tmp); }
        // "data/${db_name}/"
        String getDataDirectory(const String & root_path, bool escape = true) const
        {
            return doGetDataDirectory(root_path, escape, moved_to_tmp);
        }
        // "extra_data/${db_name}/". db_name is not escaped.
        String getExtraDirectory(const String & extra_root, bool escape = true) const
        {
            return doGetExtraDirectory(extra_root, escape, moved_to_tmp);
        }

        void renameToTmpDirectories(const Context & ctx, Poco::Logger * log);

        // "metadata/db_${id}.sql"
        String getNewMetaFilePath(const String & root_path) const;
        // "metadata/db_${id}/"
        String getNewMetaDirectory(const String & root_path) const;
        // "data/"
        static String getNewDataDirectory(const String & root_path);
        // "extra_data/"
        static String getNewExtraDirectory(const String & extra_root);

    private:
        // "metadata/${db_name}.sql"
        String doGetMetaFilePath(const String & root_path, bool tmp) const;
        // "metadata/${db_name}/"
        String doGetMetaDirectory(const String & root_path, bool tmp) const;
        // "data/${db_name}/"
        String doGetDataDirectory(const String & root_path, bool escape, bool tmp) const;
        // "extra_data/${db_name}/"
        String doGetExtraDirectory(const String & extra_root, bool escape, bool tmp) const;
    };

public:
    /// Upgrader
    // If some database can not find in TiDB, they will be dropped
    // if theirs name is not in reserved_databases
    IDAsPathUpgrader(Context & global_ctx_, bool is_mock_, std::unordered_set<std::string> reserved_databases_);

    bool needUpgrade();

    void doUpgrade();

private:
    std::vector<TiDB::DBInfoPtr> fetchInfosFromTiDB() const;

    void linkDatabaseTableInfos(const std::vector<TiDB::DBInfoPtr> & all_databases);

    // Some path created by old PathPool, its database / table name is not escaped,
    // normalized those names first.
    void fixNotEscapedDirectories();

    void resolveConflictDirectories();

    void doRename();

    void renameDatabase(const String & db_name, const DatabaseDiskInfo & db_info);

    void renameTable(
        const String & db_name,
        const DatabaseDiskInfo & db_info,
        const String & mapped_db_name,
        const TableDiskInfo & table_info);

private:
    Context & global_context;

    const String root_path;

    std::map<String, DatabaseDiskInfo> databases;

    const bool is_mock = false;

    std::shared_ptr<SchemaNameMapper> mapper;

    const std::unordered_set<std::string> reserved_databases;

    Poco::Logger * log;
};

} // namespace DB
