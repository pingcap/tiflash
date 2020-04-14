#include <Core/Types.h>
#include <Storages/Transaction/Types.h>

#include <map>
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

        TableDiskInfo(TiDB::TableInfoPtr info_, std::shared_ptr<SchemaNameMapper> mapper_)
            : tidb_table_info(std::move(info_)), mapper(std::move(mapper_))
        {}

    private:
        TiDB::TableInfoPtr tidb_table_info;
        std::shared_ptr<SchemaNameMapper> mapper;

    public:
        // "metadata/${db_name}/${tbl_name}.sql"
        String getMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const;
        // "data/${db_name}/${tbl_name}/"
        String getDataDirectory(const String & root_path, const DatabaseDiskInfo & db) const;
        // "extra_data/${db_name}/${tbl_name}/"
        String getExtraDirectory(const String & root_path, const DatabaseDiskInfo & db) const;

        // "metadata/db_${db_id}/t_${id}.sql"
        String getNewMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const;
        // "data/t_${id}/"
        String getNewDataDirectory(const String & root_path, const DatabaseDiskInfo & db) const;
        // "extra_data/t_${id}"
        String getNewExtraDirectory(const String & root_path, const DatabaseDiskInfo & db) const;
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
        DatabaseDiskInfo(String name_, std::shared_ptr<SchemaNameMapper> mapper_) : name(std::move(name_)), mapper(std::move(mapper_)) {}

        void setDBInfo(TiDB::DBInfoPtr info_);

        bool hasValidTiDBInfo() const { return tidb_db_info != nullptr; }

        String newName() const;

        String getTiDBSerializeInfo() const;

        // "metadata/${db_name}.sql"
        String getMetaFilePath(const String & root_path) const { return getMetaFilePath(root_path, moved_to_tmp); }
        // "metadata/${db_name}/"
        String getMetaDirectory(const String & root_path) const { return getMetaDirectory(root_path, moved_to_tmp); }
        // "data/${db_name}/"
        String getDataDirectory(const String & root_path) const { return getDataDirectory(root_path, moved_to_tmp); }
        // "extra_data/${db_name}/"
        String getExtraDirectory(const String & extra_root) const { return getExtraDirectory(extra_root, moved_to_tmp); }

        void renameToTmpDirectories(const Context & ctx, Poco::Logger * log);

        // "metadata/db_${id}.sql"
        String getNewMetaFilePath(const String & root_path) const;
        // "metadata/db_${id}/"
        String getNewMetaDirectory(const String & root_path) const;
        // "data/"
        String getNewDataDirectory(const String & root_path) const;
        // "extra_data/"
        String getNewExtraDirectory(const String & extra_root) const;

    private:
        // "metadata/${db_name}.sql"
        String getMetaFilePath(const String & root_path, bool tmp) const;
        // "metadata/${db_name}/"
        String getMetaDirectory(const String & root_path, bool tmp) const;
        // "data/${db_name}/"
        String getDataDirectory(const String & root_path, bool tmp) const;
        // "extra_data/${db_name}/"
        String getExtraDirectory(const String & extra_root, bool tmp) const;
    };

public:
    /// Upgrader
    // If some database can not find in TiDB, they will be dropped
    // if theirs name is not in ignore_dbs
    IDAsPathUpgrader(Context & global_ctx_, bool is_mock_);

    bool needUpgrade();

    void doUpgrade();

private:
    std::vector<TiDB::DBInfoPtr> fetchInfosFromTiDB() const;

    void linkDatabaseTableInfos(const std::vector<TiDB::DBInfoPtr> & all_databases);

    void resolveConflictDirectories();

    void doRename();

    void renameDatabase(const String & db_name, const DatabaseDiskInfo & db_info);

    void renameTable(
        const String & db_name, const DatabaseDiskInfo & db_info, const String & mapped_db_name, const TableDiskInfo & table_info);

private:
    Context & global_context;

    const String root_path;

    std::map<String, DatabaseDiskInfo> databases;

    const bool is_mock = false;

    std::shared_ptr<SchemaNameMapper> mapper;

    Poco::Logger * log;
};

} // namespace DB
