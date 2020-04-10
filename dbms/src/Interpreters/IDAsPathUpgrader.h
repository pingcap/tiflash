#include <Core/Types.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/Types.h>

#include <map>

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

class IDAsPathUpgrader
{
    struct DatabaseDiskInfo;

    struct TableDiskInfo
    {
        TableID id;
        String name;
        String meta_file_path;

    public:
        // "metadata/${db_name}/${tbl_name}.sql"
        String getMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const;
        // "data/${db_name}/${tbl_name}/"
        String getDataDirectory(const String & root_path, const DatabaseDiskInfo & db) const;
        // "extra_data/${db_name}/${tbl_name}/"
        String getExtraDirectory(const String & root_path, const DatabaseDiskInfo & db) const;

        // "metadata/t_${id}.sql"
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

        DatabaseID id = -1;
        String engine;
        std::vector<TableDiskInfo> tables;

    private:
        String name;
        bool moved_to_tmp = false;

    public:
        DatabaseDiskInfo(String name_) : name(std::move(name_)) {}

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
        // "metadata/"
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
    IDAsPathUpgrader(Context & global_ctx_);

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

    SchemaNameMapper mapper;

    Poco::Logger * log;
};

} // namespace DB
