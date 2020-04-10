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
    struct TableDiskInfo
    {
        TableID id;
        String name;
        String meta_file_path;
    };

    struct DatabaseDiskInfo
    {
        static constexpr auto TMP_SUFFIX = "_flash_upgrade";

        String meta_dir_path;
        String engine;
        DatabaseID id = 0;
        bool moved_to_tmp = false;

        std::vector<TableDiskInfo> tables;

        DatabaseDiskInfo(String meta_dir) : meta_dir_path(std::move(meta_dir)) {}

        // "metadata/${db_name}.sql"
        String getMetaFilePath() const;

        String getDataDirectory(const String & root, const String & name) const;

        std::vector<String> getExtraDirectories(const PathPool & pool, const String & name) const;

        void renameToTmpDirectories(const Context & ctx);
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
