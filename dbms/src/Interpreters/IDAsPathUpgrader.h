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
        String meta_dir_path;
        String engine;
        DatabaseID id;

        std::vector<TableDiskInfo> tables;

        DatabaseDiskInfo(String meta_dir) : meta_dir_path(std::move(meta_dir)) {}

        String getMetaFilePath() const;
    };

public:
    IDAsPathUpgrader(Context & global_ctx_);

    bool needUpgrade();

    void doUpgrade();

private:
    std::tuple<std::vector<TiDB::DBInfoPtr>, std::vector<std::pair<TableID, DatabaseID>>> //
    fetchInfosFromTiDB() const;

    void linkDatabaseTableInfos(
        const std::vector<TiDB::DBInfoPtr> & all_databases, const std::vector<std::pair<TableID, DatabaseID>> & all_tables_mapping);

    void doRename();

    void renameDatabase(const String & db_name, const DatabaseDiskInfo & db_info);

    void renameTable(const String & db_name, const String & mapped_db_name, const TableDiskInfo & table_info);

private:
    Context & global_context;

    std::map<String, DatabaseDiskInfo> databases;

    SchemaNameMapper mapper;

    Poco::Logger * log;
};

} // namespace DB
