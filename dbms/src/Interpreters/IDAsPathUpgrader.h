#include <Core/Types.h>
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
    struct DatabaseDiskInfo
    {
        String path;
        String engine;
        DatabaseID id;
    };

    struct TableDiskInfo
    {
        String meta_file_path;
        // Maybe multi-path
        std::vector<String> data_path;
        TableID id;
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

private:
    Context & global_context;

    std::map<String, DatabaseDiskInfo> databases;

    std::map<String, TableDiskInfo> tables;

    Poco::Logger * log;
};

} // namespace DB
