#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{
class IManageableStorage : public IStorage
{
public:
    enum StorageEngine
    {
        TMT = 0,
        DM,
    };

public:
    explicit IManageableStorage() : IStorage() {}
    explicit IManageableStorage(const ColumnsDescription & columns_) : IStorage(columns_) {}
    ~IManageableStorage() override = default;

    virtual void flushDelta() {}

    virtual BlockInputStreamPtr status() { return {}; }

    virtual void check(const Context &) {}

    virtual StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    virtual void alterFromTiDB(
            const AlterCommands & commands, const TiDB::TableInfo & table_info, const String & database_name, const Context & context)
    = 0;
};

} // namespace DB