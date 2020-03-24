#pragma once

#include <DataStreams/CoprocessorBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <pingcap/coprocessor/Client.h>
#include <tipb/select.pb.h>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{
/**
 * An interface for Storages synced from TiDB.
 *
 * Note that you must override `startup` and `shutdown` to register/remove this table into TMTContext.
 */
class IManageableStorage : public IStorage
{
public:
    enum class PKType
    {
        INT64 = 0,
        UINT64,
        UNSPECIFIED,
    };

public:
    explicit IManageableStorage() : IStorage() {}
    explicit IManageableStorage(const ColumnsDescription & columns_) : IStorage(columns_) {}
    ~IManageableStorage() override = default;

    virtual void flushDelta() {}

    virtual void flushCache(const Context & /*context*/, const DB::HandleRange<HandleID> & /* range_to_flush */) {}

    virtual BlockInputStreamPtr status() { return {}; }

    virtual void checkStatus(const Context &) {}

    virtual void deleteRows(const Context &, size_t /*rows*/) { throw Exception("Unsupported"); }

    virtual BlockInputStreamPtr listSegments(const Context &) { throw Exception("Unsupported"); }

    virtual ::TiDB::StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    virtual void alterFromTiDB(
        const AlterCommands & commands, const TiDB::TableInfo & table_info, const String & database_name, const Context & context)
        = 0;

    
    /// Remove this storage from TMTContext. Should be called after its metadata and data have been removed from disk.
    virtual void removeFromTMTContext() = 0;

    PKType getPKType() const
    {
        static const DataTypeInt64 & dataTypeInt64 = {};
        static const DataTypeUInt64 & dataTypeUInt64 = {};

        auto pk_data_type = getPKTypeImpl();
        if (pk_data_type->equals(dataTypeInt64))
            return PKType::INT64;
        else if (pk_data_type->equals(dataTypeUInt64))
            return PKType::UINT64;
        return PKType::UNSPECIFIED;
    }

    virtual SortDescription getPrimarySortDescription() const = 0;

private:
    virtual DataTypePtr getPKTypeImpl() const = 0;
};

} // namespace DB
