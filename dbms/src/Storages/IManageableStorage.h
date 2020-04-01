#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/Types.h>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{

struct SchemaNameMapper;

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
    explicit IManageableStorage(bool tombstone_) : IStorage(), tombstone(tombstone_) {}
    explicit IManageableStorage(const ColumnsDescription & columns_, bool tombstone_) : IStorage(columns_), tombstone(tombstone_) {}
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

    bool isTombstone() const { return tombstone; }
    void setTombstone(bool tombstone_) { IManageableStorage::tombstone = tombstone_; }

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    virtual void alterFromTiDB(const AlterCommands & commands, const String & database_name, const TiDB::TableInfo & table_info,
        const SchemaNameMapper & name_mapper, const Context & context)
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

private:
    bool tombstone;
};

} // namespace DB
