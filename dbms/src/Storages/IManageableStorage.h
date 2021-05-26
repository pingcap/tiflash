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
class ASTStorage;
class Region;

namespace DM
{
struct RowKeyRange;
}

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
    explicit IManageableStorage(Timestamp tombstone_) : IStorage(), tombstone(tombstone_) {}
    explicit IManageableStorage(const ColumnsDescription & columns_, Timestamp tombstone_) : IStorage(columns_), tombstone(tombstone_) {}
    ~IManageableStorage() override = default;

    virtual void flushCache(const Context & /*context*/) {}

    virtual void flushCache(const Context & /*context*/, const DM::RowKeyRange & /*range_to_flush*/) {}

    virtual BlockInputStreamPtr status() { return {}; }

    virtual void checkStatus(const Context &) {}

    virtual void deleteRows(const Context &, size_t /*rows*/) { throw Exception("Unsupported"); }

    // `limit` is the max number of segments to gc, return value is the number of segments gced
    virtual UInt64 onSyncGc(Int64 /*limit*/) { throw Exception("Unsupported"); }
    
    // Return true is data dir exist
    virtual bool initStoreIfDataDirExist() { throw Exception("Unsupported"); }

    virtual void mergeDelta(const Context &) { throw Exception("Unsupported"); }

    virtual BlockInputStreamPtr listSegments(const Context &) { throw Exception("Unsupported"); }

    virtual ::TiDB::StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    // Update tidb table info in memory.
    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    bool isTombstone() const { return tombstone; }
    Timestamp getTombstone() const { return tombstone; }
    void setTombstone(Timestamp tombstone_) { IManageableStorage::tombstone = tombstone_; }

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    // Once called, table_info is guaranteed to be persisted, regardless commands being empty or not.
    virtual void alterFromTiDB(const TableLockHolder &, const AlterCommands & commands, const String & database_name,
        const TiDB::TableInfo & table_info, const SchemaNameMapper & name_mapper, const Context & context)
        = 0;

    /** Rename the table.
      *
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * Different from `IStorage::rename`, storage's data path do not contain database name, nothing to do with data path, `new_path_to_db` is ignored.
      * But `getDatabaseName` and `getTableInfo` means we usally store database name / TiDB table info as member in storage,
      * we need to update database name with `new_database_name`, and table name in tidb table info with `new_display_table_name`.
      *
      * Called when the table structure is locked for write.
      * TODO: For TiFlash, we can rename without any lock on data?
      */
    virtual void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name,
        const String & new_display_table_name)
        = 0;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
    {
        // Keep for DatabaseOrdinary::rename, only use for develop
        return rename(new_path_to_db, new_database_name, new_table_name, /*new_display_table_name=*/new_table_name);
    }

    virtual void modifyASTStorage(ASTStorage * /*storage*/, const TiDB::TableInfo & /*table_info*/)
    {
        throw Exception("Method modifyASTStorage is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

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

    virtual bool isCommonHandle() const { return false; }

    virtual size_t getRowKeyColumnSize() const { return 1; }

private:
    virtual DataTypePtr getPKTypeImpl() const = 0;

private:
    /// Timestamp when this table is dropped.
    /// Zero means this table is not dropped.
    Timestamp tombstone;
};

} // namespace DB
