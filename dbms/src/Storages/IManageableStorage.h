// Copyright 2023 PingCAP, Inc.
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

#include <Common/UniThreadPool.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/StorageEngineType.h>
#include <Storages/KVStore/Types.h>


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
struct GCOptions;
} // namespace DM
using BlockUPtr = std::unique_ptr<Block>;

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
    explicit IManageableStorage(Timestamp tombstone_)
        : IStorage()
        , tombstone(tombstone_)
    {}
    explicit IManageableStorage(const ColumnsDescription & columns_, Timestamp tombstone_)
        : IStorage(columns_)
        , tombstone(tombstone_)
    {}
    ~IManageableStorage() override = default;

    virtual void flushCache(const Context & /*context*/) {}

    virtual bool flushCache(
        const Context & /*context*/,
        const DM::RowKeyRange & /*range_to_flush*/,
        [[maybe_unused]] bool try_until_succeed)
    {
        return true;
    }

    // Get the statistics of this table.
    // Used by `manage table xxx status` in ch-client
    virtual BlockInputStreamPtr status() { return {}; }

    virtual void checkStatus(const Context &) {}

    virtual void deleteRows(const Context &, size_t /*rows*/) { throw Exception("Unsupported"); }

    /// `limit` is the max number of segments to gc, return value is the number of segments gced
    virtual UInt64 onSyncGc(Int64 /*limit*/, const DM::GCOptions &) { throw Exception("Unsupported"); }

    /// Return true if the DeltaMergeStore instance need to be inited
    virtual bool initStoreIfNeed(ThreadPool * /*thread_pool*/) { throw Exception("Unsupported"); }

    virtual TiDB::StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    /// Update tidb table info in memory.
    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    bool isTombstone() const { return tombstone; }
    Timestamp getTombstone() const { return tombstone; }
    void setTombstone(Timestamp tombstone_) { IManageableStorage::tombstone = tombstone_; }

    virtual void updateTombstone(
        const TableLockHolder &,
        const AlterCommands & commands,
        const String & database_name,
        const TiDB::TableInfo & table_info,
        const SchemaNameMapper & name_mapper,
        const Context & context)
        = 0;

    virtual void alterSchemaChange(
        const TableLockHolder &,
        TiDB::TableInfo & table_info,
        const String & database_name,
        const String & table_name,
        const Context & context)
        = 0;

    virtual DM::ColumnDefines getStoreColumnDefines() const = 0;
    /// Rename the table.
    ///
    /// Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
    /// Different from `IStorage::rename`, storage's data path do not contain database name, nothing to do with data path, `new_path_to_db` is ignored.
    /// But `getDatabaseName` and `getTableInfo` means we usually store database name / TiDB table info as member in storage,
    /// we need to update database name with `new_database_name`, and table name in tidb table info with `new_display_table_name`.
    ///
    /// Called when the table structure is locked for write.
    virtual void rename(
        const String & new_path_to_db,
        const String & new_database_name,
        const String & new_table_name,
        const String & new_display_table_name)
        = 0;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
    {
        // Keep for DatabaseOrdinary::rename, only use for develop
        return rename(new_path_to_db, new_database_name, new_table_name, /*new_display_table_name=*/new_table_name);
    }

    virtual void modifyASTStorage(ASTStorage * /*storage*/, const TiDB::TableInfo & /*table_info*/)
    {
        throw Exception(
            "Method modifyASTStorage is not supported by storage " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Remove this storage from TMTContext. Should be called after its metadata and data have been removed from disk.
    virtual void removeFromTMTContext() = 0;

    PKType getPKType() const
    {
        static const DataTypeInt64 & data_type_int64 = {};
        static const DataTypeUInt64 & data_type_u_int64 = {};

        auto pk_data_type = getPKTypeImpl();
        if (pk_data_type->equals(data_type_int64))
            return PKType::INT64;
        else if (pk_data_type->equals(data_type_u_int64))
            return PKType::UINT64;
        return PKType::UNSPECIFIED;
    }

    virtual SortDescription getPrimarySortDescription() const = 0;

    virtual bool isCommonHandle() const { return false; }

    virtual size_t getRowKeyColumnSize() const { return 1; }

    /// when `need_block` is true, it will try return a cached block corresponding to DecodingStorageSchemaSnapshotConstPtr,
    ///     and `releaseDecodingBlock` need to be called when the block is free
    /// when `need_block` is false, it will just return an nullptr
    /// This method must be called under the protection of table structure lock
    virtual std::pair<DB::DecodingStorageSchemaSnapshotConstPtr, BlockUPtr> getSchemaSnapshotAndBlockForDecoding(
        const TableStructureLockHolder & /* table_structure_lock */,
        bool /* need_block */,
        bool /* has_version_block */)
    {
        throw Exception(
            "Method getDecodingSchemaSnapshot is not supported by storage " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /// The `block_decoding_schema_epoch` is just an internal version for `DecodingStorageSchemaSnapshot`,
    /// And it has no relation with the table schema version.
    virtual void releaseDecodingBlock(Int64 /* block_decoding_schema_epoch */, BlockUPtr /* block */)
    {
        throw Exception(
            "Method getDecodingSchemaSnapshot is not supported by storage " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    virtual DataTypePtr getPKTypeImpl() const = 0;

private:
    /// Timestamp when this table is dropped.
    /// Zero means this table is not dropped.
    Timestamp tombstone;
};

} // namespace DB
