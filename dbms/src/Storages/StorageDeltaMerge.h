#pragma once

#include <Core/Defines.h>
#include <Core/SortDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/TiDB.h>

#include <ext/shared_ptr_helper.h>

namespace Poco
{
class Logger;
}

namespace DB
{

namespace DM
{
struct RowKeyRange;
class DeltaMergeStore;
using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;
} // namespace DM

class StorageDeltaMerge : public ext::shared_ptr_helper<StorageDeltaMerge>, public IManageableStorage
{
public:
    ~StorageDeltaMerge() override;

    bool supportsModification() const override { return true; }

    String getName() const override;
    String getTableName() const override;
    String getDatabaseName() const override;

    void drop() override;

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    /// Write from raft layer.
    void write(Block && block, const Settings & settings);

    void flushCache(const Context & context) override;

    void flushCache(const Context & context, const DM::RowKeyRange & range_to_flush) override;

    void mergeDelta(const Context & context) override;

    void deleteRange(const DM::RowKeyRange & range_to_delete, const Settings & settings);

    void ingestFiles(
        const DM::RowKeyRange & range, const std::vector<UInt64> & file_ids, bool clear_data_in_range, const Settings & settings);

    UInt64 onSyncGc(Int64) override;

    void rename(const String & new_path_to_db,
        const String & new_database_name,
        const String & new_table_name,
        const String & new_display_table_name) override;

    void modifyASTStorage(ASTStorage * storage_ast, const TiDB::TableInfo & table_info) override;

    void alter(const TableLockHolder &,
        const AlterCommands & commands,
        const String & database_name,
        const String & table_name,
        const Context & context) override;

    ::TiDB::StorageEngine engineType() const override { return ::TiDB::StorageEngine::DT; }

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    void alterFromTiDB(const TableLockHolder &, //
        const AlterCommands & commands,         //
        const String & database_name,           //
        const TiDB::TableInfo & table_info,     //
        const SchemaNameMapper & name_mapper,   //
        const Context & context) override;

    void setTableInfo(const TiDB::TableInfo & table_info_) override { tidb_table_info = table_info_; }

    const TiDB::TableInfo & getTableInfo() const override { return tidb_table_info; }

    void startup() override;

    void shutdown() override;

    void removeFromTMTContext() override;

    SortDescription getPrimarySortDescription() const override;

    const OrderedNameSet & getHiddenColumnsImpl() const override { return hidden_columns; }

    BlockInputStreamPtr status() override;

    void checkStatus(const Context & context) override;
    void deleteRows(const Context &, size_t rows) override;

    const DM::DeltaMergeStorePtr & getStore() 
    {
         return getAndMaybeInitStore();
    }

    bool isCommonHandle() const override { return is_common_handle; }

    size_t getRowKeyColumnSize() const override { return rowkey_column_size; }
    
    bool initStoreIfDataDirExist() override;
protected:
    StorageDeltaMerge( //
        const String & db_engine,
        const String & db_name_,
        const String & name_,
        const DM::OptionTableInfoConstRef table_info_,
        const ColumnsDescription & columns_,
        const ASTPtr & primary_expr_ast_,
        Timestamp tombstone,
        Context & global_context_);

    Block buildInsertBlock(bool is_import, bool is_delete, const Block & block);

private:
    void alterImpl(const AlterCommands & commands,
        const String & database_name,
        const String & table_name,
        const DB::DM::OptionTableInfoConstRef table_info_,
        const Context & context);

    DataTypePtr getPKTypeImpl() const override;

    DM::DeltaMergeStorePtr& getAndMaybeInitStore();
    bool storeInited() const { return store_inited.load(std::memory_order_acquire); }
    void updateTableColumnInfo();
    DM::ColumnDefines getStoreColumnDefines() const;
    bool dataDirExist();
private:
    struct TableColumnInfo 
    {
        TableColumnInfo(const String& db, const String& table, const ASTPtr& pk)
            : db_name(db), table_name(table), pk_expr_ast(pk) {}
        String db_name;
        String table_name;
        ASTPtr pk_expr_ast;
        DM::ColumnDefines table_column_defines;
        DM::ColumnDefine handle_column_define;
    };
    const bool data_path_contains_database_name = false;

    mutable std::mutex store_mutex;
    
    std::unique_ptr<TableColumnInfo> table_column_info;  // After create DeltaMergeStore object, it is deprecated.
    std::atomic<bool> store_inited;
    DM::DeltaMergeStorePtr _store;

    Strings pk_column_names; // TODO: remove it. Only use for debug from ch-client.
    bool is_common_handle;
    bool pk_is_handle;
    size_t rowkey_column_size;
    OrderedNameSet hidden_columns;

    // The table schema synced from TiDB
    TiDB::TableInfo tidb_table_info;

    // Used to allocate new column-id when this table is NOT synced from TiDB
    ColumnID max_column_id_used;

    std::atomic<bool> shutdown_called{false};

    std::atomic<UInt64> next_version = 1; //TODO: remove this!!!

    Context & global_context;
    Logger * log;
};


} // namespace DB
