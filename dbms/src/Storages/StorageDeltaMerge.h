#pragma once

#include <Core/Defines.h>
#include <Core/SortDescription.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/TiDB.h>
#include <common/logger_useful.h>

#include <ext/shared_ptr_helper.h>
#include <tuple>

namespace DB
{

namespace DM
{
class DeltaMergeStore;
using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;
} // namespace DM

class StorageDeltaMerge : public ext::shared_ptr_helper<StorageDeltaMerge>, public IManageableStorage
{
public:
    ~StorageDeltaMerge() override;

    bool supportsModification() const override { return true; }

    String getName() const override { return "DeltaMerge"; }
    String getTableName() const override { return table_name; }

    void drop() override;

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void flushCache(const Context & context, HandleID start, HandleID end) override
    {
        flushCache(context, DM::toDMHandleRange(start, end));
    }

    void flushCache(const Context & context, const DM::HandleRange & range_to_flush);

    void deleteRange(const DM::HandleRange & range_to_delete, const Settings & settings);

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) override;

    String getDatabaseName() const override { return db_name; }

    void alter(const AlterCommands & commands, const String & database_name, const String & table_name, const Context & context) override;

    ::TiDB::StorageEngine engineType() const override { return ::TiDB::StorageEngine::DT; }

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    void alterFromTiDB(
        const AlterCommands & commands, const TiDB::TableInfo & table_info, const String & database_name, const Context & context) override;

    void setTableInfo(const TiDB::TableInfo & table_info_) override { tidb_table_info = table_info_; }

    const TiDB::TableInfo & getTableInfo() const override { return tidb_table_info; }

    void startup() override;

    void shutdown() override;

    SortDescription getPrimarySortDescription() const override;

    const OrderedNameSet & getHiddenColumnsImpl() const override { return hidden_columns; }

    BlockInputStreamPtr status() override;

    void checkStatus(const Context & context) override;
    void deleteRows(const Context &, size_t rows) override;

protected:
    StorageDeltaMerge(const String & path_,
        const String & db_name_,
        const String & name_,
        const DM::OptionTableInfoConstRef table_info_,
        const ColumnsDescription & columns_,
        const ASTPtr & primary_expr_ast_,
        Context & global_context_);

    Block buildInsertBlock(bool is_import, bool is_delete, const Block & block);

private:
    void alterImpl(const AlterCommands & commands,
        const String & database_name,
        const String & table_name,
        const DB::DM::OptionTableInfoConstRef table_info_,
        const Context & context);

    DataTypePtr getPKTypeImpl() const override;

private:
    using ColumnIdMap = std::unordered_map<String, size_t>;

    String path;
    String db_name;
    String table_name;

    DM::DeltaMergeStorePtr store;

    Strings pk_column_names;
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
