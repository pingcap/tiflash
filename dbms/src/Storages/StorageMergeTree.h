#pragma once

#include <Common/SimpleIncrement.h>
#include <Core/TMTPKType.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

#include <ext/shared_ptr_helper.h>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree : public ext::shared_ptr_helper<StorageMergeTree>, public IManageableStorage
{
    friend class MergeTreeBlockOutputStream;
    friend class TxnMergeTreeBlockOutputStream;

    using TableInfo = TiDB::TableInfo;

public:
    void startup() override;
    void shutdown() override;
    void removeFromTMTContext() override;
    ~StorageMergeTree() override;

    std::string getName() const override { return data.merging_params.getModeName() + "MergeTree"; }

    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    bool supportsSampling() const override { return data.supportsSampling(); }
    bool supportsPrewhere() const override { return data.supportsPrewhere(); }
    bool supportsFinal() const override { return data.supportsFinal(); }
    bool supportsIndexForIn() const override { return true; }
    bool supportsModification() const override
    {
        return data.merging_params.mode == MergeTreeData::MergingParams::Mode::Mutable
            || data.merging_params.mode == MergeTreeData::MergingParams::Mode::Txn;
    }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override { return data.mayBenefitFromIndexForIn(left_in_operand); }

    const ColumnsDescription & getColumns() const override { return data.getColumns(); }
    void setColumns(ColumnsDescription columns_) override { return data.setColumns(std::move(columns_)); }

    NameAndTypePair getColumn(const String & column_name) const override { return data.getColumn(column_name); }

    bool hasColumn(const String & column_name) const override { return data.hasColumn(column_name); }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    UInt64 getVersionSeed(const Settings & settings) const;

    /** Perform the next step in combining the parts.
      */
    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;
    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;
    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    void drop() override;

    void truncate(const ASTPtr & /*query*/, const Context & /*context*/) override;

    void rename(const String & new_path_to_db,
        const String & new_database_name,
        const String & new_table_name,
        const String & new_display_table_name) override;

    void modifyASTStorage(ASTStorage * storage_ast, const TiDB::TableInfo & table_info) override;

    void alter(const TableLockHolder &, const AlterCommands & params, const String & database_name, const String & table_name,
        const Context & context) override;

    void alterFromTiDB(const TableLockHolder &, const AlterCommands & params, const String & database_name,
        const TiDB::TableInfo & table_info, const SchemaNameMapper & name_mapper, const Context & context) override;

    ::TiDB::StorageEngine engineType() const override
    {
        if (data.merging_params.mode == MergeTreeData::MergingParams::Txn)
            return ::TiDB::StorageEngine::TMT;
        else
            return ::TiDB::StorageEngine::UNSUPPORTED_ENGINES;
    }

    bool checkTableCanBeDropped() const override;

    const TableInfo & getTableInfo() const override;
    void setTableInfo(const TableInfo & table_info_) override;

    MergeTreeData & getData() { return data; }
    const MergeTreeData & getData() const { return data; }

    String getDataPath() const override { return full_path; }

    SortDescription getPrimarySortDescription() const override { return data.getPrimarySortDescription(); }

private:
    String path;
    const bool data_path_contains_database_name = false;
    String database_name;
    String table_name;
    String full_path;

    Context & context;
    BackgroundProcessingPool & background_pool;

    MergeTreeData data;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMerger merger;

    /// For block numbers.
    SimpleIncrement increment{0};

    /// For clearOldParts, clearOldTemporaryDirectories.
    AtomicStopwatch time_after_previous_cleanup;

    MergeTreeData::DataParts currently_merging;
    std::mutex currently_merging_mutex;

    Logger * log;

    std::atomic<bool> shutdown_called{false};

    BackgroundProcessingPool::TaskHandle merge_task_handle;

    const OrderedNameSet & getHiddenColumnsImpl() const override;

    friend struct CurrentlyMergingPartsTagger;

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(size_t aio_threshold, bool aggressive, const String & partition_id, bool final, bool deduplicate,
        String * out_disable_reason = nullptr);

    bool mergeTask();

    void alterInternal(const AlterCommands & params, const String & database_name, const String & table_name,
        const std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info, const Context & context);

    DataTypePtr getPKTypeImpl() const override;

protected:
    /** Attach the table with the appropriate name, along the appropriate path (with  / at the end),
      *  (correctness of names and paths are not checked)
      *  consisting of the specified columns.
      *
      * primary_expr_ast      - expression for sorting;
      * date_column_name      - if not empty, the name of the column with the date used for partitioning by month;
          otherwise, partition_expr_ast is used as the partitioning expression;
      */
    StorageMergeTree(const String & path_,
        const String & db_engine,
        const String & database_name_,
        const String & table_name_,
        const ColumnsDescription & columns_,
        bool attach,
        Context & context_,
        const TableInfo & table_info_,
        const ASTPtr & primary_expr_ast_,
        const ASTPtr & secondary_sorting_expr_list_,
        const String & date_column_name,
        const ASTPtr & partition_expr_ast_,
        const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
        const MergeTreeData::MergingParams & merging_params_,
        const MergeTreeSettings & settings_,
        bool has_force_restore_data_flag,
        Timestamp tombstone);
};

} // namespace DB
