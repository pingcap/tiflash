#pragma once

#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/TiDB.h>

#include <ext/shared_ptr_helper.h>
#include <mutex>
#include <utility>


namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageDebugging : public ext::shared_ptr_helper<StorageDebugging>, public IManageableStorage
{
    friend class DebuggingBlockInputStream;
    friend class DebuggingBlockOutputStream;

public:
    enum class Mode
    {
        Normal = 0,
        RejectFirstWrite = 1,
    };

public:
    std::string getName() const override;
    std::string getTableName() const override { return table_name; }

    size_t getSize() const { return data.size(); }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void drop() override;
    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name,
        const String & new_display_table_name) override;


    ::TiDB::StorageEngine engineType() const override { return ::TiDB::StorageEngine::DEBUGGING_MEMORY; }

    String getDatabaseName() const override { return database_name; }

    void setTableInfo(const TiDB::TableInfo & /*table_info_*/) override;

    const TiDB::TableInfo & getTableInfo() const override;

    void alterFromTiDB(const AlterCommands & /*commands*/,
        const String & /*database_name*/,
        const TiDB::TableInfo & /*table_info*/,
        const SchemaNameMapper & /*name_mapper*/,
        const Context & /*context*/) override;

    SortDescription getPrimarySortDescription() const override;

    void startup() override;

    void shutdown() override;

    void removeFromTMTContext() override;

private:
    String database_name;
    String table_name;
    Mode mode;
    TiDB::TableInfo tidb_table_info;

    std::atomic<UInt64> num_write_called = 0;

    std::atomic<bool> shutdown_called{false};

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    const Context & global_context;

    std::mutex mutex;

protected:
    StorageDebugging(String database_name_,
        String table_name_,
        const ColumnsDescription & columns_description_,
        std::optional<std::reference_wrapper<const TiDB::TableInfo>>
            table_info,
        Timestamp tombstone,
        const Context & context_,
        Mode mode_ = Mode::Normal);
};

} // namespace DB
