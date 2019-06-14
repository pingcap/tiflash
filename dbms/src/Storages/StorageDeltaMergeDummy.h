#pragma once

#include <tuple>
#include <ext/shared_ptr_helper.h>

#include <Poco/File.h>

#include <Core/Defines.h>
#include <Core/SortDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DummyDefines.h>
#include <Storages/IStorage.h>

#include <common/logger_useful.h>

namespace DB
{
class StorageDeltaMergeDummy : public ext::shared_ptr_helper<StorageDeltaMergeDummy>, public IStorage
{
public:
    bool supportsModification() const override
    {
        return true;
    }

    String getName() const override
    {
        return "DeltaMerge";
    }
    String getTableName() const override
    {
        return name;
    }

    bool checkData() const override
    {
        return stable_storage->checkData();
    };

    String getDataPath() const override
    {
        return stable_storage->getDataPath();
    }

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
    {
        name = new_table_name;
        stable_storage->rename(new_path_to_db, new_database_name, new_table_name);
    }

    BlockInputStreams read(const Names & column_names,
                           const SelectQueryInfo & query_info,
                           const Context & context,
                           QueryProcessingStage::Enum & processed_stage,
                           size_t max_block_size,
                           unsigned num_streams) override;

    BlockInputStreams read(const Names & column_names,
                           QueryProcessingStage::Enum & processed_stage,
                           const size_t max_block_size,
                           const size_t max_read_buffer_size);

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    UInt64 stable_size();

    /// entries, inserts, deletes, modifies
    std::tuple<UInt64, UInt64, UInt64, UInt64> delta_status();

    void flushDelta();

    BlockInputStreamPtr status();

    void initDelta();

    void check();

    StorageDeltaMergeDummy(const std::string & path_,
                      const std::string & name_,
                      const ColumnsDescription & columns_,
                      const ASTPtr & primary_expr_ast_,
                      bool attach,
                      size_t max_compress_block_size_);

private:
    String path;
    String name;
    size_t max_compress_block_size;

    StoragePtr stable_storage;
    SortDescription primary_sort_descr;

    MyDeltaTreePtr delta_tree;
    MyValueSpacePtr insert_value_space;
    MyValueSpacePtr modify_value_space;

    Logger * log;
};

} // namespace DB