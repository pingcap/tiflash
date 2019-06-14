#pragma once

#include <ext/shared_ptr_helper.h>
#include <tuple>

#include <Poco/File.h>

#include <Core/Defines.h>
#include <Core/SortDescription.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/IStorage.h>

#include <common/logger_useful.h>

namespace DB
{
class StorageDeltaMerge : public ext::shared_ptr_helper<StorageDeltaMerge>, public IStorage
{
public:
    bool supportsModification() const override { return true; }

    String getName() const override { return "DeltaMerge"; }
    String getTableName() const override { return name; }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

protected:
    StorageDeltaMerge(const std::string & path_,
        const std::string & name_,
        const ColumnsDescription & columns_,
        const ASTPtr & primary_expr_ast_,
        Context & global_context_);

    Block buildInsertBlock(const Block & block);

private:
    using ColumnIdMap = std::unordered_map<String, size_t>;

    String path;
    String name;

    DeltaMergeStorePtr store;

    ColumnDefines table_column_defines;
    ColumnDefine handle_column_define;
    Strings pk_column_names;

    std::atomic<UInt64> next_version = 1; //TODO: remove this!!!

    Context & global_context;
    Block header;

    Logger * log;
};

} // namespace DB