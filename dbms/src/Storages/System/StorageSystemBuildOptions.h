#pragma once

#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "build_options" with many params used for clickhouse building
  */
class StorageSystemBuildOptions : public ext::SharedPtrHelper<StorageSystemBuildOptions>
    , public IStorage
{
public:
    std::string getName() const override { return "SystemBuildOptions"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemBuildOptions(const std::string & name_);
};

} // namespace DB
