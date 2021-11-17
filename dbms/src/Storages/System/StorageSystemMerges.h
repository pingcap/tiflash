#pragma once

#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


class StorageSystemMerges : public ext::SharedPtrHelper<StorageSystemMerges>
    , public IStorage
{
public:
    std::string getName() const override { return "SystemMerges"; }
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
    StorageSystemMerges(const std::string & name);
};

} // namespace DB
