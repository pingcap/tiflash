#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


class StorageSystemDTSegments : public ext::shared_ptr_helper<StorageSystemDTSegments>, public IStorage
{
public:
    std::string getName() const override { return "SystemDTSegments"; }
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
    StorageSystemDTSegments(const std::string & name_);
};

}
