#pragma once

#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** Implements `events` system table, which allows you to obtain information for profiling.
  */
class StorageSystemEvents : public ext::SharedPtrHelper<StorageSystemEvents>
    , public IStorage
{
public:
    std::string getName() const override { return "SystemEvents"; }
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
    StorageSystemEvents(const std::string & name_);
};

} // namespace DB
