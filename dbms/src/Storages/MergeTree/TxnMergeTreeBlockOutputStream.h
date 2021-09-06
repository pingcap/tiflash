#pragma once

#include <Core/Row.h>
#include <DataStreams/IBlockOutputStream.h>
#include <common/logger_useful.h>

namespace DB
{
class Block;
class StorageMergeTree;

class TxnMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    TxnMergeTreeBlockOutputStream(StorageMergeTree & storage_, UInt64 partition_id_ = 0)
        : storage(storage_)
        , log(&Poco::Logger::get("TxnMergeTreeBlockOutputStream"))
        , partition_id(partition_id_)
    {}

    Block getHeader() const override;
    void write(const Block & block) override;
    void write(Block && block);

private:
    StorageMergeTree & storage;
    Poco::Logger * log;
    size_t partition_id;
};

} // namespace DB
