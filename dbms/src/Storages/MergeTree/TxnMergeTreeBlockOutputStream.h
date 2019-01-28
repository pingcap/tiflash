#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/Row.h>
#include <common/logger_useful.h>

namespace DB
{

class Block;
class StorageMergeTree;

class TxnMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    TxnMergeTreeBlockOutputStream(StorageMergeTree & storage_, UInt64 partition_id_) :
        storage(storage_), log(&Logger::get("TxnMergeTreeBlockOutputStream")), partition_id(partition_id_)
    {
    }

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    Logger *log;
    size_t partition_id;
};

}
