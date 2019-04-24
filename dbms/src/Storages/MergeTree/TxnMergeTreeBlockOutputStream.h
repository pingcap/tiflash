#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/Row.h>
#include <common/logger_useful.h>

namespace DB
{

class Block;
class StorageMergeTree;

// TODO REVIEW: this stream op seems too empty, we may move more txn-related handling from writer to this op
class TxnMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    TxnMergeTreeBlockOutputStream(StorageMergeTree & storage_, UInt64 partition_id_ = 0) :
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
