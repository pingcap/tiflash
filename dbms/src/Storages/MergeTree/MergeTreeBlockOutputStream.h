#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <common/logger_useful.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_)
        : storage(storage_), log(&Logger::get("MergeTreeBlockOutputStream")) {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    Logger *log;
};

}
