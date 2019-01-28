#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>

namespace DB
{

Block TxnMergeTreeBlockOutputStream::getHeader() const
{
    return storage.getSampleBlock();
}

void TxnMergeTreeBlockOutputStream::write(const Block & block)
{
    storage.data.delayInsertIfNeeded();

    Row partition(1, Field(UInt64(partition_id)));
    Block block_copy = block;
    BlockWithPartition part_block(std::move(block_copy), std::move(partition));

    Stopwatch watch;

    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(part_block);
    storage.data.renameTempPartAndAdd(part, &storage.increment);

    PartLog::addNewPartToTheLog(storage.context, * part, watch.elapsed());

    storage.merge_task_handle->wake();
}

}
