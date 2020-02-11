#include <Common/TiFlashMetrics.h>
#include <Interpreters/PartLog.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

Block TxnMergeTreeBlockOutputStream::getHeader() const { return storage.getSampleBlock(); }

void TxnMergeTreeBlockOutputStream::write(Block && block)
{
    storage.data.delayInsertIfNeeded();

    Row partition(1, Field(UInt64(partition_id)));

    BlockWithPartition part_block(std::move(block), std::move(partition));

    Stopwatch watch;

    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(part_block);
    storage.data.renameTempPartAndAdd(part, &storage.increment);

    GET_METRIC(storage.context.getTiFlashMetrics(), tiflash_tmt_write_parts_count).Increment();
    GET_METRIC(storage.context.getTiFlashMetrics(), tiflash_tmt_write_parts_duration_seconds).Observe(watch.elapsedSeconds());
    PartLog::addNewPartToTheLog(storage.context, *part, watch.elapsed());

    storage.merge_task_handle->wake();
}

void TxnMergeTreeBlockOutputStream::write(const Block & block)
{
    Block block_copy = block;
    write(std::move(block_copy));
}

} // namespace DB
