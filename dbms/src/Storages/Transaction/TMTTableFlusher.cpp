#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MutableSupport.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTTableFlusher.h>
#include <Storages/Transaction/SchemaSyncer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TMTTableFlusher::TMTTableFlusher(Context & context_, TableID table_id_, size_t flush_threshold_rows_) :
    context(context_), table_id(table_id_), flush_threshold_rows(flush_threshold_rows_),
    log(&Logger::get("TMTTableFlusher"))
{
    TMTContext & tmt = context.getTMTContext();
    tmt.getSchemaSyncer()->syncSchema(table_id, context);
}

void TMTTableFlusher::setFlushThresholdRows(size_t flush_threshold_rows)
{
    std::lock_guard<std::mutex> lock(mutex);
    this->flush_threshold_rows = flush_threshold_rows;
}

void TMTTableFlusher::onPutNotification(RegionPtr region, size_t put_rows)
{
    std::lock_guard<std::mutex> lock(mutex);

    RegionID region_id = region->id();
    TMTContext & tmt = context.getTMTContext();
    PartitionID partition_id = tmt.region_partition.getOrInsertRegion(table_id, region_id, context);

    auto it = partitions.find(partition_id);
    if (it == partitions.end())
        it = partitions.emplace(partition_id, TMTTableFlusher::Partition(partition_id, put_rows)).first;
    else
        it->second.onPut(put_rows);

    if (it->second.cached_rows >= flush_threshold_rows)
        asyncFlush(partition_id);
}

void TMTTableFlusher::tryAsyncFlush(size_t deadline_seconds)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto now = TMTTableFlusher::Clock::now();
    for (auto it = partitions.begin(); it != partitions.end(); ++it)
    {
        if (now - it->second.last_modified_time > std::chrono::seconds(deadline_seconds) && (it->second.cached_rows > 0))
            asyncFlush(it->second.partition_id);
    }
}

void TMTTableFlusher::asyncFlush(UInt64 partition_id)
{
    // TODO: async flush, flush queue, etc
    flush(partition_id);
    auto it = partitions.find(partition_id);
    if (it == partitions.end())
        throw Exception("Partition id(?) not exists", ErrorCodes::LOGICAL_ERROR);
    it->second.onFlush();
}

void TMTTableFlusher::flush(UInt64 partition_id)
{
    TMTContext & tmt = context.getTMTContext();
    tmt.getSchemaSyncer()->syncSchema(table_id, context);

    StoragePtr storage = tmt.storages.get(table_id);

    // TODO: handle if storage is nullptr
    // drop table and create another with same name, but the previous one will still flush
    if (storage == nullptr)
    {
        LOG_ERROR(log, "table " << table_id <<" flush partition " << partition_id << " , but storage is not found");
        return;
    }

    StorageMergeTree * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());

    const auto & table_info = merge_tree->getTableInfo();
    const auto & columns = merge_tree->getColumns();
    // TODO: confirm names is right
    Names names = columns.getNamesOfPhysical();

    BlockInputStreamPtr input = tmt.region_partition.getBlockInputStreamByPartition(
        table_id, partition_id, table_info, columns, names, context, true, false, false, 0);
    if (!input)
        return;

    TxnMergeTreeBlockOutputStream output(*merge_tree, partition_id);
    input->readPrefix();
    output.writePrefix();
    while (true)
    {
        Block block = input->read();
        if (!block || block.rows() == 0)
            break;
        output.write(block);
    }
    output.writeSuffix();
    input->readSuffix();
}

TMTTableFlushers::TMTTableFlushers(Context & context_, size_t deadline_seconds_,
    size_t flush_threshold_rows_) : context(context_), deadline_seconds(deadline_seconds_),
    flush_threshold_rows(flush_threshold_rows_), interval_thread_stopping(false)
{
    interval_thread = std::thread([&]
    {
        while (!interval_thread_stopping)
        {
            // TODO: since we don't have a real async flush implement, we clone flushers to avoid long time locking, for now
            FlusherMap cloned;
            {
                std::lock_guard<std::mutex> lock(mutex);
                cloned = flushers;
            }

            for (auto it = cloned.begin(); it != cloned.end(); ++it)
                it->second->tryAsyncFlush(deadline_seconds);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });
}

TMTTableFlushers::~TMTTableFlushers()
{
    interval_thread_stopping = true;

    if (interval_thread.joinable())
        interval_thread.join();
}

void TMTTableFlushers::setFlushThresholdRows(size_t flush_threshold_rows)
{
    std::lock_guard<std::mutex> lock(mutex);
    this->flush_threshold_rows = flush_threshold_rows;

    for (auto it = flushers.begin(); it != flushers.end(); ++it)
        it->second->setFlushThresholdRows(flush_threshold_rows);
}

void TMTTableFlushers::setDeadlineSeconds(size_t deadline_seconds)
{
    std::lock_guard<std::mutex> lock(mutex);
    this->deadline_seconds = deadline_seconds;
}

TMTTableFlusherPtr TMTTableFlushers::getOrCreate(TableID table_id)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = flushers.find(table_id);
    if (it != flushers.end())
        return it->second;

    TMTTableFlusherPtr flusher = std::make_shared<TMTTableFlusher>(context,
        table_id, flush_threshold_rows);
    flushers.emplace(table_id, flusher);
    return flusher;
}

void TMTTableFlushers::onPutTryFlush(RegionPtr region)
{
    auto region_id = region->id();
    TMTContext & tmt = context.getTMTContext();

    // TODO: Revise the way of "finding tables in a given region".
    // Now WAR using data CF, which may miss the short value in write CF, so make the value long!!!
    //
    // const Region::KVMap & data_cf = region->data_cf_();
    // std::unordered_set<TableID> table_ids;
    // for (const auto & kv : data_cf)
    // {
    //     const auto & key = kv.first;
    //     table_ids.emplace(RecordKVFormat::getTableId(key));
    // }

    // TODO: Use this to instead codes above, and make it faster
    std::unordered_set<TableID> table_ids;
    {
        auto scanner = region->createCommittedScanRemover(InvalidTableID);
        while (true)
        {
            TableID table_id = scanner->hasNext();
            if (table_id == InvalidTableID)
                break;
            table_ids.emplace(table_id);
            scanner->next();
        }
    }

    for (const auto table_id : table_ids)
    {
        tmt.region_partition.getOrInsertRegion(table_id, region_id, context);

        auto flusher = getOrCreate(table_id);
        // This is not strickly the right number of newly added rows, but in most cases it works
        flusher->onPutNotification(region, region->getNewlyAddedRows());
        region->resetNewlyAddedRows();
    }

    tmt.region_partition.updateRegionRange(region);

    // TODO: This doesn't work unless we settle down the region/table mapping problem.
    // auto handler = [&] (TableID table_id)
    // {
    //     auto flusher = getOrCreate(table_id);
    //     // This is not strickly the right number of newly added rows, but in most cases it works
    //     flusher->onPutNotification(region, region->getNewlyAddedRows());
    //     region->resetNewlyAddedRows();
    // };
    // tmt.region_partition.traverseTablesOfRegion(region_id, handler);
}

void TMTTableFlushers::dropRegionsInTable(TableID /*table_id*/)
{
    // TODO: impl
}

}
