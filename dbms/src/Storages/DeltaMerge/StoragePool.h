#pragma once

#include <atomic>
#include <chrono>

#include <Storages/Page/PageStorage.h>

namespace DB
{
namespace DM
{

static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class StoragePool : private boost::noncopyable
{
public:
    using Clock     = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Duration  = Clock::duration;
    using Seconds   = std::chrono::seconds;

    StoragePool(const String & name, const String & path);

    PageId maxLogPageId() { return max_log_page_id; }
    PageId maxDataPageId() { return max_data_page_id; }
    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newDataPageId() { return ++max_data_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

    PageStorage & log() { return log_storage; }
    PageStorage & data() { return data_storage; }
    PageStorage & meta() { return meta_storage; }

    bool gc(const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStorage log_storage;
    PageStorage data_storage;
    PageStorage meta_storage;

    std::atomic<PageId> max_log_page_id;
    std::atomic<PageId> max_data_page_id;
    std::atomic<PageId> max_meta_page_id;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;
};
using GenPageId = std::function<PageId()>;

const static PageStorage::SnapshotPtr EMPTY_PS_SNAP_PTR = {};

struct StorageSnapshot
{
    StorageSnapshot(StoragePool & storage, bool snapshot_read = true)
        : log_reader(storage.log(), snapshot_read ? storage.log().getSnapshot() : EMPTY_PS_SNAP_PTR),
          data_reader(storage.data(), snapshot_read ? storage.data().getSnapshot() : EMPTY_PS_SNAP_PTR),
          meta_reader(storage.meta(), snapshot_read ? storage.meta().getSnapshot() : EMPTY_PS_SNAP_PTR)
    {
    }

    PageReader log_reader;
    PageReader data_reader;
    PageReader meta_reader;
};
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

struct WriteBatches
{
    WriteBatch log;
    WriteBatch data;
    WriteBatch meta;

    PageIds writtenLog;
    PageIds writtenData;

    WriteBatch removed_log;
    WriteBatch removed_data;
    WriteBatch removed_meta;

    void writeLogAndData(StoragePool & storage_pool)
    {
        storage_pool.log().write(log);
        storage_pool.data().write(data);

        for (auto & w : log.getWrites())
            writtenLog.push_back(w.page_id);
        for (auto & w : data.getWrites())
            writtenData.push_back(w.page_id);

        log.clear();
        data.clear();
    }

    void rollbackWrittenLogAndData(StoragePool & storage_pool)
    {
        WriteBatch log_wb;
        for (auto p : writtenLog)
            log_wb.delPage(p);
        WriteBatch data_wb;
        for (auto p : writtenData)
            data_wb.delPage(p);

        storage_pool.log().write(log_wb);
        storage_pool.data().write(data_wb);
    }

    void writeMeta(StoragePool & storage_pool)
    {
        storage_pool.meta().write(meta);
        meta.clear();
    }

    void writeRemoves(StoragePool & storage_pool)
    {
        storage_pool.log().write(removed_log);
        storage_pool.data().write(removed_data);
        storage_pool.meta().write(removed_meta);

        removed_log.clear();
        removed_data.clear();
        removed_meta.clear();
    }

    void writeAll(StoragePool & storage_pool)
    {
        writeLogAndData(storage_pool);
        writeMeta(storage_pool);
        writeRemoves(storage_pool);
    }
};

} // namespace DM
} // namespace DB
