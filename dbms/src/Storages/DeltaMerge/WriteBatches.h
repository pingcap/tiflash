#pragma once

#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{

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