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
        PageIds log_write_pages, data_write_pages;
        for (auto & w : log.getWrites())
            log_write_pages.push_back(w.page_id);
        for (auto & w : data.getWrites())
            data_write_pages.push_back(w.page_id);

        storage_pool.log().write(std::move(log));
        storage_pool.data().write(std::move(data));

        for (auto page_id : log_write_pages)
            writtenLog.push_back(page_id);
        for (auto page_id : data_write_pages)
            writtenData.push_back(page_id);

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

        storage_pool.log().write(std::move(log_wb));
        storage_pool.data().write(std::move(data_wb));
    }

    void writeMeta(StoragePool & storage_pool)
    {
        storage_pool.meta().write(std::move(meta));
        meta.clear();
    }

    void writeRemoves(StoragePool & storage_pool)
    {
        storage_pool.log().write(std::move(removed_log));
        storage_pool.data().write(std::move(removed_data));
        storage_pool.meta().write(std::move(removed_meta));

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
