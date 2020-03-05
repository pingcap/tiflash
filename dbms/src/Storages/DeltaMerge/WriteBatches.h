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

    ~WriteBatches()
    {
        if constexpr (DM_RUN_CHECK)
        {
            Logger * logger      = &Logger::get("WriteBatches");
            auto     check_empty = [&](const WriteBatch & wb, const String & name) {
                if (!wb.empty())
                {
                    StackTrace trace;
                    LOG_ERROR(logger,
                              "!!!=========================Modifications in " + name
                                      + " haven't persisted=========================!!! Stack trace: "
                                  << trace.toString());
                }
            };
            check_empty(log, "log");
            check_empty(data, "data");
            check_empty(meta, "meta");
            check_empty(removed_log, "removed_log");
            check_empty(removed_data, "removed_data");
            check_empty(removed_meta, "removed_meta");
        }
    }

    void writeLogAndData(StoragePool & storage_pool)
    {
        PageIds log_write_pages, data_write_pages;

        if constexpr (DM_RUN_CHECK)
        {
            Logger * logger = &Logger::get("WriteBatches");
            auto     check  = [](const WriteBatch & wb, const String & what, Logger * logger) {
                if (wb.empty())
                    return;
                for (auto & w : wb.getWrites())
                {
                    if (unlikely(w.type == WriteBatch::WriteType::DEL))
                        throw Exception("Unexpected deletes in " + what);
                }
                LOG_TRACE(logger, "Write into " + what + " : " + wb.toString());
            };

            check(log, "log", logger);
            check(data, "data", logger);
        }

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

        if constexpr (DM_RUN_CHECK)
        {
            Logger * logger = &Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Logger * logger) {
                if (wb.empty())
                    return;
                for (auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_TRACE(logger, "Rollback remove from " + what + " : " + wb.toString());
            };

            check(log_wb, "log_wb", logger);
            check(data_wb, "data_wb", logger);
        }

        storage_pool.log().write(std::move(log_wb));
        storage_pool.data().write(std::move(data_wb));
    }

    void writeMeta(StoragePool & storage_pool)
    {
        if constexpr (DM_RUN_CHECK)
        {
            Logger * logger = &Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Logger * logger) {
                if (wb.empty())
                    return;
                for (auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::PUT))
                        throw Exception("Expected puts in " + what);
                }
                LOG_TRACE(logger, "Write into " + what + " : " + wb.toString());
            };

            check(meta, "meta", logger);
        }

        storage_pool.meta().write(std::move(meta));
        meta.clear();
    }

    void writeRemoves(StoragePool & storage_pool)
    {
        if constexpr (DM_RUN_CHECK)
        {
            Logger * logger = &Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Logger * logger) {
                if (wb.empty())
                    return;
                for (auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_TRACE(logger, "Write into " + what + " : " + wb.toString());
            };

            check(removed_log, "removed_log", logger);
            check(removed_data, "removed_data", logger);
            check(removed_meta, "removed_meta", logger);
        }

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

    void clear()
    {
        log.clear();
        data.clear();
        meta.clear();

        writtenLog.clear();
        writtenData.clear();

        removed_log.clear();
        removed_data.clear();
        removed_meta.clear();
    }
};
} // namespace DM
} // namespace DB
