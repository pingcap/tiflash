#pragma once

#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{

struct WriteBatches : private boost::noncopyable
{
    WriteBatch log;
    WriteBatch data;
    WriteBatch meta;

    PageIds writtenLog;
    PageIds writtenData;

    WriteBatch removed_log;
    WriteBatch removed_data;
    WriteBatch removed_meta;

    StoragePool & storage_pool;
    bool          should_roll_back = false;

    RateLimiterPtr rate_limiter;

    WriteBatches(StoragePool & storage_pool_, const RateLimiterPtr & rate_limiter_ = nullptr)
        : storage_pool(storage_pool_), rate_limiter(rate_limiter_)
    {
    }

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

        if (should_roll_back)
        {
            rollbackWrittenLogAndData();
        }
    }

    void setRollback() { should_roll_back = true; }

    void writeLogAndData()
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

        storage_pool.log().write(std::move(log), rate_limiter);
        storage_pool.data().write(std::move(data), rate_limiter);

        for (auto page_id : log_write_pages)
            writtenLog.push_back(page_id);
        for (auto page_id : data_write_pages)
            writtenData.push_back(page_id);

        log.clear();
        data.clear();
    }

    void rollbackWrittenLogAndData()
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

        storage_pool.log().write(std::move(log_wb), nullptr);
        storage_pool.data().write(std::move(data_wb), nullptr);

        writtenLog.clear();
        writtenData.clear();
    }

    void writeMeta()
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

        storage_pool.meta().write(std::move(meta), rate_limiter);
        meta.clear();
    }

    void writeRemoves()
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

        storage_pool.log().write(std::move(removed_log), rate_limiter);
        storage_pool.data().write(std::move(removed_data), rate_limiter);
        storage_pool.meta().write(std::move(removed_meta), rate_limiter);

        removed_log.clear();
        removed_data.clear();
        removed_meta.clear();
    }

    void writeAll()
    {
        writeLogAndData();
        writeMeta();
        writeRemoves();
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
