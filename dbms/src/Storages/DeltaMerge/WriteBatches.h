// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{
struct WriteBatches : private boost::noncopyable
{
    NamespaceId ns_id;
    WriteBatch log;
    WriteBatch data;
    WriteBatch meta;

    PageIds written_log;
    PageIds written_data;

    WriteBatch removed_log;
    WriteBatch removed_data;
    WriteBatch removed_meta;

    StoragePool & storage_pool;
    bool should_roll_back = false;

    WriteLimiterPtr write_limiter;

    WriteBatches(StoragePool & storage_pool_, const WriteLimiterPtr & write_limiter_ = nullptr)
        : ns_id(storage_pool_.getNamespaceId())
        , log(ns_id)
        , data(ns_id)
        , meta(ns_id)
        , removed_log(ns_id)
        , removed_data(ns_id)
        , removed_meta(ns_id)
        , storage_pool(storage_pool_)
        , write_limiter(write_limiter_)
    {
    }

    ~WriteBatches()
    {
        if constexpr (DM_RUN_CHECK)
        {
            Poco::Logger * logger = &Poco::Logger::get("WriteBatches");
            auto check_empty = [&](const WriteBatch & wb, const String & name) {
                if (!wb.empty())
                {
                    StackTrace trace;
                    LOG_FMT_ERROR(logger,
                                  "!!!=========================Modifications in {} haven't persisted=========================!!! Stack trace: {}",
                                  name,
                                  trace.toString());
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
            Poco::Logger * logger = &Poco::Logger::get("WriteBatches");
            auto check = [](const WriteBatch & wb, const String & what, Poco::Logger * logger) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type == WriteBatch::WriteType::DEL))
                        throw Exception("Unexpected deletes in " + what);
                }
                LOG_FMT_TRACE(logger, "Write into {} : {}", what, wb.toString());
            };

            check(log, "log", logger);
            check(data, "data", logger);
        }

        for (auto & w : log.getWrites())
            log_write_pages.push_back(w.page_id);
        for (auto & w : data.getWrites())
            data_write_pages.push_back(w.page_id);

        storage_pool.logWriter()->write(std::move(log), write_limiter);
        storage_pool.dataWriter()->write(std::move(data), write_limiter);

        for (auto page_id : log_write_pages)
            written_log.push_back(page_id);
        for (auto page_id : data_write_pages)
            written_data.push_back(page_id);

        log.clear();
        data.clear();
    }

    void rollbackWrittenLogAndData()
    {
        WriteBatch log_wb(ns_id);
        for (auto p : written_log)
            log_wb.delPage(p);
        WriteBatch data_wb(ns_id);
        for (auto p : written_data)
            data_wb.delPage(p);

        if constexpr (DM_RUN_CHECK)
        {
            Poco::Logger * logger = &Poco::Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Poco::Logger * logger) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_FMT_TRACE(logger, "Rollback remove from {} : {}", what, wb.toString());
            };

            check(log_wb, "log_wb", logger);
            check(data_wb, "data_wb", logger);
        }

        storage_pool.logWriter()->write(std::move(log_wb), write_limiter);
        storage_pool.dataWriter()->write(std::move(data_wb), write_limiter);

        written_log.clear();
        written_data.clear();
    }

    void writeMeta()
    {
        if constexpr (DM_RUN_CHECK)
        {
            Poco::Logger * logger = &Poco::Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Poco::Logger * logger) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::PUT))
                        throw Exception("Expected puts in " + what);
                }
                LOG_FMT_TRACE(logger, "Write into {} : {}", what, wb.toString());
            };

            check(meta, "meta", logger);
        }

        storage_pool.metaWriter()->write(std::move(meta), write_limiter);
        meta.clear();
    }

    void writeRemoves()
    {
        if constexpr (DM_RUN_CHECK)
        {
            Poco::Logger * logger = &Poco::Logger::get("WriteBatches");

            auto check = [](const WriteBatch & wb, const String & what, Poco::Logger * logger) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatch::WriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_FMT_TRACE(logger, "Write into {} : {}", what, wb.toString());
            };

            check(removed_log, "removed_log", logger);
            check(removed_data, "removed_data", logger);
            check(removed_meta, "removed_meta", logger);
        }

        storage_pool.logWriter()->write(std::move(removed_log), write_limiter);
        storage_pool.dataWriter()->write(std::move(removed_data), write_limiter);
        storage_pool.metaWriter()->write(std::move(removed_meta), write_limiter);

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

        written_log.clear();
        written_data.clear();

        removed_log.clear();
        removed_data.clear();
        removed_meta.clear();
    }
};
} // namespace DM
} // namespace DB
