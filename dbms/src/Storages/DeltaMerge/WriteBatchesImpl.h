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
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/WriteBatchWrapperImpl.h>

namespace DB
{
namespace DM
{
struct WriteBatches : private boost::noncopyable
{
    KeyspaceID keyspace_id;
    NamespaceID ns_id;
    PageStorageRunMode run_mode;
    WriteBatchWrapper log;
    WriteBatchWrapper data;
    WriteBatchWrapper meta;

    PageIdU64s written_log;
    PageIdU64s written_data;

    WriteBatchWrapper removed_log;
    WriteBatchWrapper removed_data;
    WriteBatchWrapper removed_meta;

    StoragePool & storage_pool;
    bool should_roll_back = false;

    WriteLimiterPtr write_limiter;

    explicit WriteBatches(StoragePool & storage_pool_, const WriteLimiterPtr & write_limiter_ = nullptr)
        : keyspace_id(storage_pool_.getKeyspaceID())
        , ns_id(storage_pool_.getTableID())
        , run_mode(storage_pool_.getPageStorageRunMode())
        , log(run_mode, keyspace_id, StorageType::Log, ns_id)
        , data(run_mode, keyspace_id, StorageType::Data, ns_id)
        , meta(run_mode, keyspace_id, StorageType::Meta, ns_id)
        , removed_log(run_mode, keyspace_id, StorageType::Log, ns_id)
        , removed_data(run_mode, keyspace_id, StorageType::Data, ns_id)
        , removed_meta(run_mode, keyspace_id, StorageType::Meta, ns_id)
        , storage_pool(storage_pool_)
        , write_limiter(write_limiter_)
    {}

    ~WriteBatches()
    {
        if constexpr (DM_RUN_CHECK)
        {
            auto check_empty = [&](const WriteBatchWrapper & wb, const String & name) {
                if (!wb.empty())
                {
                    StackTrace trace;
                    LOG_ERROR(
                        Logger::get(),
                        "!!!=========================Modifications in {} haven't persisted=========================!!! "
                        "Stack trace: {}",
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
        if constexpr (DM_RUN_CHECK)
        {
            auto check = [](const auto & wb, const String & what) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type == WriteBatchWriteType::DEL))
                        throw Exception("Unexpected deletes in " + what);
                }
                LOG_TRACE(Logger::get(), "Write into {} : {}", what, wb.toString());
            };
            switch (run_mode)
            {
            case PageStorageRunMode::UNI_PS:
            {
                check(log.getUniversalWriteBatch(), "log");
                check(data.getUniversalWriteBatch(), "data");
                break;
            }
            default:
            {
                check(log.getWriteBatch(), "log");
                check(data.getWriteBatch(), "data");
                break;
            }
            }
        }

        PageIdU64s log_write_pages, data_write_pages;
        switch (run_mode)
        {
        case PageStorageRunMode::UNI_PS:
        {
            for (const auto & w : log.getUniversalWriteBatch().getWrites())
                log_write_pages.push_back(UniversalPageIdFormat::getU64ID(w.page_id));
            for (const auto & w : data.getUniversalWriteBatch().getWrites())
                data_write_pages.push_back(UniversalPageIdFormat::getU64ID(w.page_id));
            break;
        }
        default:
        {
            for (const auto & w : log.getWriteBatch().getWrites())
                log_write_pages.push_back(w.page_id);
            for (const auto & w : data.getWriteBatch().getWrites())
                data_write_pages.push_back(w.page_id);
            break;
        }
        }

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
        WriteBatchWrapper log_wb(run_mode, keyspace_id, StorageType::Log, ns_id);
        for (auto p : written_log)
            log_wb.delPage(p);
        WriteBatchWrapper data_wb(run_mode, keyspace_id, StorageType::Data, ns_id);
        for (auto p : written_data)
            data_wb.delPage(p);

        if constexpr (DM_RUN_CHECK)
        {
            auto check = [](const auto & wb, const String & what) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatchWriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_TRACE(Logger::get(), "Rollback remove from {} : {}", what, wb.toString());
            };

            switch (run_mode)
            {
            case PageStorageRunMode::UNI_PS:
            {
                check(log_wb.getUniversalWriteBatch(), "log_wb");
                check(data_wb.getUniversalWriteBatch(), "data_wb");
                break;
            }
            default:
            {
                check(log_wb.getWriteBatch(), "log_wb");
                check(data_wb.getWriteBatch(), "data_wb");
                break;
            }
            }
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
            auto check = [](const auto & wb, const String & what) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatchWriteType::PUT))
                        throw Exception("Expected puts in " + what);
                }
                LOG_TRACE(Logger::get(), "Write into {} : {}", what, wb.toString());
            };
            switch (run_mode)
            {
            case PageStorageRunMode::UNI_PS:
            {
                check(meta.getUniversalWriteBatch(), "meta");
                break;
            }
            default:
            {
                check(meta.getWriteBatch(), "meta");
                break;
            }
            }
        }

        storage_pool.metaWriter()->write(std::move(meta), write_limiter);
        meta.clear();
    }

    void writeRemoves()
    {
        if constexpr (DM_RUN_CHECK)
        {
            auto check = [](const auto & wb, const String & what) {
                if (wb.empty())
                    return;
                for (const auto & w : wb.getWrites())
                {
                    if (unlikely(w.type != WriteBatchWriteType::DEL))
                        throw Exception("Expected deletes in " + what);
                }
                LOG_TRACE(Logger::get(), "Write into {} : {}", what, wb.toString());
            };

            switch (run_mode)
            {
            case PageStorageRunMode::UNI_PS:
            {
                check(removed_log.getUniversalWriteBatch(), "removed_log");
                check(removed_data.getUniversalWriteBatch(), "removed_data");
                check(removed_meta.getUniversalWriteBatch(), "removed_meta");
                break;
            }
            default:
            {
                check(removed_log.getWriteBatch(), "removed_log");
                check(removed_data.getWriteBatch(), "removed_data");
                check(removed_meta.getWriteBatch(), "removed_meta");
                break;
            }
            }
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
