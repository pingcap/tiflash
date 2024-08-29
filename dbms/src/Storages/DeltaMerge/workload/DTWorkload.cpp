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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/workload/DTWorkload.h>
#include <Storages/DeltaMerge/workload/DataGenerator.h>
#include <Storages/DeltaMerge/workload/Handle.h>
#include <Storages/DeltaMerge/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/workload/Limiter.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <Storages/DeltaMerge/workload/ReadColumnsGenerator.h>
#include <Storages/DeltaMerge/workload/TableGenerator.h>
#include <Storages/DeltaMerge/workload/TimestampGenerator.h>
#include <Storages/DeltaMerge/workload/Utils.h>
#include <TestUtils/TiFlashTestEnv.h>

namespace DB::DM::tests
{

DTWorkload::DTWorkload(
    const WorkloadOptions & opts_,
    std::shared_ptr<SharedHandleTable> handle_table_,
    const TableInfo & table_info_,
    ContextPtr context_)
    : log(&Poco::Logger::get("DTWorkload"))
    , context(context_)
    , opts(std::make_unique<WorkloadOptions>(opts_))
    , table_info(std::make_unique<TableInfo>(table_info_))
    , handle_table(handle_table_)
    , writing_threads(opts_.write_thread_count)
    , stat(opts_.write_thread_count, opts_.read_thread_count)
{
    auto v = table_info->toStrings();
    for (const auto & s : v)
    {
        LOG_INFO(log, "TableInfo: {}", s);
    }

    key_gen = KeyGenerator::create(opts_);
    ts_gen = std::make_unique<TimestampGenerator>();
    // max page id is only updated at restart, so we need recreate page v3 before recreate table
    context->initializeGlobalPageIdAllocator();
    context->initializeGlobalStoragePoolIfNeed(context->getPathPool());
    Stopwatch sw;
    store = DeltaMergeStore::createUnique(
        *context,
        true,
        table_info->db_name,
        table_info->table_name,
        NullspaceID,
        table_info->table_id,
        true,
        *table_info->columns,
        table_info->handle,
        table_info->is_common_handle,
        table_info->rowkey_column_indexes.size(),
        DeltaMergeStore::Settings());
    stat.init_ms = sw.elapsedMilliseconds();
    LOG_INFO(log, "Init store {} ms", stat.init_ms);

    if (opts_.verification)
    {
        handle_lock = std::make_unique<HandleLock>();
        verifyHandle(std::numeric_limits<uint64_t>::max()); // uint64_max means recovery.
    }
}

DTWorkload::~DTWorkload()
{
    store->flushCache(*context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
}

// Update block's key, timestamp, and reuse other fields.
uint64_t DTWorkload::updateBlock(Block & block, uint64_t key)
{
    {
        auto & cd = (*table_info->columns)[0];
        if (cd.type->getTypeId() != TypeIndex::Int64)
        {
            LOG_ERROR(
                log,
                "Column type not match: {} but {} is required",
                magic_enum::enum_name(cd.type->getTypeId()),
                magic_enum::enum_name(TypeIndex::Int64));
            throw std::invalid_argument("Column type not match");
        }

        Field f = static_cast<int64_t>(key);
        auto mut_col = cd.type->createColumn();
        mut_col->insert(f);

        ColumnWithTypeAndName col({}, cd.type, cd.name, cd.id);
        col.column = std::move(mut_col);

        block.erase(cd.name);
        block.insert(col);
    }

    auto ts = ts_gen->get();
    {
        auto & cd = (*table_info->columns)[1];
        if (cd.id != VERSION_COLUMN_ID)
        {
            LOG_ERROR(log, "Column id not match: {} but {} is required", cd.id, VERSION_COLUMN_ID);
            throw std::invalid_argument("Column id not match");
        }

        Field f = ts;
        auto mut_col = cd.type->createColumn();
        mut_col->insert(f);

        ColumnWithTypeAndName col({}, cd.type, cd.name, cd.id);
        col.column = std::move(mut_col);

        block.erase(cd.name);
        block.insert(std::move(col));
    }
    return ts;
}

void DTWorkload::write(ThreadStat & write_stat)
{
    try
    {
        auto data_gen = DataGenerator::create(*opts, *table_info, *ts_gen);
        auto limiter = Limiter::create(*opts);
        uint64_t write_count = std::ceil(static_cast<double>(opts->write_count) / opts->write_thread_count);
        auto block = std::get<0>(data_gen->get(0)); // Block for reuse.

        Stopwatch sw;
        for (uint64_t i = 0; i < write_count; i++)
        {
            limiter->request();
            uint64_t key = key_gen->get64();
            std::unique_lock<std::recursive_mutex> lock;
            if (handle_lock != nullptr)
            {
                lock = handle_lock->getLock(key);
            }
            auto ts = updateBlock(block, key);

            store->write(*context, context->getSettingsRef(), block);

            if (handle_table != nullptr)
            {
                handle_table->write(key, ts);
            }
            if (opts->log_write_request)
            {
                LOG_INFO(log, "{} => {}", key, ts);
            }
        }

        write_stat.ms = sw.elapsedMilliseconds();
        write_stat.count = write_count;
        LOG_INFO(log, "write_stat: {}", write_stat.toString());
        writing_threads.fetch_sub(1, std::memory_order_relaxed);
    }
    catch (...)
    {
        tryLogCurrentException("exception thrown in DTWorkload::write");
        throw;
    }
}

template <typename T>
void DTWorkload::read(const ColumnDefines & columns, int stream_count, T func)
{
    auto ranges = {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())};
    auto filter = EMPTY_FILTER;
    int excepted_block_size = 1024;
    uint64_t read_ts = ts_gen->get();
    auto streams = store->read(
        *context,
        context->getSettingsRef(),
        columns,
        ranges,
        stream_count,
        read_ts,
        filter,
        std::vector<RuntimeFilterPtr>(),
        0,
        "DTWorkload",
        false,
        opts->is_fast_scan,
        excepted_block_size);
    std::vector<std::thread> threads;
    threads.reserve(streams.size());
    for (auto & stream : streams)
    {
        threads.push_back(std::thread(func, stream, read_ts));
    }
    for (auto & t : threads)
    {
        t.join();
    }
}

void DTWorkload::verifyHandle(uint64_t r)
{
    ColumnDefines columns;
    columns.push_back((*table_info->columns)[0]);
    columns.push_back((*table_info->columns)[1]);

    auto read_cols_gen = ReadColumnsGenerator::create(*table_info);
    int stream_count = read_cols_gen->streamCount();

    std::atomic<uint64_t> read_count = 0;
    auto verify = [r, &read_count, &handle_table = handle_table, &log = log](BlockInputStreamPtr in, uint64_t read_ts) {
        while (Block block = in->read())
        {
            read_count.fetch_add(block.rows(), std::memory_order_relaxed);
            const auto & cols = block.getColumnsWithTypeAndName();
            if (cols.size() != 2)
            {
                LOG_ERROR(log, "verifyHandle: block has {} columns, but 2 is required.", cols.size());
                throw std::logic_error("Invalid block");
            }
            const auto & handle_col = cols[0].column;
            const auto & ts_col = cols[1].column;
            if (handle_col->size() != ts_col->size())
            {
                LOG_ERROR(
                    log,
                    "verifyHandle: handle_col size {} ts_col size {}, they should be equal.",
                    handle_col->size(),
                    ts_col->size());
                throw std::logic_error("Invalid block");
            }
            for (size_t i = 0; i < handle_col->size(); i++)
            {
                // Handle must be int64 or uint64. Currently, TableGenerator would ensure this limit.
                uint64_t h = handle_col->getInt(i);
                uint64_t store_ts = ts_col->getInt(i);

                auto table_ts = handle_table->read(h);
                if (store_ts != table_ts && table_ts <= read_ts)
                {
                    LOG_ERROR(
                        log,
                        "verifyHandle: round {} handle {} ts in store {} ts in table {} read_ts {}",
                        r,
                        h,
                        store_ts,
                        table_ts,
                        read_ts);
                    throw std::logic_error("Data inconsistent");
                }
            }
        }
    };

    try
    {
        Stopwatch sw;
        read(columns, stream_count, verify);

        auto handle_count = handle_table->count();
        if (read_count.load(std::memory_order_relaxed) != handle_count)
        {
            LOG_INFO(log, "Handle count from store {} handle count from table {}", read_count, handle_count);
            throw std::logic_error("Data inconsistent");
        }
        LOG_INFO(
            log,
            "verifyHandle: round {} columns {} streams {} read_count {} read_ms {}",
            r,
            columns.size(),
            stream_count,
            handle_count,
            sw.elapsedMilliseconds());
    }
    catch (...)
    {
        tryLogCurrentException("exception thrown in verifyHandle");
        throw;
    }
}

void DTWorkload::scanAll(ThreadStat & read_stat)
{
    try
    {
        while (writing_threads.load(std::memory_order_relaxed) > 0)
        {
            const auto & columns = store->getTableColumns();
            int stream_count = opts->read_stream_count;
            std::atomic<uint64_t> read_count = 0;
            auto count_row = [&read_count](BlockInputStreamPtr in, [[maybe_unused]] uint64_t read_ts) {
                while (Block block = in->read())
                {
                    read_count.fetch_add(block.rows(), std::memory_order_relaxed);
                }
            };
            Stopwatch sw;
            read(columns, stream_count, count_row);
            read_stat.ms = sw.elapsedMilliseconds();
            read_stat.count = read_count;
            LOG_INFO(
                log,
                "scanAll: columns {} streams {} read_stat {}",
                columns.size(),
                stream_count,
                read_stat.toString());
        }
    }
    catch (...)
    {
        tryLogCurrentException("exception thrown in scanAll");
        throw;
    }
}

void DTWorkload::run(uint64_t r)
{
    std::vector<std::thread> write_threads;
    for (uint64_t i = 0; i < opts->write_thread_count; i++)
    {
        write_threads.push_back(std::thread(&DTWorkload::write, this, std::ref(stat.write_stats[i])));
    }

    std::vector<std::thread> read_threads;
    for (uint64_t i = 0; i < opts->read_thread_count; i++)
    {
        read_threads.push_back(std::thread(&DTWorkload::scanAll, this, std::ref(stat.read_stats[i])));
    }

    for (auto & t : write_threads)
    {
        t.join();
    }
    for (auto & t : read_threads)
    {
        t.join();
    }
    if (opts->verification)
    {
        verifyHandle(r);
    }
}
} // namespace DB::DM::tests
