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

#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/stress/DMStressProxy.h>
#include <sys/time.h>

namespace DB
{
namespace DM
{
namespace tests
{
IDGenerator<Int64> pk{0};

IDGenerator<UInt64> tso{clock_gettime_ns(CLOCK_MONOTONIC)};

template <typename T>
void insertColumn(Block & block, const DataTypePtr & type, const String & name, Int64 col_id, const std::vector<T> & values)
{
    ColumnWithTypeAndName col({}, type, name, col_id);
    IColumn::MutablePtr m_col = col.type->createColumn();
    for (const T & v : values)
    {
        Field field = v;
        m_col->insert(field);
    }
    col.column = std::move(m_col);
    block.insert(std::move(col));
}

DMStressProxy::DMStressProxy(const StressOptions & opts_)
    : name(opts_.table_name)
    , col_balance_define(2, "balance", std::make_shared<DataTypeUInt64>())
    , col_random_define(3, "random_text", std::make_shared<DataTypeString>())
    , log(&Poco::Logger::get("DMStressProxy"))
    , opts(opts_)
    , rnd(::time(nullptr))
    , stop(false)
{
    context = std::make_unique<Context>(DMTestEnv::getContext());
    auto cols = DMTestEnv::getDefaultColumns();
    cols->emplace_back(col_balance_define);
    cols->emplace_back(col_random_define);
    auto handle_col = (*cols)[0];
    store = std::make_shared<DeltaMergeStore>(*context, true, "test", name, 100, *cols, handle_col, false, 1, DeltaMergeStore::Settings());
    if (opts_.verify)
    {
        ColumnDefines columns;
        columns.emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
        BlockInputStreamPtr in = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ tso.get(),
                                             EMPTY_FILTER,
                                             /* expected_block_size= */ 1024)[0];
        while (Block block = in->read())
        {
            if (block.columns() != 1)
            {
                LOG_FMT_ERROR(log, "block columns must be 1.");
                throw DB::Exception("block columns must be 1.", ErrorCodes::LOGICAL_ERROR);
            }
            // only one columns, so only need to fetch begin iterator.
            auto itr = block.begin();
            std::vector<Int64> ids;
            ids.reserve(itr->column->size());
            for (size_t i = 0; i < itr->column->size(); i++)
            {
                ids.emplace_back(itr->column->getInt(i));
            }
            pks.insert(ids);
        }
    }
}

void DMStressProxy::genMultiThread()
{
    if (opts.gen_total_rows <= 0)
    {
        LOG_FMT_INFO(log, "opts.gen_total_rows {}", opts.gen_total_rows);
        return;
    }

    UInt64 gen_rows_per_thread = opts.gen_total_rows / opts.gen_concurrency + 1;
    UInt64 gen_total_rows = gen_rows_per_thread * opts.gen_concurrency; // May large than opts.gen_total_row.
    LOG_FMT_INFO(log, "Generate concurrency: {} Generate rows per thread: {} Generate total rows: {}", opts.gen_concurrency, gen_rows_per_thread, gen_total_rows);

    gen_threads.reserve(opts.gen_concurrency);
    for (UInt32 i = 0; i < opts.gen_concurrency; i++)
    {
        gen_threads.push_back(std::thread(&DMStressProxy::genData, this, i, gen_rows_per_thread));
    }
}

void DMStressProxy::genData(UInt32 id, UInt64 rows)
{
    try
    {
        std::string thread_name = "dm_gen_" + std::to_string(id);
        setThreadName(thread_name.c_str());
        UInt64 generated_count = 0;
        while (generated_count < rows)
        {
            auto c = std::min(opts.gen_rows_per_block, rows - generated_count);
            auto ids = pk.get(c);
            write(ids);
            generated_count += c;
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("genData fail");
    }
}

void DMStressProxy::write(const std::vector<Int64> & ids)
{
    try
    {
        Block block;
        genBlock(block, ids);

        auto locks = key_lock.getLocks(ids);
        store->write(*context, context->getSettingsRef(), block);
        if (opts.verify)
        {
            pks.insert(ids);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("runProxy fail");
    }
}

void DMStressProxy::genBlock(Block & block, const std::vector<Int64> & ids)
{
    insertColumn<Int64>(block, std::make_shared<DataTypeInt64>(), pk_name, EXTRA_HANDLE_COLUMN_ID, ids);
    std::vector<UInt64> v_tso = tso.get(static_cast<UInt32>(ids.size()));
    insertColumn<UInt64>(block, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME, VERSION_COLUMN_ID, v_tso);
    std::vector<UInt64> v_tag(ids.size(), 0);
    insertColumn<UInt64>(block, TAG_COLUMN_TYPE, TAG_COLUMN_NAME, TAG_COLUMN_ID, v_tag);
    std::vector<UInt64> v_balance(ids.size(), 1024);
    insertColumn<UInt64>(block, col_balance_define.type, col_balance_define.name, col_balance_define.id, v_balance);
    std::string s(128, 'C');
    std::vector<String> v_s(ids.size(), s);
    insertColumn<String>(block, col_random_define.type, col_random_define.name, col_random_define.id, v_s);
}

void DMStressProxy::readMultiThread()
{
    auto work = [&](UInt32 id) {
        try
        {
            std::string thread_name = "dm_read_" + std::to_string(id);
            setThreadName(thread_name.c_str());
            while (!stop)
            {
                countRows(rnd() % 100);
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("readMultiThread fail");
        }
    };

    read_threads.reserve(opts.read_concurrency);
    for (UInt32 i = 0; i < opts.read_concurrency; i++)
    {
        read_threads.push_back(std::thread(work, i));
    }
}

UInt64 DMStressProxy::countRows(UInt32 rnd_break_prob)
{
    const auto & columns = store->getTableColumns();

    BlockInputStreamPtr in = store->read(*context,
                                         context->getSettingsRef(),
                                         columns,
                                         {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                         /* num_streams= */ 1,
                                         /* max_version= */ tso.get(),
                                         EMPTY_FILTER,
                                         /* expected_block_size= */ 1024)[0];

    UInt64 total_count = 0;
    while (Block block = in->read())
    {
        total_count += block.rows();
        if (opts.read_sleep_us > 0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(opts.read_sleep_us));
        }
        if (rnd() % 100 < rnd_break_prob)
        {
            break; // Randomly break
        }
    }
    LOG_FMT_INFO(log, "countRows ThreadID: {} TotalCount: {}", std::this_thread::get_id(), total_count);
    return total_count;
}

void DMStressProxy::insertMultiThread()
{
    auto work = [&](UInt32 id) {
        try
        {
            std::string thread_name = "dm_insert_" + std::to_string(id);
            setThreadName(thread_name.c_str());
            while (!stop)
            {
                insert();
                if (opts.write_sleep_us > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
                }
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("insertMultiThread fail");
        }
    };

    insert_threads.reserve(opts.insert_concurrency);
    for (UInt32 i = 0; i < opts.insert_concurrency; i++)
    {
        insert_threads.push_back(std::thread(work, i));
    }
}

void DMStressProxy::updateMultiThread()
{
    auto work = [&](UInt32 id) {
        try
        {
            std::string thread_name = "dm_update_" + std::to_string(id);
            setThreadName(thread_name.c_str());
            while (!stop)
            {
                update();
                if (opts.write_sleep_us > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
                }
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("updateMultiThread fail");
        }
    };

    update_threads.reserve(opts.update_concurrency);
    for (UInt32 i = 0; i < opts.update_concurrency; i++)
    {
        update_threads.push_back(std::thread(work, i));
    }
}

void DMStressProxy::deleteMultiThread()
{
    auto work = [&](UInt32 id) {
        try
        {
            std::string thread_name = "dm_delete_" + std::to_string(id);
            setThreadName(thread_name.c_str());
            while (!stop)
            {
                deleteRange();
                if (opts.write_sleep_us > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
                }
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("deleteMultiThread fail");
        }
    };

    delete_threads.reserve(opts.delete_concurrency);
    for (UInt32 i = 0; i < opts.delete_concurrency; i++)
    {
        delete_threads.push_back(std::thread(work, i));
    }
}

void DMStressProxy::insert()
{
    auto new_ids = pk.get(opts.write_rows_per_block); // Generate new id for insert
    write(new_ids);
}

void DMStressProxy::update()
{
    std::vector<Int64> ids;
    for (UInt32 i = 0; i < opts.write_rows_per_block; i++)
    {
        Int64 id = rnd() % pk.max();
        if (std::find(ids.begin(), ids.end(), id) == ids.end())
        {
            ids.push_back(id); // Get already exist id for update
        }
    }
    write(ids);
}

void DMStressProxy::deleteRange()
{
    Int64 id1 = rnd() % pk.max();
    Int64 id2 = id1 + (rnd() % opts.write_rows_per_block) + 1;
    auto range = RowKeyRange::fromHandleRange(HandleRange{id1, id2});

    std::vector<Int64> ids; // [id1, id2)
    ids.reserve(id2 - id1);
    for (Int64 i = id1; i < id2; i++)
    {
        ids.push_back(i);
    }
    auto locks = key_lock.getLocks(ids);
    store->deleteRange(*context, context->getSettingsRef(), range);
    if (opts.verify)
    {
        pks.erase(ids);
    }
}

void DMStressProxy::waitGenThreads()
{
    LOG_FMT_INFO(log, "wait gen threads begin: {}", gen_threads.size());
    joinThreads(gen_threads);
    LOG_FMT_INFO(log, "wait gen threads end: {}", gen_threads.size());
}

void DMStressProxy::waitReadThreads()
{
    LOG_FMT_INFO(log, "wait read threads begin: {}", read_threads.size());
    joinThreads(read_threads);
    LOG_FMT_INFO(log, "wait read threads end: {}", read_threads.size());
}

void DMStressProxy::waitInsertThreads()
{
    LOG_FMT_INFO(log, "wait insert threads begin: {}", insert_threads.size());
    joinThreads(insert_threads);
    LOG_FMT_INFO(log, "wait insert threads end: {}", insert_threads.size());
}

void DMStressProxy::waitUpdateThreads()
{
    LOG_FMT_INFO(log, "wait update threads begin: {}", update_threads.size());
    joinThreads(update_threads);
    LOG_FMT_INFO(log, "wait update threads end: {}", update_threads.size());
}

void DMStressProxy::waitDeleteThreads()
{
    LOG_FMT_INFO(log, "wait delete threads begin: {}", delete_threads.size());
    joinThreads(delete_threads);
    LOG_FMT_INFO(log, "wait delete threads end: {}", delete_threads.size());
}

void DMStressProxy::joinThreads(std::vector<std::thread> & threads)
{
    for (auto & t : threads)
    {
        t.join();
    }
    threads.clear();
}

void DMStressProxy::verifySingleThread()
{
    auto work = [&]() {
        try
        {
            setThreadName("verify");
            while (!stop)
            {
                verify();
                sleep(opts.verify_sleep_sec);
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("verifySingleThread fail");
        }
    };
    verify_thread = std::thread(work);
}

void DMStressProxy::waitVerifyThread()
{
    LOG_FMT_INFO(log, "wait verify thread begin");
    verify_thread.join();
    LOG_FMT_INFO(log, "wait verify thread end");
}

void DMStressProxy::verify()
{
    auto locks = key_lock.getAllLocks(); // Prevent all keys being write.
    ColumnDefines columns;
    columns.emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
    BlockInputStreamPtr in = store->read(*context,
                                         context->getSettingsRef(),
                                         columns,
                                         {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                         /* num_streams= */ 1,
                                         /* max_version= */ tso.get(),
                                         EMPTY_FILTER,
                                         /* expected_block_size= */ 1024)[0];
    UInt64 dm_total_count = 0;
    while (Block block = in->read())
    {
        dm_total_count += block.rows();
        std::string msg = "Verify rows: " + std::to_string(block.rows()) + " columns: " + std::to_string(block.columns());
        if (block.columns() != 1)
        {
            LOG_FMT_ERROR(log, "{} columns must be 1.", msg);
            throw DB::Exception(msg + " columns must be 1.", ErrorCodes::LOGICAL_ERROR);
        }

        auto itr = block.begin();
        for (size_t i = 0; i < itr->column->size(); i++)
        {
            Int64 id = itr->column->getInt(i);
            if (!pks.exist(id))
            {
                LOG_FMT_ERROR(log, "Verify id {} not found from pks.", id);
                throw DB::Exception("id " + std::to_string(id) + " not found from pks.", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    UInt64 pks_total_count = pks.count();
    std::string msg = "Verify dm_total_count: " + std::to_string(dm_total_count) + " pks_total_count: " + std::to_string(pks_total_count);
    if (pks_total_count != dm_total_count)
    {
        LOG_FMT_ERROR(log, "{} total_count mismatch.", msg);
        throw DB::Exception(msg, ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        LOG_FMT_INFO(log, "{} Verify success!", msg);
    }

    if (pks_total_count >= max_total_count)
    {
        LOG_FMT_INFO(log, "pks_total_count: {} max_total_count: {}", pks_total_count, max_total_count);
        stop.store(true); // Stop the process to avoid use too much memory.
    }
}

void DMStressProxy::run()
{
    genMultiThread(); // Run the generate data threads with other read-write thread concurrently
    readMultiThread();
    insertMultiThread();
    updateMultiThread();
    deleteMultiThread();
    if (opts.verify)
    {
        verifySingleThread();
    }

    waitGenThreads();
    waitReadThreads();
    waitInsertThreads();
    waitUpdateThreads();
    waitDeleteThreads();
    if (opts.verify)
    {
        waitVerifyThread();
    }
}
} // namespace tests
} // namespace DM
} // namespace DB
