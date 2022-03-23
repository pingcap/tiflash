// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Config/TOMLConfiguration.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tools/workload/DTWorkload.h>
#include <Storages/DeltaMerge/tools/workload/DataGenerator.h>
#include <Storages/DeltaMerge/tools/workload/Handle.h>
#include <Storages/DeltaMerge/tools/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/tools/workload/Limiter.h>
#include <Storages/DeltaMerge/tools/workload/Options.h>
#include <Storages/DeltaMerge/tools/workload/ReadColumnsGenerator.h>
#include <Storages/DeltaMerge/tools/workload/TableGenerator.h>
#include <Storages/DeltaMerge/tools/workload/TimestampGenerator.h>
#include <Storages/DeltaMerge/tools/workload/Utils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <cpptoml.h>

namespace DB::DM::tests
{
DB::Settings createSettings(const std::string & fname)
{
    DB::Settings settings;
    if (!fname.empty())
    {
        auto table = cpptoml::parse_file(fname);
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
        config->add(new DB::TOMLConfiguration(table), /*shared=*/false); // Take ownership of TOMLConfig
        settings.setProfile("default", *config);
    }
    return settings;
}

DTWorkload::DTWorkload(const WorkloadOptions & opts_, std::shared_ptr<SharedHandleTable> handle_table_, const TableInfo & table_info_)
    : log(&Poco::Logger::get("DTWorkload"))
    , opts(std::make_unique<WorkloadOptions>(opts_))
    , table_info(std::make_unique<TableInfo>(table_info_))
    , handle_table(handle_table_)
    , writing_threads(0)
{
    auto settings = createSettings(opts_.config_file);
    context = std::make_unique<Context>(DB::tests::TiFlashTestEnv::getContext(settings, opts_.work_dirs));

    auto v = table_info->toStrings();
    for (const auto & s : v)
    {
        LOG_FMT_INFO(log, "TableInfo: {}", s);
    }

    key_gen = KeyGenerator::create(opts_);
    ts_gen = std::make_unique<TimestampGenerator>();

    Stopwatch sw;
    store = std::make_unique<DeltaMergeStore>(
        *context,
        true,
        table_info->db_name,
        table_info->table_name,
        table_info->table_id,
        *table_info->columns,
        table_info->handle,
        table_info->is_common_handle,
        table_info->rowkey_column_indexes.size(),
        DeltaMergeStore::Settings());
    stat.init_store_sec = sw.elapsedSeconds();
    LOG_FMT_INFO(log, "Init store {} seconds", stat.init_store_sec);

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

void DTWorkload::write(ThreadWriteStat & write_stat)
{
    try
    {
        auto data_gen = DataGenerator::create(*opts, *table_info, *ts_gen);
        auto limiter = Limiter::create(*opts);
        uint64_t write_count = std::ceil(static_cast<double>(opts->write_count) / opts->write_thread_count);

        Stopwatch sw;
        uint64_t max_do_write_us = std::numeric_limits<uint64_t>::min();
        uint64_t total_do_write_us = 0;
        for (uint64_t i = 0; i < write_count; i++)
        {
            limiter->request();
            uint64_t key = key_gen->get64();
            std::unique_lock<std::recursive_mutex> lock;
            if (handle_lock != nullptr)
            {
                lock = handle_lock->getLock(key);
            }
            auto [block, ts] = data_gen->get(key);

            Stopwatch w;
            store->write(*context, context->getSettingsRef(), block);
            uint64_t t = w.elapsed() / 1000; // us
            max_do_write_us = std::max(max_do_write_us, t);
            total_do_write_us += t;

            if (handle_table != nullptr)
            {
                handle_table->write(key, ts);
            }
            if (opts->log_write_request)
            {
                LOG_FMT_INFO(log, "{} => {}", key, ts);
            }
        }

        write_stat.total_write_sec = sw.elapsedSeconds();
        write_stat.total_do_write_sec = total_do_write_us / 1000000.0;
        write_stat.write_count = write_count;
        write_stat.max_do_write_us = max_do_write_us;

        LOG_FMT_INFO(log, "{}", write_stat.toString());
        writing_threads.fetch_sub(1, std::memory_order_relaxed);
    }
    catch (...)
    {
        tryLogCurrentException("exception thrown in DTWorkload::write");
        std::abort();
    }
}

std::string DTWorkload::ThreadWriteStat::toString() const
{
    double write_tps = write_count / total_write_sec;
    double do_write_tps = write_count / total_do_write_sec;
    uint64_t avg_do_write_us = total_do_write_sec * 1000000 / write_count;
    return fmt::format("total_write_sec {} total_do_write_sec {} write_count {} write_tps {} do_write_tps {} avg_do_write_us {} max_do_write_us {}",
                       total_write_sec,
                       total_do_write_sec,
                       write_count,
                       write_tps,
                       do_write_tps,
                       avg_do_write_us,
                       max_do_write_us);
}

template <typename T>
void DTWorkload::read(const ColumnDefines & columns, int stream_count, T func)
{
    auto ranges = {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())};
    auto filter = EMPTY_FILTER;
    int excepted_block_size = 1024;
    uint64_t read_ts = ts_gen->get();
    auto streams = store->read(*context, context->getSettingsRef(), columns, ranges, stream_count, read_ts, filter, excepted_block_size);
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
                auto s = fmt::format("loadHandle: block has {} columns, but 2 is required.", cols.size());
                LOG_ERROR(log, s);
                throw std::logic_error(s);
            }
            const auto & handle_col = cols[0].column;
            const auto & ts_col = cols[1].column;
            if (handle_col->size() != ts_col->size())
            {
                auto s = fmt::format("loadHandle: handle_col size {} ts_col size {}, they should be equal.", handle_col->size(), ts_col->size());
                LOG_ERROR(log, s);
                throw std::logic_error(s);
            }
            for (size_t i = 0; i < handle_col->size(); i++)
            {
                // Handle must be int64 or uint64. Currently, TableGenterator would ensure this limit.
                uint64_t h = handle_col->getInt(i);
                uint64_t store_ts = ts_col->getInt(i);

                auto table_ts = handle_table->read(h);
                if (store_ts != table_ts && table_ts <= read_ts)
                {
                    auto s = fmt::format("round {} handle {} ts in store {} ts in table {} read_ts {}", r, h, store_ts, table_ts, read_ts);
                    LOG_ERROR(log, s);
                    throw std::logic_error(s);
                }
            }
        }
    };

    Stopwatch sw;
    read(columns, stream_count, verify);
    stat.verify_sec = sw.elapsedSeconds();

    auto handle_count = handle_table->count();
    if (read_count.load(std::memory_order_relaxed) != handle_count)
    {
        auto s = fmt::format("Handle count from store {} handle count from table {}", read_count, handle_count);
        LOG_ERROR(log, s);
        throw std::logic_error(s);
    }
    stat.verify_count = handle_count;
    LOG_FMT_INFO(
        log,
        "verifyHandle: round {} columns {} streams {} read_count {} read_sec {} handle_count {}",
        r,
        columns.size(),
        stream_count,
        read_count.load(std::memory_order_relaxed),
        stat.verify_sec,
        handle_count);
}

void DTWorkload::scanAll(uint64_t i)
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
            double read_sec = sw.elapsedSeconds();

            stat.total_read_usec.fetch_add(read_sec * 1000000, std::memory_order_relaxed);
            stat.total_read_count.fetch_add(read_count.load(std::memory_order_relaxed), std::memory_order_relaxed);

            LOG_FMT_INFO(
                log,
                "scanAll[{}]: columns {} streams {} read_count {} read_sec {} handle_count {}",
                i,
                columns.size(),
                stream_count,
                read_count.load(std::memory_order_relaxed),
                read_sec,
                handle_table->count());
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
    writing_threads.store(opts->write_thread_count, std::memory_order_relaxed);
    stat.write_stats.resize(opts->write_thread_count);
    for (uint64_t i = 0; i < opts->write_thread_count; i++)
    {
        write_threads.push_back(std::thread(&DTWorkload::write, this, std::ref(stat.write_stats[i])));
    }

    std::vector<std::thread> read_threads;
    for (uint64_t i = 0; i < opts->read_thread_count; i++)
    {
        read_threads.push_back(std::thread(&DTWorkload::scanAll, this, i));
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

std::vector<std::string> DTWorkload::Statistics::toStrings(uint64_t i) const
{
    std::vector<std::string> v;
    v.push_back(fmt::format("[{}]{}", i, localTime()));
    v.push_back(fmt::format("init_store_sec {}", init_store_sec));
    v.push_back(fmt::format("total_read_count {} total_read_sec {}", total_read_count, total_read_usec / 1000000.0));
    uint64_t total_write_count = 0;
    double total_do_write_sec = 0.0;
    for (size_t k = 0; k < write_stats.size(); k++)
    {
        v.push_back(fmt::format("Thread[{}] {}", k, write_stats[k].toString()));
        total_write_count += write_stats[k].write_count;
        total_do_write_sec += write_stats[k].total_do_write_sec;
    }
    v.push_back(fmt::format("total_write_count {} total_do_write_sec {}", total_write_count, total_do_write_sec));
    v.push_back(fmt::format("verify_count {} verify_sec {}", verify_count, verify_sec));
    return v;
}
} // namespace DB::DM::tests
