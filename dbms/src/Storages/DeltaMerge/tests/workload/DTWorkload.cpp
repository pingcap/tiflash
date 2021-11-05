#include <Poco/Logger.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/DeltaMerge/tests/workload/DTWorkload.h>
#include <Storages/DeltaMerge/tests/workload/DataGenerator.h>
#include <Storages/DeltaMerge/tests/workload/Handle.h>
#include <Storages/DeltaMerge/tests/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/tests/workload/Limiter.h>
#include <Storages/DeltaMerge/tests/workload/Options.h>
#include <Storages/DeltaMerge/tests/workload/ReadColumnsGenerator.h>
#include <Storages/DeltaMerge/tests/workload/TableGenerator.h>
#include <Storages/DeltaMerge/tests/workload/TimestampGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
namespace DB::DM::tests
{
DTWorkload::DTWorkload(const WorkloadOptions & opts_)
    : log(&Poco::Logger::get("DTWorkload"))
    , context(std::make_unique<Context>(DMTestEnv::getContext()))
    , opts(std::make_unique<WorkloadOptions>(opts_))
    , writing_threads(0)
{
    table_gen = TableGenerator::create(opts_);
    table_info = std::make_unique<TableInfo>(table_gen->get());
    auto v = table_info->toStrings();
    for (const auto & s : v)
    {
        LOG_INFO(log, "TableInfo: " << s);
    }

    key_gen = KeyGenerator::create(opts_);
    ts_gen = std::make_unique<TimestampGenerator>();

    Stopwatch sw;
    store = std::make_unique<DeltaMergeStore>(*context, true, table_info->db_name, table_info->table_name, *table_info->columns, //
                                              table_info->handle,
                                              table_info->is_common_handle,
                                              table_info->rowkey_column_indexes.size(),
                                              DeltaMergeStore::Settings());
    stat.init_store_sec = sw.elapsedSeconds();
    LOG_INFO(log, fmt::format("Init store {} seconds", stat.init_store_sec));

    if (opts_.verification)
    {
        handle_lock = std::make_unique<HandleLock>();
        initHandleTable();
        verifyHandle(std::numeric_limits<uint64_t>::max()); // uint64_max means recovery.
    }
}

DTWorkload::~DTWorkload()
{
    store->flushCache(*context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
}

void DTWorkload::initHandleTable()
{
    // HandleTable supports simply WAL to handle crash recovery and verify data consistency with DeltaMergeStore.
    // However, DeltaMergeStore may loss data after crash.
    // Currently, we use this WAL to recover handle table after gracefully shutdown and open again (Although seems not very necessary).
    auto wal_dir = fmt::format("{}handle_wal/{}/{}", DB::tests::TiFlashTestEnv::getTemporaryPath(), table_info->db_name, table_info->table_name);
    LOG_INFO(log, "wal_dir: " << wal_dir);

    // Create wal dir if not exist...use 'system' for simplicity.
    auto cmd = fmt::format("mkdir -p {}", wal_dir);
    system(cmd.c_str());

    Stopwatch sw;
    handle_table = std::make_unique<SharedHandleTable>(wal_dir);
    stat.init_handle_table_sec = sw.elapsedSeconds();
    LOG_INFO(log, fmt::format("Init handle table {} seconds", stat.init_handle_table_sec));
}

void DTWorkload::write(ThreadWriteStat & write_stat)
{
    try
    {
        auto data_gen = DataGenerator::create(*opts, *table_info, *key_gen, *ts_gen);
        auto limiter = Limiter::create(*opts);
        uint64_t write_count = std::ceil(static_cast<double>(opts->write_count) / opts->write_thread_count);

        Stopwatch sw;
        uint64_t max_do_write_us = std::numeric_limits<uint64_t>::min();
        uint64_t total_do_write_us = 0;
        for (uint64_t i = 0; i < write_count; i++)
        {
            limiter->request();
            auto [block, key, ts] = data_gen->get();

            std::unique_lock<std::recursive_mutex> lock;
            if (handle_lock != nullptr)
            {
                lock = handle_lock->getLock(key);
            }

            Stopwatch w;
            store->write(*context, context->getSettingsRef(), std::move(block));
            uint64_t t = w.elapsed() / 1000; // us
            max_do_write_us = std::max(max_do_write_us, t);
            total_do_write_us += t;

            if (handle_table != nullptr)
            {
                handle_table->write(key, ts);
            }
        }

        write_stat.total_write_sec = sw.elapsedSeconds();
        write_stat.total_do_write_sec = total_do_write_us / 1000000.0;
        write_stat.write_count = write_count;
        write_stat.max_do_write_us = max_do_write_us;

        LOG_INFO(log, write_stat.toString());
        writing_threads.fetch_sub(1, std::memory_order_relaxed);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, e.message());
        std::abort();
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, e.what());
        std::abort();
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknow exception.");
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
    auto streams = store->read(*context, context->getSettingsRef(), columns, ranges, stream_count, ts_gen->get(), filter, excepted_block_size);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < streams.size(); i++)
    {
        threads.push_back(std::thread(func, streams[i]));
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
    auto verify = [r, &read_count, &columns, &handle_table = handle_table](BlockInputStreamPtr in) {
        while (Block block = in->read())
        {
            read_count.fetch_add(block.rows(), std::memory_order_relaxed);
            auto & cols = block.getColumnsWithTypeAndName();
            if (cols.size() != 2)
            {
                throw std::out_of_range(fmt::format("loadHandle: block has {} columns, but 2 is required.", cols.size()));
            }
            auto & handle_col = cols[0].column;
            auto & ts_col = cols[1].column;
            if (handle_col->size() != ts_col->size())
            {
                throw std::out_of_range(fmt::format("loadHandle: handle_col size {} ts_col size {}, they should be equal.", handle_col->size(), ts_col->size()));
            }
            for (size_t i = 0; i < handle_col->size(); i++)
            {
                // Handle must be int64 or uint64. Currently, TableGenterator would ensure this limit.
                uint64_t h = handle_col->getInt(i);
                uint64_t store_ts = ts_col->getInt(i);

                auto table_ts = handle_table->read(h);
                if (store_ts != table_ts)
                {
                    throw std::logic_error(fmt::format("round {} handle {} ts in store {} ts in table {}", r, h, store_ts, table_ts));
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
        throw std::logic_error(fmt::format("Handle count from store {} handle count from table {}", read_count, handle_count));
    }
    stat.verify_count = handle_count;
    LOG_INFO(log, fmt::format("verifyHandle: round {} columns {} streams {} read_count {} read_sec {} handle_count {}", r, columns.size(), stream_count, read_count.load(std::memory_order_relaxed), stat.verify_sec, handle_count));
}

void DTWorkload::randomRead()
{
    auto read_cols_gen = ReadColumnsGenerator::create(*table_info);
    while (writing_threads.load(std::memory_order_relaxed) > 0)
    {
        auto & columns = store->getTableColumns();
        int stream_count = read_cols_gen->streamCount();

        std::atomic<uint64_t> read_count = 0;
        auto countRow = [&read_count](BlockInputStreamPtr in) {
            while (Block block = in->read())
            {
                read_count.fetch_add(block.rows(), std::memory_order_relaxed);
            }
        };
        Stopwatch sw;
        read(columns, stream_count, countRow);
        double read_sec = sw.elapsedSeconds();

        stat.total_read_sec += read_sec;
        stat.total_read_count += read_count.load(std::memory_order_relaxed);

        LOG_INFO(log, fmt::format("randomRead: columns {} streams {} read_count {} read_sec {} handle_count {}", columns.size(), stream_count, read_count.load(std::memory_order_relaxed), read_sec, handle_table->count()));
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
    randomRead();
    for (auto & t : write_threads)
    {
        t.join();
    }
    if (handle_table != nullptr)
    {
        verifyHandle(r);
    }
}
} // namespace DB::DM::tests
