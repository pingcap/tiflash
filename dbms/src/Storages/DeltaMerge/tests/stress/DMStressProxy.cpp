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

IDGenerator<UInt64> tso{0};

template <typename T>
void insertColumn(Block & block, const DataTypePtr & type, const String & name, Int64 col_id, const std::vector<T> & values)
{
    ColumnWithTypeAndName col({}, type, name, col_id);
    IColumn::MutablePtr   m_col = col.type->createColumn();
    for (const T & v : values)
    {
        Field field = v;
        m_col->insert(field);
    }
    col.column = std::move(m_col);
    block.insert(std::move(col));
}

void DMStressProxy::genMultiThread()
{
    if (opts.gen_total_rows <= 0)
    {
        LOG_INFO(log, "opts.gen_total_rows " << opts.gen_total_rows);
        return;
    }

    UInt64 gen_rows_per_thread = opts.gen_total_rows / opts.gen_concurrency + 1;
    UInt64 gen_total_rows      = gen_rows_per_thread * opts.gen_concurrency; // May large than opts.gen_total_row.
    LOG_INFO(log,
             "Generate concurrency: " << opts.gen_concurrency << " Generate rows per thread: " << gen_rows_per_thread
                                      << " Generate total rows: " << gen_total_rows);

    gen_threads.reserve(opts.gen_concurrency);
    for (UInt32 i = 0; i < opts.gen_concurrency; i++)
    {
        gen_threads.push_back(std::thread(&DMStressProxy::genData, this, gen_rows_per_thread));
    }

    LOG_INFO(log, "Generate data finished.");
    UInt64 total_rows = countRows();
    LOG_INFO(log, "Total rows: " << total_rows);
}

void DMStressProxy::genData(UInt64 rows)
{
    UInt64 generated_count = 0;
    while (generated_count < rows)
    {
        auto c   = std::min(opts.gen_rows_per_block, rows - generated_count);
        auto ids = pk.get(c);
        write(ids);
        generated_count += c;
    }
}

UInt64 getCurrUS()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000000ul + tv.tv_usec;
}

void DMStressProxy::write(const std::vector<Int64> & ids)
{
    Block block;
    genBlock(block, ids);
    store->write(*context, context->getSettingsRef(), std::move(block));
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
    std::string         s("C", 128);
    std::vector<String> v_s(ids.size(), s);
    insertColumn<String>(block, col_random_define.type, col_random_define.name, col_random_define.id, v_s);
}

void DMStressProxy::readMultiThread()
{
    auto work = [&]() {
        for (;;)
        {
            countRows();
        }
    };
    read_threads.reserve(opts.read_concurrency);
    for (UInt32 i = 0; i < opts.read_concurrency; i++)
    {
        read_threads.push_back(std::thread(work));
    }
}

UInt64 DMStressProxy::countRows()
{
    BlockInputStreamPtr in;
    {
        const auto &    columns = store->getTableColumns();
        in                      = store->read(*context,
                         context->getSettingsRef(),
                         columns,
                         {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                         /* num_streams= */ 1,
                         /* max_version= */ tso.get(),
                         EMPTY_FILTER,
                         /* expected_block_size= */ 1024)[0];
    }
    UInt64 total_count = 0;
    while (Block block = in->read())
    {
        total_count += block.rows();
        if (opts.read_sleep_us > 0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(opts.read_sleep_us));
        }
    }
    LOG_INFO(log, "ThreadID: " << std::this_thread::get_id() << " TotalCount: " << total_count);
    return total_count;
}

void DMStressProxy::insertMultiThread()
{
    auto work = [&]() {
        for (;;)
        {
            insert();
            if (opts.write_sleep_us > 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
            }
        }
    };

    insert_threads.reserve(opts.insert_concurrency);
    for (UInt32 i = 0; i < opts.insert_concurrency; i++)
    {
        insert_threads.push_back(std::thread(work));
    }
}

void DMStressProxy::updateMultiThread()
{
    auto work = [&]() {
        for (;;)
        {
            update();
            if (opts.write_sleep_us > 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
            }
        }
    };

    update_threads.reserve(opts.update_concurrency);
    for (UInt32 i = 0; i < opts.update_concurrency; i++)
    {
        update_threads.push_back(std::thread(work));
    }
}

void DMStressProxy::deleteMultiThread()
{
    auto work = [&]() {
        for (;;)
        {
            deleteRange();
            if (opts.write_sleep_us > 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(opts.write_sleep_us));
            }
        }
    };

    delete_threads.reserve(opts.delete_concurrency);
    for (UInt32 i = 0; i < opts.delete_concurrency; i++)
    {
        delete_threads.push_back(std::thread(work));
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
    Int64 id1   = rnd() % pk.max();
    Int64 id2   = id1 + (rnd() % opts.write_rows_per_block);
    auto range = RowKeyRange::fromHandleRange(HandleRange{id1, id2});
    store->deleteRange(*context, context->getSettingsRef(), range);
}

void DMStressProxy::waitGenThreads()
{
    LOG_INFO(log, "wait gen threads begin: " << gen_threads.size());
    joinThreads(gen_threads);
    LOG_INFO(log, "wait gen threads end: " << gen_threads.size());
}

void DMStressProxy::waitReadThreads()
{
    LOG_INFO(log, "wait read threads begin: " << read_threads.size());
    joinThreads(read_threads);
    LOG_INFO(log, "wait read threads end: " << read_threads.size());
}

void DMStressProxy::waitInsertThreads()
{
    LOG_INFO(log, "wait insert threads begin: " << insert_threads.size());
    joinThreads(insert_threads);
    LOG_INFO(log, "wait insert threads end: " << insert_threads.size());
}

void DMStressProxy::waitUpdateThreads()
{
    LOG_INFO(log, "wait update threads begin: " << update_threads.size());
    joinThreads(update_threads);
    LOG_INFO(log, "wait update threads end: " << update_threads.size());
}

void DMStressProxy::waitDeleteThreads()
{
    LOG_INFO(log, "wait delete threads begin: " << delete_threads.size());
    joinThreads(delete_threads);
    LOG_INFO(log, "wait delete threads end: " << delete_threads.size());
}

void DMStressProxy::joinThreads(std::vector<std::thread> & threads)
{
    for (auto & t : threads)
    {
        t.join();
    }
    threads.clear();
}

} // namespace tests
} // namespace DM
} // namespace DB
