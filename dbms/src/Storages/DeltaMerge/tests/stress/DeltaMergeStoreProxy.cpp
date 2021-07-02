#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/stress/DeltaMergeStoreProxy.h>
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

void DeltaMergeStoreProxy::genBlock(Block & block, const std::vector<Int64> & ids)
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

void DeltaMergeStoreProxy::write(const std::vector<Int64> & ids)
{
    Block block;
    genBlock(block, ids);
    std::lock_guard lock{mutex};
    store->write(*context, context->getSettingsRef(), std::move(block));
}

UInt64 DeltaMergeStoreProxy::countRows()
{
    BlockInputStreamPtr in;
    {
        std::lock_guard lock{mutex};
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
    }
    LOG_INFO(log, "ThreadID: " << std::this_thread::get_id() <<  " TotalCount: " << total_count);
    return total_count;
}

void DeltaMergeStoreProxy::genDataMultiThread(UInt64 count, Int32 concurrency)
{
    UInt64 count_per_thread = count / concurrency + 1;
    UInt64 will_gen_count = count_per_thread * concurrency;
    LOG_INFO(log, "Will gen " << will_gen_count << " rows.");
    std::vector<std::thread> threads;
    threads.reserve(concurrency);
    for (Int32 i = 0; i < concurrency; i++)
    {
        threads.push_back(std::thread(&DeltaMergeStoreProxy::genData, this, count_per_thread));
    }

    for (auto & t : threads)
    {
        t.join();
    }

    LOG_INFO(log, "Generate data finished.");
    UInt64 real_rows = countRows();
    LOG_INFO(log, "Except rows: " << will_gen_count << " Real rows: " << real_rows);    
}

void DeltaMergeStoreProxy::genData(UInt64 count)
{
    UInt64 gen_count = 0;
    while (gen_count < count)
    {
        auto c   = std::min(gen_count + 128, count) - gen_count;
        auto ids = pk.get(c);
        write(ids);
        gen_count += 128;
    }
}

void DeltaMergeStoreProxy::readDataMultiThread(Int32 concurrency)
{
    auto work = [&]()
    {
        for (;;)
        {
            countRows();
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(concurrency);
    for (Int32 i = 0; i < concurrency; i++)
    {
        threads.push_back(std::thread(work));
    }

    for (auto & t : threads)
    {
        t.join();
    }
}
} // namespace tests
} // namespace DM
} // namespace DB
