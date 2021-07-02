#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

namespace DB
{
namespace DM
{
namespace tests
{

struct StressOptions
{
    String mode;
    UInt32 insert_concurrency;
    UInt32 update_concurrency;
    UInt32 delete_concurrency;
    UInt32 write_rows_per_block;  // insert or delete block size
    UInt32 write_sleep_us;
    UInt32 read_concurrency;
    UInt32 read_sleep_us;
    UInt64 gen_total_rows;
    UInt32 gen_rows_per_block;
    UInt32 gen_concurrency;
};

template <typename T>
class IDGenerator
{
public:
    IDGenerator(T t_) : t(t_) {}
    std::vector<T> get(Int32 count)
    {
        T              start = t.fetch_add(count);
        std::vector<T> v(count);
        for (int i = 0; i < count; i++)
        {
            v[i] = start + i;
        }
        return v;
    }

    T get() { return t.fetch_add(1); }

    T max() { return t.load(); }

private:
    std::atomic<T> t;
};

class DeltaMergeStoreProxy
{
public:
    DeltaMergeStoreProxy(const StressOptions& opts_)
        : name("stress"),
          col_balance_define(2, "balance", std::make_shared<DataTypeUInt64>()),
          col_random_define(3, "random_text", std::make_shared<DataTypeString>()),
          log(&Poco::Logger::get("DeltaMergeStoreProxy")),
          opts(opts_)
    {
        String path = DB::tests::TiFlashTestEnv::getTemporaryPath() + name;
        Poco::File file(path);
        if (file.exists())
        {
            file.remove(true);
        }
        context = std::make_unique<Context>(DMTestEnv::getContext());
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_balance_define);
        table_column_defines->emplace_back(col_random_define);
        ColumnDefine handle_column_define = (*table_column_defines)[0];
        store = std::make_shared<DeltaMergeStore>(
            *context, true, "test", name, *table_column_defines, handle_column_define, false, 1, DeltaMergeStore::Settings());
    }


    void genMultiThread();
    void readMultiThread();
    void insertMultiThread();
    void updateMultiThread();
    void deleteMultiThread();

    void waitGenThreads();
    void waitReadThreads();
    void waitInsertThreads();
    void waitUpdateThreads();
    void waitDeleteThreads();
private:
    void genData(UInt64 rows);
    void write(const std::vector<Int64> & ids);
    UInt64 countRows();
    void genBlock(Block & block, const std::vector<Int64> & ids);
    void joinThreads(std::vector<std::thread>& threads);
    void insert();
    void update();
    void deleteRange();

    String                   name;
    std::unique_ptr<Context> context;
    const ColumnDefine       col_balance_define;
    const ColumnDefine       col_random_define;
    DeltaMergeStorePtr       store;

    const String pk_name = EXTRA_HANDLE_COLUMN_NAME;

    std::mutex mutex;

    Poco::Logger * log;
    StressOptions opts;

    std::vector<std::thread> gen_threads;
    std::vector<std::thread> read_threads;
    std::vector<std::thread> insert_threads;
    std::vector<std::thread> update_threads;
    std::vector<std::thread> delete_threads;
};
} // namespace tests
} // namespace DM
} // namespace DB
