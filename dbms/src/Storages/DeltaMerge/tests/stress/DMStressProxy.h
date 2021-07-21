#pragma once
#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

#include <mutex>
#include <random>

namespace DB
{
namespace DM
{
namespace tests
{

struct StressOptions
{
    UInt32 insert_concurrency;
    UInt32 update_concurrency;
    UInt32 delete_concurrency;
    UInt32 write_rows_per_block; // insert or update block size
    UInt32 write_sleep_us;
    UInt32 read_concurrency;
    UInt32 read_sleep_us;
    UInt64 gen_total_rows;
    UInt32 gen_rows_per_block;
    UInt32 gen_concurrency;
    String table_name;
    bool   verify;
    UInt32 verify_sleep_sec;
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

class KeyLock
{
public:
    static constexpr UInt32 default_key_lock_slot_count = 4096;

    KeyLock(UInt32 slot_count = default_key_lock_slot_count) : key_rmutexs(slot_count) {}

    std::vector<std::unique_lock<std::recursive_mutex>> getLocks(const std::vector<Int64> & keys)
    {
        std::vector<UInt32> idxs;
        for (Int64 key : keys)
        {
            idxs.push_back(getLockIdx(key));
        }
        sort(idxs.begin(), idxs.end()); // Sort mutex to avoid dead lock.
        std::vector<std::unique_lock<std::recursive_mutex>> locks;
        for (UInt32 i : idxs)
        {
            locks.push_back(getLockByIdx(i));
        }
        return locks;
    }

    std::vector<std::unique_lock<std::recursive_mutex>> getAllLocks()
    {
        std::vector<std::unique_lock<std::recursive_mutex>> locks;
        for (UInt32 i = 0; i < key_rmutexs.size(); i++)
        {
            locks.push_back(getLockByIdx(i));
        }
        return locks;
    }

private:
    UInt32                                 getLockIdx(Int64 key) { return key % key_rmutexs.size(); }
    std::unique_lock<std::recursive_mutex> getLockByIdx(UInt32 idx) { return std::unique_lock(key_rmutexs[idx]); }
    std::vector<std::recursive_mutex>      key_rmutexs;
};

class KeySet
{
public:
    static constexpr UInt32 default_key_set_slot_count = 4096;
    KeySet(UInt32 slot_count = default_key_set_slot_count) : key_set_mutexs(slot_count), key_sets(slot_count) {}

    void insert(const std::vector<Int64> & ids)
    {
        for (Int64 id : ids)
        {
            insert(id);
        }
    }

    void erase(const std::vector<Int64> & ids)
    {
        for (Int64 id : ids)
        {
            erase(id);
        }
    }

    bool exist(Int64 id)
    {
        int             idx = id % key_set_mutexs.size();
        std::lock_guard lock(key_set_mutexs[idx]);
        return key_sets[idx].count(id) > 0;
    }

    // The caller should guarantees thread safety
    UInt64 count()
    {
        UInt64 total_count = 0;
        for (const auto & key_set : key_sets)
        {
            total_count += key_set.size();
        }
        return total_count;
    }

private:
    void insert(Int64 id)
    {
        int             idx = id % key_set_mutexs.size();
        std::lock_guard lock(key_set_mutexs[idx]);
        key_sets[idx].insert(id); // ignore fail
    }

    void erase(Int64 id)
    {
        int             idx = id % key_set_mutexs.size();
        std::lock_guard lock(key_set_mutexs[idx]);
        key_sets[idx].erase(id);
    }

    std::vector<std::mutex>                key_set_mutexs;
    std::vector<std::unordered_set<Int64>> key_sets;
};
class DMStressProxy
{
public:
    DMStressProxy(const StressOptions & opts_)
        : name(opts_.table_name),
          col_balance_define(2, "balance", std::make_shared<DataTypeUInt64>()),
          col_random_define(3, "random_text", std::make_shared<DataTypeString>()),
          log(&Poco::Logger::get("DMStressProxy")),
          opts(opts_),
          rnd(::time(nullptr)),
          stop(false)
    {
        if (opts_.verify)
        {
            auto       dir_name = DB::tests::TiFlashTestEnv::getTemporaryPath() + "data/test/" + name;
            Poco::File dir(dir_name);
            LOG_INFO(log, "dir_name: " << dir_name);
            if (dir.exists())
            {
                LOG_INFO(log, "remove dir_name: " << dir_name);
                dir.remove(true);
            }
        }
        context   = std::make_unique<Context>(DMTestEnv::getContext());
        auto cols = DMTestEnv::getDefaultColumns();
        cols->emplace_back(col_balance_define);
        cols->emplace_back(col_random_define);
        auto handle_col = (*cols)[0];
        store = std::make_shared<DeltaMergeStore>(*context, true, "test", name, *cols, handle_col, false, 1, DeltaMergeStore::Settings());
    }

    void run();

    void genMultiThread();
    void readMultiThread();
    void insertMultiThread();
    void updateMultiThread();
    void deleteMultiThread();
    void verifySingleThread();

    void waitGenThreads();
    void waitReadThreads();
    void waitInsertThreads();
    void waitUpdateThreads();
    void waitDeleteThreads();
    void waitVerifyThread();

private:
    void   genData(UInt32 id, UInt64 rows);
    void   write(const std::vector<Int64> & ids);
    UInt64 countRows(UInt32 rnd_break_prob);
    void   genBlock(Block & block, const std::vector<Int64> & ids);
    void   joinThreads(std::vector<std::thread> & threads);
    void   insert();
    void   update();
    void   deleteRange();
    void   verify();

    String                   name;
    std::unique_ptr<Context> context;
    const ColumnDefine       col_balance_define;
    const ColumnDefine       col_random_define;
    DeltaMergeStorePtr       store;

    const String pk_name = EXTRA_HANDLE_COLUMN_NAME;

    std::mutex mutex;

    Poco::Logger * log;
    StressOptions  opts;

    std::vector<std::thread> gen_threads;
    std::vector<std::thread> read_threads;
    std::vector<std::thread> insert_threads;
    std::vector<std::thread> update_threads;
    std::vector<std::thread> delete_threads;
    std::thread              verify_thread;

    std::default_random_engine rnd;

    KeyLock key_lock; // Prevent the same key write concurrent.
    KeySet  pks;

    std::atomic<bool>       stop;
    static constexpr UInt64 max_total_count = 200000000; // 20kw
};
} // namespace tests
} // namespace DM
} // namespace DB
