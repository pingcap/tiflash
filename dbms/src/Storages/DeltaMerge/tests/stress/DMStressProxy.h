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
    bool verify;
    UInt32 verify_sleep_sec;

    std::vector<std::string> failpoints;
    UInt32 min_restart_sec;
    UInt32 max_restart_sec;
};

template <typename T>
class IDGenerator
{
public:
    IDGenerator(T t_)
        : t(t_)
    {}
    std::vector<T> get(Int32 count)
    {
        T start = t.fetch_add(count);
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

    KeyLock(UInt32 slot_count = default_key_lock_slot_count)
        : key_rmutexs(slot_count)
    {}

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
    UInt32 getLockIdx(Int64 key) { return key % key_rmutexs.size(); }
    std::unique_lock<std::recursive_mutex> getLockByIdx(UInt32 idx) { return std::unique_lock(key_rmutexs[idx]); }
    std::vector<std::recursive_mutex> key_rmutexs;
};

class KeySet
{
public:
    static constexpr UInt32 default_key_set_slot_count = 4096;
    KeySet(UInt32 slot_count = default_key_set_slot_count)
        : key_set_mutexs(slot_count)
        , key_sets(slot_count)
    {}

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
        int idx = id % key_set_mutexs.size();
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
        int idx = id % key_set_mutexs.size();
        std::lock_guard lock(key_set_mutexs[idx]);
        key_sets[idx].insert(id); // ignore fail
    }

    void erase(Int64 id)
    {
        int idx = id % key_set_mutexs.size();
        std::lock_guard lock(key_set_mutexs[idx]);
        key_sets[idx].erase(id);
    }

    std::vector<std::mutex> key_set_mutexs;
    std::vector<std::unordered_set<Int64>> key_sets;
};
class DMStressProxy
{
public:
    DMStressProxy(const StressOptions & opts_);

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
    void genData(UInt32 id, UInt64 rows);
    void write(const std::vector<Int64> & ids);
    UInt64 countRows(UInt32 rnd_break_prob);
    void genBlock(Block & block, const std::vector<Int64> & ids);
    static void joinThreads(std::vector<std::thread> & threads);
    void insert();
    void update();
    void deleteRange();
    void verify();

    String name;
    std::unique_ptr<Context> context;
    const ColumnDefine col_balance_define;
    const ColumnDefine col_random_define;
    DeltaMergeStorePtr store;

    const String pk_name = EXTRA_HANDLE_COLUMN_NAME;

    std::mutex mutex;

    Poco::Logger * log;
    StressOptions opts;

    std::vector<std::thread> gen_threads;
    std::vector<std::thread> read_threads;
    std::vector<std::thread> insert_threads;
    std::vector<std::thread> update_threads;
    std::vector<std::thread> delete_threads;
    std::thread verify_thread;

    std::default_random_engine rnd;

    KeyLock key_lock; // Prevent the same key write concurrent.
    KeySet pks;

    std::atomic<bool> stop;
    static constexpr UInt64 max_total_count = 200000000; // 20kw
};
} // namespace tests
} // namespace DM
} // namespace DB
