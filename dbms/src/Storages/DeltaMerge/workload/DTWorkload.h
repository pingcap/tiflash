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

#include <Interpreters/Context_fwd.h>
#include <fmt/ranges.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace DB
{
class Context;
class Block;
} // namespace DB

namespace DB::DM
{
class DeltaMergeStore;
struct ColumnDefine;
using ColumnDefines = std::vector<ColumnDefine>;
} // namespace DB::DM

namespace Poco
{
class Logger;
}

namespace DB::DM::tests
{
class KeyGenerator;
class TimestampGenerator;
class DataGenerator;
class TableGenerator;
struct TableInfo;
struct WorkloadOptions;
class HandleLock;
class SharedHandleTable;

class ThreadStat
{
public:
    std::string toString() const { return fmt::format("ms {} count {} speed {}", ms, count, speed()); }

    // count per second
    uint64_t speed() const { return ms == 0 ? 0 : count * 1000 / ms; }

private:
    uint64_t ms = 0;
    uint64_t count = 0;
    friend class DTWorkload;
};

class Statistics
{
public:
    explicit Statistics(int write_thread_count = 0, int read_thread_count = 0)
        : init_ms(0)
        , write_stats(write_thread_count)
        , read_stats(read_thread_count)
    {}

    uint64_t writePerSecond() const
    {
        uint64_t wps = 0;
        std::for_each(write_stats.begin(), write_stats.end(), [&](const ThreadStat & stat) { wps += stat.speed(); });
        return wps;
    }

    uint64_t readPerSecond() const
    {
        uint64_t rps = 0;
        std::for_each(read_stats.begin(), read_stats.end(), [&](const ThreadStat & stat) { rps += stat.speed(); });
        return rps;
    }

    uint64_t initMS() const { return init_ms; }

    std::vector<std::string> toStrings() const
    {
        std::vector<std::string> v;
        v.push_back(fmt::format("init_ms {}", initMS()));
        for (size_t i = 0; i < write_stats.size(); i++)
        {
            v.push_back(fmt::format("write_{}: {}", i, write_stats[i].toString()));
        }
        for (size_t i = 0; i < read_stats.size(); i++)
        {
            v.push_back(fmt::format("read_{}: {}", i, read_stats[i].toString()));
        }
        return v;
    }

private:
    uint64_t init_ms;
    std::vector<ThreadStat> write_stats;
    std::vector<ThreadStat> read_stats;

    friend class DTWorkload;
};

class DTWorkload
{
public:
    static int mainEntry(int argc, char ** argv);

    DTWorkload(
        const WorkloadOptions & opts_,
        std::shared_ptr<SharedHandleTable> handle_table_,
        const TableInfo & table_info_,
        ContextPtr context_);
    ~DTWorkload();

    void run(uint64_t r);

    const Statistics & getStat() const { return stat; }

private:
    void write(ThreadStat & write_stat);
    void verifyHandle(uint64_t r);
    void scanAll(ThreadStat & read_stat);
    template <typename T>
    void read(const ColumnDefines & columns, int stream_count, T func);
    uint64_t updateBlock(Block & block, uint64_t key);

    Poco::Logger * log;

    ContextPtr context;
    std::unique_ptr<WorkloadOptions> opts;
    std::unique_ptr<TableInfo> table_info;
    std::unique_ptr<KeyGenerator> key_gen;
    std::unique_ptr<TimestampGenerator> ts_gen;
    std::shared_ptr<DeltaMergeStore> store;

    std::unique_ptr<HandleLock> handle_lock;
    std::shared_ptr<SharedHandleTable> handle_table;

    std::atomic<int> writing_threads;

    Statistics stat;
};
} // namespace DB::DM::tests
