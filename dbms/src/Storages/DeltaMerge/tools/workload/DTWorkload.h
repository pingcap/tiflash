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

#include <atomic>
#include <memory>
#include <vector>

namespace DB
{
class Context;
}

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

class DTWorkload
{
public:
    static int mainEntry(int argc, char ** argv);

    DTWorkload(const WorkloadOptions & opts_, std::shared_ptr<SharedHandleTable> handle_table_, const TableInfo & table_info_);
    ~DTWorkload();

    void run(uint64_t r);

    struct ThreadWriteStat
    {
        double total_write_sec = 0.0;
        double total_do_write_sec = 0.0;
        uint64_t write_count = 0;
        uint64_t max_do_write_us = 0;

        std::string toString() const;
    };

    struct Statistics
    {
        double init_store_sec = 0.0;
        std::vector<ThreadWriteStat> write_stats;
        std::atomic<uint64_t> total_read_count = 0;
        std::atomic<uint64_t> total_read_usec = 0;

        double verify_sec = 0.0;
        uint64_t verify_count = 0;

        std::vector<std::string> toStrings(uint64_t i) const;
    };

    const Statistics & getStat() const
    {
        return stat;
    }

private:
    void write(ThreadWriteStat & write_stat);
    void verifyHandle(uint64_t r);
    void scanAll(uint64_t i);
    template <typename T>
    void read(const ColumnDefines & columns, int stream_count, T func);

    Poco::Logger * log;

    std::unique_ptr<Context> context;
    std::unique_ptr<WorkloadOptions> opts;
    std::unique_ptr<TableInfo> table_info;
    std::unique_ptr<KeyGenerator> key_gen;
    std::unique_ptr<TimestampGenerator> ts_gen;
    std::unique_ptr<DeltaMergeStore> store;

    std::unique_ptr<HandleLock> handle_lock;
    std::shared_ptr<SharedHandleTable> handle_table;

    std::atomic<int> writing_threads;

    Statistics stat;
};
} // namespace DB::DM::tests
