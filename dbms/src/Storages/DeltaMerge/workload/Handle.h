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

#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <Storages/DeltaMerge/workload/TableGenerator.h>
#include <fcntl.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB::ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
}
namespace DB::DM::tests
{
// HandleLock, HandleTable/SharedHandleTable are data consistency check helpers.
// Currently, only handle's data type must be int64 or uint64.


// HandleLock is to to avoid concurrent writes to the same handle between HandleTable and DeltaMergeStore.
class HandleLock
{
public:
    static constexpr uint64_t default_lock_count = 4096;

    static std::unique_ptr<HandleLock> create(const TableInfo & table_info);
    explicit HandleLock(uint64_t lock_count = default_lock_count)
        : rmtxs(lock_count)
    {}

    std::unique_lock<std::recursive_mutex> getLock(const uint64_t & handle) { return getLockByIndex(index(handle)); }

    std::vector<std::unique_lock<std::recursive_mutex>> getLocks(const std::vector<uint64_t> & handles)
    {
        std::vector<uint64_t> indexes;
        indexes.reserve(handles.size());
        for (const auto & h : handles)
        {
            indexes.push_back(index(h));
        }
        // Sort mutex indexes to avoid dead lock.
        sort(indexes.begin(), indexes.end());
        std::vector<std::unique_lock<std::recursive_mutex>> locks;
        locks.reserve(indexes.size());
        for (auto i : indexes)
        {
            locks.push_back(getLockByIndex(i));
        }
        return locks;
    }

private:
    uint64_t index(const uint64_t & handle) { return hash_func(handle) % rmtxs.size(); }

    std::unique_lock<std::recursive_mutex> getLockByIndex(uint64_t idx) { return std::unique_lock(rmtxs[idx]); }

    // Use std::recursive_mutex instead of std::mutex, because different handles may hash to the same lock in getLocks.
    std::vector<std::recursive_mutex> rmtxs;
    std::hash<uint64_t> hash_func;
};

// HandleTable is a unordered_map that maintain handle to timestamp in memory and is used to verify data in DeltaMergeStore.
class HandleTable
{
public:
    HandleTable(const std::string & fname, uint64_t max_key_count)
    {
        handle_to_ts.reserve(max_key_count * 1.2); // An extra 20% key count to avoid rehash due to sharding unbalance.
        if (!fname.empty())
        {
            recover(fname);
            if (!handle_to_ts.empty())
            {
                checkpoint(fname);
            }
            wal = std::make_unique<PosixWritableFile>(fname, false, O_CREAT | O_APPEND | O_WRONLY, 0666);
        }
    }

    void write(const uint64_t & handle, uint64_t ts)
    {
        std::lock_guard lock(mtx);
        handle_to_ts[handle] = ts;
        Record r{handle, ts};
        if (wal != nullptr && wal->write(reinterpret_cast<char *>(&r), sizeof(r)) != sizeof(r))
        {
            throw std::runtime_error(fmt::format("write ret {}", strerror(errno)));
        }
    }

    uint64_t read(const uint64_t & handle)
    {
        std::lock_guard lock(mtx);
        auto itr = handle_to_ts.find(handle);
        if (itr != handle_to_ts.end())
        {
            return itr->second;
        }
        return 0;
    }

    uint64_t count()
    {
        std::lock_guard lock(mtx);
        return handle_to_ts.size();
    }

private:
    void recover(const std::string & fname)
    {
        try
        {
            PosixRandomAccessFile f(fname, -1);
            Record r{};
            while (f.read(reinterpret_cast<char *>(&r), sizeof(r)) == sizeof(r))
            {
                handle_to_ts[r.handle] = r.ts;
            }
        }
        catch (const DB::Exception & e)
        {
            if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
            {
                throw e;
            }
        }
    }

    void checkpoint(const std::string & fname)
    {
        std::string tmp = fname + ".tmp";
        PosixWritableFile f(tmp, false, O_CREAT | O_APPEND | O_WRONLY, 0666);
        for (const auto & pa : handle_to_ts)
        {
            Record r{pa.first, pa.second};
            if (f.write(reinterpret_cast<char *>(&r), sizeof(r)) != sizeof(r))
            {
                throw std::runtime_error(fmt::format("write ret {}", strerror(errno)));
            }
        }
        f.close();

        int ret = ::unlink(fname.c_str());
        if (ret != 0)
        {
            throw std::runtime_error(fmt::format("unlink {} ret {}", fname, strerror(errno)));
        }

        ret = ::rename(tmp.c_str(), fname.c_str());
        if (ret != 0)
        {
            throw std::runtime_error(fmt::format("rename {} to {} ret {}", tmp, fname, strerror(errno)));
        }
    }
    struct Record
    {
        uint64_t handle;
        uint64_t ts;
    };
    std::mutex mtx;
    std::unordered_map<uint64_t, uint64_t> handle_to_ts;
    std::unique_ptr<PosixWritableFile> wal;
};

// SharedHandleTable is a simple wrapper of several HandleTable to reduce lock contention.
class SharedHandleTable
{
public:
    static constexpr uint64_t default_shared_count = 4096;

    explicit SharedHandleTable(
        uint64_t max_key_count,
        const std::string & waldir = "",
        uint64_t shared_cnt = default_shared_count)
        : tables(shared_cnt)
    {
        uint64_t max_key_count_per_shared = max_key_count / default_shared_count + 1;
        for (uint64_t i = 0; i < shared_cnt; i++)
        {
            auto fname = waldir.empty() ? "" : fmt::format("{}/{}.wal", waldir, i);
            tables[i] = std::make_unique<HandleTable>(fname, max_key_count_per_shared);
        }
    }
    void write(const uint64_t & handle, uint64_t ts) { tables[hash_func(handle) % tables.size()]->write(handle, ts); }
    uint64_t read(const uint64_t & handle) { return tables[hash_func(handle) % tables.size()]->read(handle); }
    uint64_t count()
    {
        uint64_t c = 0;
        for (auto & t : tables)
        {
            c += t->count();
        }
        return c;
    }

private:
    std::vector<std::unique_ptr<HandleTable>> tables;
    std::hash<uint64_t> hash_func;
};
} // namespace DB::DM::tests