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
#include <PSStressEnv.h>
#include <Poco/Runnable.h>
#include <Storages/Page/PageDefines.h>

const DB::PageId MAX_PAGE_ID_DEFAULT = 1000;

class PSRunnable : public Poco::Runnable
{
public:
    void run() override;

    size_t getBytesUsed() const;
    size_t getPagesUsed() const;

    virtual String description() = 0;
    virtual bool runImpl() = 0;

    size_t bytes_used = 0;
    size_t pages_used = 0;
};

class PSWriter : public PSRunnable
{
    static size_t approx_page_mb;

public:
    PSWriter(const PSPtr & ps_, DB::UInt32 index_)
        : ps(ps_)
        , index(index_)
    {
        gen.seed(time(nullptr));
    }

    virtual ~PSWriter()
    {
        if (memory != nullptr)
        {
            free(memory);
        }
    }

    virtual String description() override
    {
        return fmt::format("(Stress Test Writer {})", index);
    }

    static void setApproxPageSize(size_t size_mb);

    static DB::ReadBufferPtr genRandomData(DB::PageId pageId, DB::MemHolder & holder);

    virtual void updatedRandomData();

    static void fillAllPages(const PSPtr & ps);

    virtual bool runImpl() override;

protected:
    virtual DB::PageId genRandomPageId();

protected:
    PSPtr ps;
    DB::UInt32 index = 0;
    std::mt19937 gen;
    DB::PageId max_page_id = MAX_PAGE_ID_DEFAULT;
    char * memory = nullptr;
    DB::ReadBufferPtr buff_ptr;
};


// PSCommonWriter can custom data size/numbers/page id range in one writebatch.
// And it also can set max_io_limit,after send limit size data into pagefile. it will stop itself.
class PSCommonWriter : public PSWriter
{
public:
    PSCommonWriter(const PSPtr & ps_, DB::UInt32 index_)
        : PSWriter(ps_, index_)
    {}

    virtual void updatedRandomData() override;

    virtual String description() override { return fmt::format("(Stress Test Common Writer {})", index); }

    virtual bool runImpl() override;

    void setBatchBufferNums(size_t numbers);

    void setBatchBufferSize(size_t size);

    void setBatchBufferLimit(size_t size_limit);

    void setBatchBufferPageRange(size_t max_page_id_);

    void setBatchBufferRange(size_t min, size_t max);

    void setFieldSize(const DB::PageFieldSizes & data_sizes);

protected:
    std::vector<DB::ReadBufferPtr> buff_ptrs;
    size_t batch_buffer_nums = 100;
    size_t batch_buffer_size = 1 * DB::MB;
    size_t batch_buffer_limit = 0;

    size_t buffer_size_min = 0;
    size_t buffer_size_max = 0;

    DB::PageFieldSizes data_sizes = {};

    virtual DB::PageId genRandomPageId() override;
    virtual size_t genBufferSize();
};


// PSWindowsWriter can better simulate the user's workload in cooperation with PSWindowsReader
// It can also be used as an independent writer to imitate user writing.
// When the user is using TiFlash, The Pageid which in PageStorage should be continuously incremented.
// In the meantime, The PageId near the end may be constantly updated.So PSWindowsWriter looks like:
//
//
//  | Pageid 1          Pageid 100                           Pageid N |
//  |-----------------------------------------------------------------|
//                                                           |              |
//                                                           |    window    |
//
// Every time the pageid written will be generated from the "window" range.
// And The random from the window should conform to the normal distribution.
// When random number bigger than Pageid N, it will be Pageid N + 1, it means new page come.
// When random number smaller than Pageid N, it means Page updated.
class PSWindowWriter : public PSCommonWriter
{
public:
    PSWindowWriter(const PSPtr & ps_, DB::UInt32 index_)
        : PSCommonWriter(ps_, index_)
    {}

    String description() override { return fmt::format("(Stress Test Window Writer {})", index); }

    void setWindowSize(size_t window_size);

    void setNormalDistributionSigma(size_t sigma);

protected:
    virtual DB::PageId genRandomPageId() override;

protected:
    size_t window_size = 100;
    size_t sigma = 9;
};

class PSIncreaseWriter : public PSCommonWriter
{
public:
    PSIncreaseWriter(const PSPtr & ps_, DB::UInt32 index_)
        : PSCommonWriter(ps_, index_)
    {}

    String description() override { return fmt::format("(Stress Test Increase Writer {})", index); }

    virtual bool runImpl() override;

    void setPageRange(size_t page_range);

protected:
    virtual DB::PageId genRandomPageId() override;

protected:
    size_t begin_page_id = 1;
    size_t end_page_id = 1;
};

class PSReader : public PSRunnable
{
public:
    PSReader(const PSPtr & ps_, DB::UInt32 index_)
        : ps(ps_)
        , index(index_)
    {
        gen.seed(time(nullptr));
    }

    virtual String description() override { return fmt::format("(Stress Test PSReader {})", index); }

    virtual bool runImpl() override;

    void setPageReadOnce(size_t page_read_once);

    void setReadDelay(size_t delay_ms);

    void setReadPageRange(size_t max_page_id);

    void setReadPageNums(size_t page_read_once);

protected:
    virtual DB::PageIds genRandomPageIds();

protected:
    PSPtr ps;
    std::mt19937 gen;
    size_t heavy_read_delay_ms = 0;
    size_t page_read_once = 5;
    DB::UInt32 index = 0;
    DB::PageId max_page_id = MAX_PAGE_ID_DEFAULT;
};

// PSWindowReader can better simulate the user's workload in cooperation with PSWindowsWriter
// It can also be used as an independent reader to imitate user reading
// Same as PSWindowWriter, it contains a "window".
// And the pageid will be randomly generated from this window (random in accordance with the normal distribution)
//
//  | Pageid 1          Pageid 100                           Pageid N |
//  |-----------------------------------------------------------------|
//                                                     |              |
//                                                     |    window    |
// The "window" will move backwards following pageid N.
// Different from PSWindowWriter, The window orientation of PSWindowReader should smaller than Pageid N.
// In this case, the pageid that PSWindowReader needs to read will never disappear.
class PSWindowReader : public PSReader
{
public:
    PSWindowReader(const PSPtr & ps_, DB::UInt32 index_)
        : PSReader(ps_, index_)
    {}

    void setWindowSize(size_t window_size);

    void setNormalDistributionSigma(size_t sigma);

    void setWriterNums(size_t writer_nums);

protected:
    virtual DB::PageIds genRandomPageIds() override;

protected:
    size_t window_size = 100;
    size_t sigma = 11;
    size_t writer_nums = 0;
    std::mt19937 gen;
};

// PSStuckReader is specially designed for holding snapshots.
// To verify the issue: https://github.com/pingcap/tics/issues/2726
// Please run it as single thread
class PSSnapshotReader : public PSReader
{
public:
    PSSnapshotReader(const PSPtr & ps_, DB::UInt32 index_)
        : PSReader(ps_, index_)
    {}

    virtual bool runImpl() override;

    void setSnapshotGetIntervalMs(size_t snapshot_get_interval_ms_);

protected:
    size_t snapshots_hold_num;
    size_t snapshot_get_interval_ms = 0;
    std::list<DB::PageStorage::SnapshotPtr> snapshots;
};