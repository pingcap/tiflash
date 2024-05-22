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

#include <Common/CurrentMetrics.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Context.h>
#include <Poco/AutoPtr.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V2/PageDefines.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>

namespace DB::PS::V2::tests
{
using PSPtr = std::shared_ptr<PageStorage>;

class PageStorageMultiWritersTest : public DB::base::TiFlashStorageTestBasic
{
public:
    PageStorageMultiWritersTest()
        : file_provider{DB::tests::TiFlashTestEnv::getDefaultFileProvider()}
    {}

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        bkg_pool = std::make_shared<DB::BackgroundProcessingPool>(
            4,
            "bg-page-",
            std::make_shared<JointThreadInfoJeallocMap>());
        // default test config
        config.file_roll_size = 4 * MB;
        config.gc_max_valid_rate = 0.5;
        config.num_write_slots = 4; // At most 4 threads for write

        storage = reopenWithConfig(config);
    }

    std::shared_ptr<PageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto spool = db_context->getPathPool().withTable("test", "t", false);
        auto delegator = spool.getPSDiskDelegatorSingle("log");
        auto storage = std::make_shared<PageStorage>("test.t", delegator, config_, file_provider, *bkg_pool);
        storage->restore();
        return storage;
    }

protected:
    PageStorageConfig config;
    std::shared_ptr<BackgroundProcessingPool> bkg_pool;
    std::shared_ptr<PageStorage> storage;
    const FileProviderPtr file_provider;
};

struct TestContext
{
    static constexpr PageId MAX_PAGE_ID = 2000;

    std::atomic<bool> running_without_exception = true;
    std::atomic<bool> running_without_timeout = true;

    bool gc_enabled = true;

    void setRunable()
    {
        running_without_exception = true;
        running_without_timeout = true;
    }
};

class PSWriter : public Poco::Runnable
{
    DB::UInt32 index = 0;
    PSPtr storage;
    std::mt19937 gen;

public:
    size_t bytes_written;
    size_t pages_written;

private:
    static size_t approx_page_kb;

    TestContext & ctx;

public:
    PSWriter(const PSPtr & storage_, DB::UInt32 idx, TestContext & ctx_)
        : index(idx)
        , storage(storage_)
        , bytes_written(0)
        , pages_written(0)
        , ctx(ctx_)
    {}

    static void setApproxPageSize(size_t size_kb)
    {
        LOG_INFO(&Poco::Logger::get("root"), "Page approx size is set to " + DB::toString(size_kb / 1024.0, 2) + "MB");
        approx_page_kb = size_kb;
    }

    static DB::ReadBufferPtr genRandomData(const PageId pageId, MemHolder & holder)
    {
        // fill page with random bytes
        const size_t buff_sz = approx_page_kb * 1024 + random() % 300;
        char * buff = static_cast<char *>(malloc(buff_sz));
        const char buff_ch = pageId % 0xFF;
        memset(buff, buff_ch, buff_sz);

        holder = createMemHolder(buff, [&](char * p) { free(p); });

        return std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz);
    }

    static void fillAllPages(const PSPtr & storage, TestContext & ctx)
    {
        for (PageId page_id = 0; page_id < ctx.MAX_PAGE_ID; ++page_id)
        {
            MemHolder holder;
            DB::ReadBufferPtr buff = genRandomData(page_id, holder);

            WriteBatch wb;
            wb.putPage(page_id, 0, buff, buff->buffer().size());
            storage->write(std::move(wb));
            if (page_id % 100 == 0)
                LOG_INFO(&Poco::Logger::get("root"), "writer wrote page" + DB::toString(page_id));
        }
    }

    void run() override
    {
        while (ctx.running_without_exception && ctx.running_without_timeout)
        {
            assert(storage != nullptr);
            std::normal_distribution<> d{ctx.MAX_PAGE_ID / 2.0, 150};
            const PageId page_id = static_cast<PageId>(std::round(d(gen))) % ctx.MAX_PAGE_ID;

            MemHolder holder;
            DB::ReadBufferPtr buff = genRandomData(page_id, holder);

            WriteBatch wb;
            wb.putPage(page_id, 0, buff, buff->buffer().size());
            storage->write(std::move(wb));
            ++pages_written;
            bytes_written += buff->buffer().size();
            // LOG_INFO(&Poco::Logger::get("root"), "writer[" + DB::toString(index) + "] wrote page" + DB::toString(pageId));
        }
        LOG_INFO(&Poco::Logger::get("root"), "writer[" + DB::toString(index) + "] exit");
    }
};

size_t PSWriter::approx_page_kb = 16;

class PSReader : public Poco::Runnable
{
    DB::UInt32 index = 0;
    PSPtr storage;
    const size_t heavy_read_delay_ms;

public:
    size_t pages_read;
    size_t bytes_read;

    TestContext & ctx;

public:
    PSReader(const PSPtr & storage_, DB::UInt32 idx, size_t delay_ms, TestContext & ctx_)
        : index(idx)
        , storage(storage_)
        , heavy_read_delay_ms(delay_ms)
        , pages_read(0)
        , bytes_read(0)
        , ctx(ctx_)
    {}

    void run() override
    {
        while (ctx.running_without_exception && ctx.running_without_timeout)
        {
            {
                // sleep [0~10) ms
                const uint32_t micro_seconds_to_sleep = random() % 10;
                usleep(micro_seconds_to_sleep * 1000);
            }
            assert(storage != nullptr);
#if 0
            const DB::PageId pageId = random() % MAX_PAGE_ID;
            try
            {
                DB::Page page = storage->read(pageId, nullptr);
                ++pages_read;
                bytes_read += page.data.size();
            }
            catch (DB::Exception & e)
            {
                LOG_TRACE(&Poco::Logger::get("root"), e.displayText());
            }
#else
            PageIds page_ids;
            for (size_t i = 0; i < 5; ++i)
            {
                page_ids.emplace_back(random() % ctx.MAX_PAGE_ID);
            }
            try
            {
                auto page_map = storage->read(page_ids);
                for (const auto & page : page_map)
                {
                    // use `sleep` to mock heavy read
                    if (heavy_read_delay_ms > 0)
                    {
                        //const uint32_t micro_seconds_to_sleep = 10;
                        usleep(heavy_read_delay_ms * 1000);
                    }
                    ++pages_read;
                    bytes_read += page.second.data.size();
                }
            }
            catch (DB::Exception & e)
            {
                LOG_TRACE(&Poco::Logger::get("root"), e.displayText());
            }
#endif
        }
        LOG_INFO(&Poco::Logger::get("root"), "reader[" + DB::toString(index) + "] exit");
    }
};

class PSGc
{
    PSPtr storage;
    TestContext & ctx;

public:
    PSGc(const PSPtr & storage_, TestContext & ctx_)
        : storage(storage_)
        , ctx(ctx_)
    {}
    void onTime(Poco::Timer & /* t */)
    {
        if (!ctx.gc_enabled)
            return;
        try
        {
            storage->gc();
        }
        catch (DB::Exception & e)
        {
            // if gc throw exception stop the test
            ctx.running_without_exception = false;
        }
    }
};

struct StressTimeout
{
    TestContext & ctx;
    explicit StressTimeout(TestContext & ctx_)
        : ctx(ctx_)
    {}
    void onTime(Poco::Timer & /* t */)
    {
        LOG_INFO(&Poco::Logger::get("root"), "Timeout. exiting...");
        ctx.running_without_timeout = false;
    }
};

// A full set of writers, readers, gc.
struct Suit
{
    Suit(
        TestContext & ctx_,
        PSPtr storage_,
        size_t num_writers_,
        size_t num_readers_,
        UInt64 gc_interval_sec,
        UInt64 cancel_sec_)
        : ctx(ctx_)
        , storage(storage_)
        , num_writers(num_writers_)
        , num_readers(num_readers_)
        , pool("multi_writers_test_pool", 1 + num_writers_ + num_readers_, 1 + num_writers_ + num_readers_)
        , writers(num_writers)
        , readers(num_readers)
        , gc_timer(1000, gc_interval_sec * 1000)
        , gc_runner(storage, ctx)
        , cancel_sec(cancel_sec_)
        , cancel_timer(cancel_sec * 1000)
        , cancel_runner(ctx)
    {
        LOG_INFO(
            &Poco::Logger::get("root"),
            "start running with these threads: W:" + DB::toString(num_writers) + ",R:" + DB::toString(num_readers)
                + ",Gc:1, config.num_writer_slots:" + DB::toString(storage->config.num_write_slots.get()));
    }

    void run()
    {
        // start writer threads
        for (size_t i = 0; i < num_writers; ++i)
        {
            writers[i] = std::make_shared<PSWriter>(storage, i, ctx);
            pool.start(*writers[i], "writer" + DB::toString(i));
        }
        // start read threads
        for (size_t i = 0; i < num_readers; ++i)
        {
            readers[i] = std::make_shared<PSReader>(storage, i, 0, ctx);
            pool.start(*readers[i], "reader" + DB::toString(i));
        }

        // start gc thread
        gc_timer.start(Poco::TimerCallback<PSGc>(gc_runner, &PSGc::onTime));

        // set timeout
        LOG_INFO(&Poco::Logger::get("root"), "benchmark timeout: " + DB::toString(cancel_sec) + "s");
        cancel_timer.start(Poco::TimerCallback<StressTimeout>(cancel_runner, &StressTimeout::onTime));
    }

    void wait() { pool.joinAll(); }

    TestContext & ctx;
    PSPtr storage;

    const size_t num_writers;
    const size_t num_readers;

    Poco::ThreadPool pool;

    std::vector<std::shared_ptr<PSWriter>> writers;
    std::vector<std::shared_ptr<PSReader>> readers;

    Poco::Timer gc_timer;
    PSGc gc_runner;

    size_t cancel_sec;
    Poco::Timer cancel_timer;
    StressTimeout cancel_runner;
};

TEST_F(PageStorageMultiWritersTest, DISABLED_MultiWriteReadRestore)
try
{
    size_t num_writers = 4;
    size_t num_readers = 4;
    size_t num_write_slots = 4;

    size_t gc_interval_s = 5;
    size_t timeout_s = 5 * 60;

    srand(0x123987);
    PageStorageConfig curr_config = config;
    curr_config.num_write_slots = num_write_slots;

    storage = reopenWithConfig(curr_config);

    TestContext ctx;
    // ctx.gc_enabled = false;
    PSWriter::fillAllPages(storage, ctx);

    // Create full suit and run
    {
        Suit suit(ctx, storage, num_writers, num_readers, gc_interval_s, timeout_s);
        suit.run();
        suit.wait();
    }

    auto old_storage = storage;
    auto old_snapshot = old_storage->getConcreteSnapshot();
    storage = reopenWithConfig(curr_config);
    auto snapshot = storage->getConcreteSnapshot();

    auto old_valid_pages = old_snapshot->version()->validPageIds();
    auto valid_pages = snapshot->version()->validPageIds();
    ASSERT_EQ(valid_pages.size(), old_valid_pages.size());

    for (const auto & page_id : old_valid_pages)
    {
        auto old_entry = old_storage->getEntry(page_id, old_snapshot);
        auto entry = storage->getEntry(page_id, snapshot);
        ASSERT_EQ(old_entry.fileIdLevel(), entry.fileIdLevel()) << "of Page[" << page_id << "]";
        ASSERT_EQ(old_entry.offset, entry.offset) << "of Page[" << page_id << "]";
        ASSERT_EQ(old_entry.size, entry.size) << "of Page[" << page_id << "]";
        ASSERT_EQ(old_entry.tag, entry.tag) << "of Page[" << page_id << "]";
        ASSERT_EQ(old_entry.checksum, entry.checksum) << "of Page[" << page_id << "]";

        auto old_page = old_storage->read(page_id, nullptr, old_snapshot);
        const char * buf = old_page.data.begin();
        for (size_t i = 0; i < old_page.data.size(); ++i)
            ASSERT_EQ(((size_t) * (buf + i)) % 0xFF, page_id % 0xFF);

        auto page = storage->read(page_id, nullptr, snapshot);
        buf = page.data.begin();
        for (size_t i = 0; i < old_page.data.size(); ++i)
            ASSERT_EQ(((size_t) * (buf + i)) % 0xFF, page_id % 0xFF);
    }
}
CATCH

} // namespace DB::PS::V2::tests
