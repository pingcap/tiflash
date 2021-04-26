#include <Common/CurrentMetrics.h>
#include <Encryption/FileProvider.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/AutoPtr.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>

#include "gtest/gtest.h"

#define private public
#include <Storages/Page/PageStorage.h>
#undef private

namespace DB
{
namespace tests
{

using PSPtr = std::shared_ptr<DB::PageStorage>;

class PageStorageMultiPaths_test : public ::testing::TestWithParam<size_t>
{
public:
    PageStorageMultiPaths_test()
        : root_path(Poco::Path{TiFlashTestEnv::getTemporaryPath() + "/ps_multi_paths/data0"}.toString()),
          storage(),
          file_provider{DB::tests::TiFlashTestEnv::getContext().getFileProvider()}
    {
    }

    static void SetUpTestCase() {}

protected:
    void SetUp() override
    {
        // drop dir if exists
        if (Poco::File p(root_path); p.exists())
        {
            Poco::File file(Poco::Path(root_path).parent());
            file.remove(true);
        }
        // default test config
        config.file_roll_size    = 4 * MB;
        config.gc_max_valid_rate = 0.5;
        config.num_write_slots   = 4; // At most 4 threads for write
    }

    static Strings getMultiTestPaths(size_t num_folders_for_test)
    {
        Strings paths;
        for (size_t i = 0; i < num_folders_for_test; ++i)
            paths.emplace_back(Poco::Path{TiFlashTestEnv::getTemporaryPath() + "/ps_multi_paths/data" + toString(i)}.toString());
        return paths;
    }

    String getParentPathForTable(const String & /*db*/, const String & table = "table")
    {
        return Poco::Path{TiFlashTestEnv::getTemporaryPath() + "/ps_multi_paths/data" + toString(0) + "/" + table + "/log"}.toString();
    }

protected:
    String                       root_path;
    PageStorage::Config          config;
    std::shared_ptr<PageStorage> storage;
    const FileProviderPtr        file_provider;
};

TEST_P(PageStorageMultiPaths_test, DeltaWriteReadRestore)
try
{
    config.file_roll_size = 128 * MB;

    size_t          number_of_paths = GetParam();
    auto            all_paths       = getMultiTestPaths(number_of_paths);
    auto            capacity = std::make_shared<PathCapacityMetrics>(0, all_paths, std::vector<size_t>{}, Strings{}, std::vector<size_t>{});
    StoragePathPool pool     = PathPool(all_paths, all_paths, Strings{}, capacity, file_provider).withTable("test", "table", false);

    storage = std::make_shared<PageStorage>("test.table", pool.getPSDiskDelegatorMulti("log"), config, file_provider);
    storage->restore();

    const UInt64 tag    = 0;
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    for (size_t i = 0; i < 100; ++i)
    {
        WriteBatch    batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(i, tag, buff, buf_sz);
        storage->write(std::move(batch));
    }

    // Read
    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    // restore
    storage = std::make_shared<PageStorage>("test.t", pool.getPSDiskDelegatorMulti("log"), config, file_provider);
    storage->restore();

    // Read again
    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    {
        // Check whether write is correctly.
        {
            WriteBatch    batch;
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(2, tag, buff, buf_sz);
            storage->write(std::move(batch));
        }
        // Read to check
        {
            Page page0 = storage->read(0);
            ASSERT_EQ(page0.data.size(), buf_sz);
            ASSERT_EQ(page0.page_id, 0UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
            }
            Page page1 = storage->read(1);
            ASSERT_EQ(page1.data.size(), buf_sz);
            ASSERT_EQ(page1.page_id, 1UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
            }
            Page page2 = storage->read(2);
            ASSERT_EQ(page2.data.size(), buf_sz);
            ASSERT_EQ(page2.page_id, 2UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page2.data.begin() + i), static_cast<char>(i % 0xff));
            }
        }
    }

    // Restore. This ensure last write is correct.
    storage = std::make_shared<PageStorage>("test.t", pool.getPSDiskDelegatorMulti("log"), config, file_provider);
    storage->restore();

    // Read again to check all data.
    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
        Page page2 = storage->read(2);
        ASSERT_EQ(page2.data.size(), buf_sz);
        ASSERT_EQ(page2.page_id, 2UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page2.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(DifferentNumberOfDeltaPaths, PageStorageMultiPaths_test, testing::Range(1UL, 7UL));


} // namespace tests
} // namespace DB
