#include "gtest/gtest.h"

#define private public
#include <Storages/Page/PageStorage.h>
#undef private

namespace DB
{
namespace tests
{

class PageStorage_test : public ::testing::Test
{
public:
    PageStorage_test(): path("./t"), storage() {}

protected:
    void SetUp() override {
        // drop dir if exists
        Poco::File file(path);
        if (file.exists()) {
            file.remove(true);
        }
        config.file_roll_size = 512;
        config.merge_hint_low_used_file_num = 1;
        storage = std::make_shared<PageStorage>(path, config);
    }
protected:
    String path;
    PageStorage::Config config;
    std::shared_ptr<PageStorage> storage;
};

TEST_F(PageStorage_test, WriteRead)
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff,sizeof(c_buff));
        batch.putPage(0, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);
        storage->write(batch);
    }

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

TEST_F(PageStorage_test, WriteReadGc)
{
    const size_t buf_sz = 256;
    char c_buff[buf_sz];

    const size_t num_repeat = 10;
    PageId pid = 1;
    const char page0_byte = 0x3f;
    {
        // put page0
        WriteBatch batch;
        memset(c_buff, page0_byte, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, 0, buff, buf_sz);
        storage->write(batch);
    }
    // repeated put page1
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(pid, 0, buff, buf_sz);
        storage->write(batch);
    }

    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

    storage->gc();

    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

}

TEST_F(PageStorage_test, GcConcurrencyDelPage)
{
    PageId pid = 0;
    // gc move Page0 -> PageFile{5,1}
    PageCacheMap map;
    map.emplace(pid, PageCache{.file_id=1, .level=0});
    // write thread del Page0 in page_map before gc thread get unique_lock of `read_mutex`
    storage->page_cache_map.clear();
    // gc continue
    storage->gcUpdatePageMap(map);
    // page0 don't update to page_map
    const PageCache entry = storage->getCache(pid);
    ASSERT_FALSE(entry.isValid());
}

static void EXPECT_PagePos_LT(PageFileIdAndLevel p0, PageFileIdAndLevel p1)
{
    EXPECT_LT(p0, p1);
}

TEST_F(PageStorage_test, GcPageMove)
{
    EXPECT_PagePos_LT({4, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 1}, {6, 1});
    EXPECT_PagePos_LT({5, 2}, {6, 1});

    const PageId pid = 0;
    // old Page0 is in PageFile{5, 0}
    storage->page_cache_map.emplace(pid, PageCache{.file_id=5, .level=0,});
    // gc move Page0 -> PageFile{5,1}
    PageCacheMap map;
    map.emplace(pid, PageCache{.file_id=5, .level=1,});
    storage->gcUpdatePageMap(map);
    // page_map get updated
    const PageCache entry = storage->getCache(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 5u);
    ASSERT_EQ(entry.level, 1u);
}

TEST_F(PageStorage_test, GcConcurrencySetPage)
{
    const PageId pid = 0;
    // gc move Page0 -> PageFile{5,1}
    PageCacheMap map;
    map.emplace(pid, PageCache{.file_id=5, .level=1,});
    // write thread insert newer Page0 before gc thread get unique_lock on `read_mutex`
    storage->page_cache_map.emplace(pid, PageCache{.file_id=6, .level=0,});
    // gc continue
    storage->gcUpdatePageMap(map);
    // read
    const PageCache entry = storage->getCache(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 6u);
    ASSERT_EQ(entry.level, 0u);
}

} // namespace tests
} // namespace DB
