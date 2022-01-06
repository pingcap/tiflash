#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/PathPool.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace PS::V3::tests
{
class PageStorageTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto delegator = path_pool->getPSDiskDelegatorSingle("log");
        Poco::File path(delegator->defaultPath());

        if (!path.exists())
        {
            path.createDirectories();
        }
        page_storage = std::make_shared<PageStorageImpl>("test.t", delegator, config, file_provider);
    }

    void pushMvccSeqForword(size_t seq_nums, UInt64 get_snapshot = UINT64_MAX)
    {
        PageId page_id = UINT64_MAX - 100;
        [[maybe_unused]] PageVersionAndEntriesV3 meanless_seq_entries;

        for (size_t idx = 0; idx < seq_nums; idx++)
        {
            putInMvccAndBlobStore(page_id, fixed_test_buff_size, 1, meanless_seq_entries, 0, true, false);
            if (get_snapshot != UINT64_MAX && idx == get_snapshot)
            {
                snapshots_holder.emplace_back(page_storage->page_directory.createSnapshot());
            }
        }
    }

    void putInMvccAndBlobStore(PageId page_id,
                               size_t buff_size,
                               size_t buff_nums,
                               PageVersionAndEntriesV3 & seq_entries,
                               UInt64 seq_start,
                               bool no_need_add = false,
                               bool copy_one_epoch = false)
    {
        char c_buff[buff_size * buff_nums];
        WriteBatch wb;
        for (size_t i = 0; i < buff_nums; ++i)
        {
            for (size_t j = 0; j < buff_size; ++j)
            {
                c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
            }

            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);

            auto edit = page_storage->blob_store.write(wb, nullptr);
            const auto & record_last = edit.getRecords().rbegin();

            if (!no_need_add)
            {
                seq_entries.emplace_back(std::make_tuple(seq_start, 0, record_last->entry));
                if (copy_one_epoch)
                {
                    // If copy_one_epoch enable
                    // We will copy a new entry in new blobfile.
                    auto new_entry = record_last->entry;
                    new_entry.file_id += 1;
                    new_entry.offset = epoch_offset;
                    epoch_offset += buff_size;
                    seq_entries.emplace_back(std::make_tuple(seq_start, 1, new_entry));
                }
                seq_start++;
            }

            page_storage->page_directory.apply(std::move(edit));
            wb.clear();
        }
    }

protected:
    FileProviderPtr file_provider;
    std::unique_ptr<StoragePathPool> path_pool;
    PageStorage::Config config;
    std::shared_ptr<PageStorageImpl> page_storage;

    std::list<PageDirectorySnapshotPtr> snapshots_holder;
    size_t fixed_test_buff_size = 1024;

    size_t epoch_offset = 0;
};

TEST_F(PageStorageTest, TestfullGC)
{
    /**
     * PS. `v` means `sequence`, `e` means `epoch`
     * before GC =>
     *   pageid: 50
     *   blobfile_id : 0
     *   entries: [v18-e1, v19-e1, v20-e1]
     *   valid_rate: 0.1
     *   total size: 20 * entries
     *   lowest_seq: 18
     * after GC =>
     *   pageid : 50
     *   blobfile_id(change to read only): 0
     *   entries: [v18-e1, v19-e1, v20-e1]
     *   blobfile change to read only
     *   blob file total size: 20 * entries
     *   -----
     *   pageid : 50
     *   blobfile_id: 1
     *   entries: [v18-e2, v19-e2, v20-e2]
     *   blob file total size: 3 * entries + 1 * anonymous entry
     */

    PageId page_id = 50;
    size_t buf_size = fixed_test_buff_size;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(17);

    // put v13
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 18, false, true);
    auto snapshot_holder = page_storage->page_directory.createSnapshot();

    // put v14-v15
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 2, exp_seq_entries, 19, false, true);

    // do full gc
    page_storage->gc(true, nullptr, nullptr);

    auto & blob_stats = page_storage->blob_store.blob_stats;
    ASSERT_EQ(blob_stats.stats_map.size(), 2);

    auto it = blob_stats.stats_map.begin();
    auto & stat_0 = *it;
    auto & stat_1 = *++it;

    // Verify BlobStats is corrent after gc
    ASSERT_EQ(stat_0->sm_total_size, 20 * buf_size);
    ASSERT_EQ(stat_0->sm_valid_rate, 0.2);

    ASSERT_EQ(stat_1->sm_total_size, 4 * buf_size);
    ASSERT_EQ(stat_1->sm_valid_rate, 1);

    // Verify MVCC is corrent after gc

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, page_storage->page_directory, page_id);
}


TEST_F(PageStorageTest, WriteRead)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    DB::Page page0 = page_storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageTest, WriteMultipleBatchRead)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    DB::Page page0 = page_storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageTest, WriteReadAfterGc)
try
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
        page_storage->write(std::move(batch));
    }
    // repeated put page1
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(pid, 0, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    {
        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = page_storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

    page_storage->gc();

    {
        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = page_storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }
}
CATCH

// TBD : will be failed....
TEST_F(PageStorageTest, WriteReadGcExternalPage)
try
{
    WriteBatch batch;
    {
        batch.putExternal(0, 0);
        batch.putRefPage(1, 0);
        batch.putExternal(1024, 0);
        page_storage->write(std::move(batch));
    }

    size_t times_remover_called = 0;

    PageStorage::ExternalPagesScanner scanner = []() -> PageStorage::PathAndIdsVec {
        return {};
    };
    PageStorage::ExternalPagesRemover remover
        = [&times_remover_called](const PageStorage::PathAndIdsVec &, const std::set<PageId> & normal_page_ids) -> void {
        times_remover_called += 1;
        ASSERT_EQ(normal_page_ids.size(), 2UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
        EXPECT_GT(normal_page_ids.count(1024), 0UL);
    };
    page_storage->registerExternalPagesCallbacks(scanner, remover);
    {
        SCOPED_TRACE("fist gc");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 1UL);
    }

    auto snapshot = page_storage->getSnapshot();

    {
        WriteBatch batch;
        batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        batch.delPage(1); // free ref 1 -> 0
        batch.delPage(1024); // free normal page 1024
        page_storage->write(std::move(batch));
    }

    {
        SCOPED_TRACE("gc with snapshot");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 2UL);
    }

    {
        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), 0UL);
        ASSERT_EQ(page0.page_id, 0UL);

        DB::Page page2 = page_storage->read(2);
        ASSERT_EQ(page2.data.size(), 0UL);
        ASSERT_EQ(page2.page_id, 2UL);
    }

    snapshot.reset();
    remover = [&times_remover_called](const PageStorage::PathAndIdsVec &, const std::set<PageId> & normal_page_ids) -> void {
        times_remover_called += 1;
        ASSERT_EQ(normal_page_ids.size(), 1UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
    };
    page_storage->registerExternalPagesCallbacks(scanner, remover);
    {
        SCOPED_TRACE("gc with snapshot released");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 3UL);
    }
}
CATCH

} // namespace PS::V3::tests
} // namespace DB