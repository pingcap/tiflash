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

#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB
{
namespace PS::universal::tests
{
class UniPageStorageTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, config, file_provider);
        page_storage->restore();

        for (size_t i = 0; i < buf_sz; ++i)
        {
            c_buff[i] = i % 0xff;
        }

        log = Logger::get("UniPageStorageTest");
    }

    void reload() { page_storage = reopenWithConfig(config); }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider);
        storage->restore();
        return storage;
    }

protected:
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;

    static constexpr size_t buf_sz = 1024;
    char c_buff[buf_sz] = {};
};

TEST_F(UniPageStorageTest, WriteRead)
try
{
    const String prefix = "aaa";
    const UInt64 tag = 0;
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 21),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 200),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        page_storage->write(std::move(wb));
    }

    DB::Page page0 = page_storage->read(UniversalPageIdFormat::toFullPageId(prefix, 0));
    ASSERT_TRUE(page0.isValid());
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(UniversalPageIdFormat::toFullPageId(prefix, 21));
    ASSERT_TRUE(page1.isValid());
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 21UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page2 = page_storage->read(UniversalPageIdFormat::toFullPageId(prefix, 500), nullptr, {}, false);
    ASSERT_TRUE(!page2.isValid());
}
CATCH

TEST_F(UniPageStorageTest, Traverse)
{
    const String prefix1 = "aaa";
    const String prefix2 = "bbbb";
    const String prefix3 = "zzzzzzzzz";
    const UInt64 tag = 0;
    const size_t write_count = 100;
    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix1, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix2, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.hasPrefix(prefix1));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix1, checker, {});
        ASSERT_EQ(read_count, write_count);
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.hasPrefix(prefix2));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix2, checker, {});
        ASSERT_EQ(read_count, write_count);
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.hasPrefix(prefix3));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix3, checker, {});
        ASSERT_EQ(read_count, 0);
    }
}

TEST_F(UniPageStorageTest, TraverseWithSnap)
{
    const String prefix1 = "aaa";
    const UInt64 tag = 0;
    const size_t write_count = 100;
    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix1, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    auto snap = page_storage->getSnapshot("UniPageStorageTest");
    // write more after create snap
    {
        UniversalWriteBatch wb;
        for (size_t i = write_count; i < 2 * write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix1, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.hasPrefix(prefix1));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix1, checker, snap);
        ASSERT_EQ(read_count, write_count);
    }

    // delete some pages
    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.delPage(UniversalPageIdFormat::toFullPageId(prefix1, i));
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.hasPrefix(prefix1));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix1, checker, snap);
        ASSERT_EQ(read_count, write_count);
    }
}

TEST_F(UniPageStorageTest, GetMaxIdWithPrefix)
{
    const String prefix1 = UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, /*ns_id*/ 100);
    const String prefix2 = UniversalPageIdFormat::toFullPrefix(/*keyspace_id*/ 100, StorageType::Data, /*ns_id*/ 300);
    const String prefix3 = UniversalPageIdFormat::toFullPrefix(/*keyspace_id*/ 300, StorageType::Data, /*ns_id*/ 700);
    const String prefix4 = "aaa";
    const String prefix5 = "bbb";
    const UInt64 tag = 0;
    const size_t write_count = 100;
    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix1, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix2, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix3, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix4, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
            wb.putPage(
                UniversalPageIdFormat::toFullPageId(prefix5, i),
                tag,
                std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
                buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    ASSERT_EQ(page_storage->getMaxIdAfterRestart(), 0);

    reload();
    ASSERT_EQ(page_storage->getMaxIdAfterRestart(), write_count - 1);
}

TEST_F(UniPageStorageTest, GetLowerBound)
{
    const String prefix = "aaa";
    UInt64 tag = 0;
    {
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 5),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 4;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 7),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 5;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 6),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 6;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 10),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 7;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 9),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);

        page_storage->write(std::move(wb));
    }

    RaftDataReader reader(*page_storage);
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 4);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(result_id.has_value());
        ASSERT_EQ(*result_id, UniversalPageIdFormat::toFullPageId(prefix, 5));
    }
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 5);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(result_id.has_value());
        ASSERT_EQ(*result_id, UniversalPageIdFormat::toFullPageId(prefix, 5));
    }
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 7);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(result_id.has_value());
        ASSERT_EQ(*result_id, UniversalPageIdFormat::toFullPageId(prefix, 7));
    }
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 8);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(result_id.has_value());
        ASSERT_EQ(*result_id, UniversalPageIdFormat::toFullPageId(prefix, 9));
    }
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 10);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(result_id.has_value());
        ASSERT_EQ(*result_id, UniversalPageIdFormat::toFullPageId(prefix, 10));
    }
    {
        auto target_id = UniversalPageIdFormat::toFullPageId(prefix, 11);
        auto result_id = reader.getLowerBound(target_id);
        ASSERT_TRUE(!result_id.has_value());
    }
}

TEST_F(UniPageStorageTest, Scan)
{
    UInt64 tag = 0;
    const UInt64 region_id = 100;
    const String region_prefix = UniversalPageIdFormat::toFullRaftLogPrefix(region_id);
    // write some raft related data
    {
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(region_prefix, 10),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 4;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(region_prefix, 15),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 5;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(region_prefix, 18),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 6;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(region_prefix, 20),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 7;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(region_prefix, 25),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);

        page_storage->write(std::move(wb));
    }

    // write some non raft data
    {
        const String prefix = "aaa";
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 10),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 4;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 15),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 5;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 18),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 6;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 20),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 7;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 25),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);

        page_storage->write(std::move(wb));
    }

    RaftDataReader reader(*page_storage);
    {
        auto start = UniversalPageIdFormat::toFullPageId(region_prefix, 15);
        auto end = UniversalPageIdFormat::toFullPageId(region_prefix, 25);
        size_t count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            UNUSED(page);
            ASSERT_EQ(UniversalPageIdFormat::getFullPrefix(page_id), region_prefix);
            count++;
        };
        reader.traverse(start, end, checker);
        ASSERT_EQ(count, 3);
    }

    {
        auto start = UniversalPageIdFormat::toFullPageId(region_prefix, 15);
        const auto * end = "";
        size_t count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            UNUSED(page);
            ASSERT_EQ(UniversalPageIdFormat::getFullPrefix(page_id), region_prefix);
            count++;
        };
        reader.traverse(start, end, checker);
        ASSERT_EQ(count, 4);
    }
}

TEST_F(UniPageStorageTest, OnlyScanRaftLog)
{
    UInt64 tag = 0;
    const UInt64 region_id = 100;
    const String region_prefix = UniversalPageIdFormat::toFullRaftLogPrefix(region_id);
    // write some raft related data
    {
        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        PS::V3::CheckpointLocation loc{
            .data_file_id = std::make_shared<String>("hello"),
            .offset_in_file = 0,
            .size_in_file = 0};
        wb.putRemotePage(UniversalPageIdFormat::toFullPageId(region_prefix, 10), tag, 0, loc, {});
        wb.putRemotePage(UniversalPageIdFormat::toFullPageId(region_prefix, 15), tag, 0, loc, {});
        wb.putRemotePage(UniversalPageIdFormat::toFullPageId(region_prefix, 18), tag, 0, loc, {});
        wb.putRemotePage(UniversalPageIdFormat::toFullRaftLogScanEnd(region_id), tag, 0, loc, {});

        page_storage->write(std::move(wb));
    }

    RaftDataReader reader(*page_storage);
    {
        size_t count = 0;
        auto checker
            = [&](const UniversalPageId & page_id, PageSize page_size, const PS::V3::CheckpointLocation & location) {
                  UNUSED(page_size, location);
                  ASSERT_EQ(UniversalPageIdFormat::getFullPrefix(page_id), region_prefix);
                  count++;
              };
        reader.traverseRemoteRaftLogForRegion(
            region_id,
            [](size_t) { return true; },
            checker);
        ASSERT_EQ(count, 3);
    }
}

TEST(UniPageStorageIdTest, UniversalPageId)
{
    {
        auto u_id = UniversalPageIdFormat::toFullPageId("aaa", 100);
        ASSERT_EQ(UniversalPageIdFormat::getU64ID(u_id), 100);
        ASSERT_EQ(UniversalPageIdFormat::getFullPrefix(u_id), "aaa");
    }

    {
        const auto * u_id = "z";
        ASSERT_EQ(UniversalPageIdFormat::getU64ID(u_id), 0);
        ASSERT_EQ(UniversalPageIdFormat::getFullPrefix(u_id), "z");
    }
}

TEST(UniPageStorageIdTest, UniversalPageIdMemoryTrace)
{
    auto prim_mem = PS::PageStorageMemorySummary::uni_page_id_bytes.load();
    {
        auto u_id = UniversalPageIdFormat::toFullPageId("aaa", 100);
        auto page1_mem = PS::PageStorageMemorySummary::uni_page_id_bytes.load();
        auto ps = page1_mem - prim_mem;
        // copy construct
        auto u_id_cpy = u_id;
        ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem + ps * 2);
        // move assignment
        UniversalPageId u_id_mv = UniversalPageIdFormat::toFullPageId("aaa", 100);
        ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem + ps * 3);
        u_id_mv = std::move(u_id_cpy);
        ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem + ps * 2);
        // copy assignment
        UniversalPageId u_id_cpy2 = UniversalPageIdFormat::toFullPageId("aaa", 100);
        u_id_cpy2 = u_id_mv;
        ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem + ps * 3);
        // move construct
        auto u_id_mv2 = std::move(u_id_cpy2);
        ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem + ps * 3);
    }
    ASSERT_EQ(PS::PageStorageMemorySummary::uni_page_id_bytes.load(), prim_mem);
}


TEST(UniPageStorageIdTest, UniversalWriteBatchMemory)
{
    const String prefix = "aaa";
    const UInt64 tag = 0;
    static constexpr size_t buf_sz = 1024;
    char c_buff[buf_sz] = {};
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }
    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 1);
    }
    ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 0);
    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        UniversalWriteBatch wb2 = std::move(wb);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 1);
    }
    ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 0);
    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        UniversalWriteBatch wb2;
        wb2.merge(wb);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 2);
    }
    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        wb.clear();
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 0);
    }
    {
        UniversalWriteBatch wb;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 0),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 1);
        UniversalWriteBatch wb2;
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 1),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        wb.putPage(
            UniversalPageIdFormat::toFullPageId(prefix, 2),
            tag,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 3);
        wb.swap(wb2);
        ASSERT_EQ(PageStorageMemorySummary::universal_write_count.load(), 3);
    }
}

} // namespace PS::universal::tests
} // namespace DB
