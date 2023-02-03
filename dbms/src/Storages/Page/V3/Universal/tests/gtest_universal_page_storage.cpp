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

#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatch.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>

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
        file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, config, file_provider);
        page_storage->restore();

        for (size_t i = 0; i < buf_sz; ++i)
        {
            c_buff[i] = i % 0xff;
        }

        log = Logger::get("PageStorageTest");
    }

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
        wb.putPage(UniversalPageId::toFullPageId(prefix, 0), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        wb.putPage(UniversalPageId::toFullPageId(prefix, 21), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        wb.putPage(UniversalPageId::toFullPageId(prefix, 200), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        page_storage->write(std::move(wb));
    }

    DB::Page page0 = page_storage->read(UniversalPageId::toFullPageId(prefix, 0));
    ASSERT_TRUE(page0.isValid());
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(UniversalPageId::toFullPageId(prefix, 21));
    ASSERT_TRUE(page1.isValid());
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 21UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page2 = page_storage->read(UniversalPageId::toFullPageId(prefix, 500), nullptr, {}, false);
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
            wb.putPage(UniversalPageId::toFullPageId(prefix1, i), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        UniversalWriteBatch wb;
        for (size_t i = 0; i < write_count; i++)
        {
            c_buff[0] = 10;
            c_buff[1] = i;
            wb.putPage(UniversalPageId::toFullPageId(prefix2, i), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.isPrefix(prefix1));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix1, checker, {});
        ASSERT_EQ(read_count, write_count);
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.isPrefix(prefix2));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix2, checker, {});
        ASSERT_EQ(read_count, write_count);
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.isPrefix(prefix3));
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
            wb.putPage(UniversalPageId::toFullPageId(prefix1, i), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
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
            wb.putPage(UniversalPageId::toFullPageId(prefix1, i), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.isPrefix(prefix1));
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
            wb.delPage(UniversalPageId::toFullPageId(prefix1, i));
        }
        page_storage->write(std::move(wb));
    }

    {
        size_t read_count = 0;
        auto checker = [&](const UniversalPageId & page_id, const DB::Page & page) {
            ASSERT_TRUE(page_id.isPrefix(prefix1));
            ASSERT_TRUE(page.isValid());
            read_count += 1;
        };
        page_storage->traverse(prefix1, checker, snap);
        ASSERT_EQ(read_count, write_count);
    }
}

TEST(UniPageStorageIdTest, UniversalPageId)
{
    {
        auto u_id = UniversalPageId::toFullPageId("aaa", 100);
        ASSERT_EQ(DB::PS::V3::universal::ExternalIdTrait::getU64ID(u_id), 100);
        ASSERT_EQ(DB::PS::V3::universal::ExternalIdTrait::getPrefix(u_id), "aaa");
    }

    {
        auto u_id = "z";
        ASSERT_EQ(DB::PS::V3::universal::ExternalIdTrait::getU64ID(u_id), 0);
        ASSERT_EQ(DB::PS::V3::universal::ExternalIdTrait::getPrefix(u_id), "");
    }
}
} // namespace PS::universal::tests
} // namespace DB
