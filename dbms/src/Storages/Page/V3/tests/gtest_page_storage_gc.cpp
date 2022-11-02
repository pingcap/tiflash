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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/Ctl.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/tests/gtest_page_storage.h>
#include <common/defines.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>

namespace DB
{
namespace FailPoints
{
extern const char force_ps_wal_compact[];
}
namespace PS::V3::tests
{

struct TestParam
{
    bool keep_snap;
    bool has_ref;

    explicit TestParam(const std::tuple<bool, bool> & t)
        : keep_snap(std::get<0>(t))
        , has_ref(std::get<1>(t))
    {
    }
};

class PageStorageFullGCTestWithParam
    : public PageStorageTest
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
public:
    PageStorageFullGCTestWithParam()
        : test_param(GetParam())
    {
    }

    void SetUp() override
    {
        PageStorageTest::SetUp();
    }

protected:
    TestParam test_param;
};

TEST_P(PageStorageFullGCTestWithParam, DontMoveDeletedPageId)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageId page_id1 = 101;
    PageId ref_page_id = 102;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        if (test_param.has_ref)
            batch.putRefPage(ref_page_id, page_id1);
        page_storage->write(std::move(batch));
    }

    // If the page id is logically delete before `tryDumpSnapshot`, then
    // full gc won't rewrite
    {
        WriteBatch batch;
        batch.delPage(page_id1);
        if (test_param.has_ref)
            batch.delPage(ref_page_id);
        page_storage->write(std::move(batch));
    }

    {
        PageStorageSnapshotPtr snap;
        if (test_param.keep_snap)
            snap = page_storage->getSnapshot("");

        // let's compact the WAL logs
        auto done_snapshot = page_storage->page_directory->tryDumpSnapshot(nullptr, nullptr, /* force */ true);
        ASSERT_TRUE(done_snapshot);

        // let's try full gc, this will not trigger full gc
        auto done_full_gc = page_storage->gcImpl(true, nullptr, nullptr);
        ASSERT_FALSE(done_full_gc);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    Group,
    PageStorageFullGCTestWithParam,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool()));


TEST_F(PageStorageTest, DeletePageIdAfterWALCompact)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageId page_id1 = 101;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        page_storage->write(std::move(batch));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_ps_wal_compact);
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::doGC_fullGC");
    auto th_gc = std::async([&]() {
        auto done_full_gc = page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
        ASSERT_FALSE(done_full_gc);
    });
    // let's compact the WAL logs
    sp_gc.waitAndPause();

    // If page is logically delete between `tryDumpSnapshot` and `fullGC`, then
    // we should able to handle the "upsert after delete" on disk
    {
        WriteBatch batch;
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }

    // let's try full gc, this will not trigger full gc
    sp_gc.next();
    th_gc.get();

    // wal compact again
    page_storage->page_directory->tryDumpSnapshot(nullptr, nullptr, true);

    LOG_INFO(log, "close and restore WAL from disk");
    page_storage.reset();

    // There is no any entry on wal log-files
    auto [wal, reader] = WALStore::create(String(NAME), file_provider, delegator, WALConfig::from(new_config));
    UNUSED(wal);
    size_t num_entries_on_wal = 0;
    while (reader->remained())
    {
        auto s = reader->next();
        if (s.has_value())
        {
            auto e = ser::deserializeFrom(s.value());
            num_entries_on_wal += e.size();
            EXPECT_TRUE(e.empty());
        }
    }
    ASSERT_EQ(num_entries_on_wal, 0);
}
CATCH

TEST_F(PageStorageTest, DeleteRefPageIdAfterWALCompact)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageId page_id1 = 101;
    PageId ref_page_id = 102;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putRefPage(ref_page_id, page_id1);
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_ps_wal_compact);
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::doGC_fullGC");
    auto th_gc = std::async([&]() {
        auto done_full_gc = page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
        ASSERT_TRUE(done_full_gc); // note that full gc is executed
    });
    // let's compact the WAL logs
    sp_gc.waitAndPause();

    // If page is logically delete between `tryDumpSnapshot` and `fullGC`, then
    // we should able to handle the "upsert after delete" on disk
    {
        WriteBatch batch;
        batch.delPage(ref_page_id);
        page_storage->write(std::move(batch));
    }

    // let's try full gc, unlike `DeletePageIdAfterWALCompact`,
    // this ** will ** trigger full gc
    sp_gc.next();
    th_gc.get();

    // wal compact again !!!! FIXME: this will throw exception
    page_storage->page_directory->tryDumpSnapshot(nullptr, nullptr, true);

    LOG_INFO(log, "close and restore WAL from disk");
    page_storage.reset();

    // There is no any entry on wal log-files
    auto [wal, reader] = WALStore::create(String(NAME), file_provider, delegator, WALConfig::from(new_config));
    UNUSED(wal);
    size_t num_entries_on_wal = 0;
    while (reader->remained())
    {
        auto s = reader->next();
        if (s.has_value())
        {
            auto e = ser::deserializeFrom(s.value());
            num_entries_on_wal += e.size();
            EXPECT_TRUE(e.empty());
        }
    }
    ASSERT_EQ(num_entries_on_wal, 0);
}
CATCH

} // namespace PS::V3::tests
} // namespace DB
