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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/Ctl.h>
#include <Common/SyncPoint/ScopeGuard.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/tests/gtest_page_storage.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <common/defines.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char force_ps_wal_compact[];
extern const char pause_before_full_gc_prepare[];
} // namespace FailPoints
namespace PS::V3::tests
{

struct FullGCParam
{
    bool keep_snap;
    bool has_ref;

    explicit FullGCParam(const std::tuple<bool, bool> & t)
        : keep_snap(std::get<0>(t))
        , has_ref(std::get<1>(t))
    {}
};

class PageStorageFullGCTest
    : public PageStorageTest
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
public:
    PageStorageFullGCTest()
        : test_param(GetParam())
    {}

    void SetUp() override { PageStorageTest::SetUp(); }

protected:
    FullGCParam test_param;
};

TEST_P(PageStorageFullGCTest, DontMoveDeletedPageId)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageIdU64 page_id1 = 101;
    PageIdU64 ref_page_id = 102;
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
        auto done_snapshot = page_storage->page_directory->tryDumpSnapshot(nullptr, /* force */ true);
        ASSERT_TRUE(done_snapshot);

        // let's try full gc, this will not trigger full gc
        auto done_full_gc = page_storage->gcImpl(true, nullptr, nullptr);
        ASSERT_FALSE(done_full_gc);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(Group, PageStorageFullGCTest, ::testing::Combine(::testing::Bool(), ::testing::Bool()));

///////
/// PageStorageFullGCConcurrentTest
///////

enum class DeleteTiming
{
    BeforeFullGCPrepare = 1,
    BeforeFullGCCommit,
    AfterFullGCCommit,
};
class PageStorageFullGCConcurrentTest
    : public PageStorageTest
    , public testing::WithParamInterface<DeleteTiming>
{
public:
    PageStorageFullGCConcurrentTest()
        : timing(GetParam())
    {}

    void SetUp() override { PageStorageTest::SetUp(); }

    SyncPointScopeGuard getSyncPoint() const
    {
        switch (timing)
        {
        case DeleteTiming::BeforeFullGCPrepare:
            return SyncPointCtl::enableInScope("before_PageStorageImpl::doGC_fullGC_prepare");
        case DeleteTiming::BeforeFullGCCommit:
            return SyncPointCtl::enableInScope("before_PageStorageImpl::doGC_fullGC_commit");
        case DeleteTiming::AfterFullGCCommit:
            return SyncPointCtl::enableInScope("after_PageStorageImpl::doGC_fullGC_commit");
        }
    }

    bool expectFullGCExecute() const
    {
        // - If delete happen before prepare, then nothing need to be rewrite.
        // - If delete happen before commit, the page is logically delete.
        //   We should able to handle the "upsert after delete" on disk.
        switch (timing)
        {
        case DeleteTiming::BeforeFullGCPrepare:
            return false;
        case DeleteTiming::BeforeFullGCCommit:
            return true;
        case DeleteTiming::AfterFullGCCommit:
            return true;
        }
    }

protected:
    DeleteTiming timing;
};

TEST_P(PageStorageFullGCConcurrentTest, DeletePage)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageIdU64 page_id1 = 101;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        page_storage->write(std::move(batch));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_ps_wal_compact);
    auto sp_gc = getSyncPoint();
    auto th_gc = std::async([&]() {
        auto done_full_gc = page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
        ASSERT_EQ(expectFullGCExecute(), done_full_gc);
    });
    // let's compact the WAL logs
    sp_gc.waitAndPause();

    {
        // the delete timing is decide by `sp_gc`
        WriteBatch batch;
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }

    // let's try full gc
    sp_gc.next();
    th_gc.get();

    // wal compact again
    page_storage->page_directory->tryDumpSnapshot(nullptr, true);

    LOG_INFO(log, "close and restore WAL from disk");
    page_storage.reset();

    // There is no any entry on wal log-files
    auto [wal, reader] = WALStore::create(String(NAME), file_provider, delegator, WALConfig::from(new_config));
    UNUSED(wal);
    size_t num_entries_on_wal = 0;
    while (reader->remained())
    {
        auto [_, s] = reader->next();
        if (s.has_value())
        {
            auto e = u128::Serializer::deserializeFrom(s.value(), nullptr);
            num_entries_on_wal += e.size();
            EXPECT_TRUE(e.empty());
        }
    }
    ASSERT_EQ(num_entries_on_wal, 0);
}
CATCH

TEST_P(PageStorageFullGCConcurrentTest, DeleteRefPage)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageIdU64 page_id1 = 101;
    PageIdU64 ref_page_id2 = 102;
    PageIdU64 ref_page_id3 = 103;
    PageIdU64 ref_page_id4 = 104;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        batch.putRefPage(ref_page_id2, page_id1);
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putRefPage(ref_page_id3, ref_page_id2);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putRefPage(ref_page_id4, ref_page_id3);
        page_storage->write(std::move(batch));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_ps_wal_compact);
    {
        auto sp_gc = getSyncPoint();
        auto th_gc = std::async([&]() {
            auto done_full_gc = page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
            ASSERT_EQ(expectFullGCExecute(), done_full_gc);
        });
        // let's compact the WAL logs
        sp_gc.waitAndPause();

        {
            // the delete timing is decide by `sp_gc`
            WriteBatch batch;
            batch.delPage(ref_page_id2);
            batch.delPage(ref_page_id3);
            batch.delPage(ref_page_id4);
            page_storage->write(std::move(batch));
        }

        // let's try full gc
        sp_gc.next();
        th_gc.get();
    }

    // wal compact again
    page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
    page_storage->page_directory->tryDumpSnapshot(nullptr, true);

    LOG_INFO(log, "close and restore WAL from disk");
    page_storage.reset();

    // There is no any entry on wal log-files
    auto [wal, reader] = WALStore::create(String(NAME), file_provider, delegator, WALConfig::from(new_config));
    UNUSED(wal);
    size_t num_entries_on_wal = 0;
    while (reader->remained())
    {
        auto [_, s] = reader->next();
        if (s.has_value())
        {
            auto e = u128::Serializer::deserializeFrom(s.value(), nullptr);
            num_entries_on_wal += e.size();
            EXPECT_TRUE(e.empty());
        }
    }
    ASSERT_EQ(num_entries_on_wal, 0);
}
CATCH

INSTANTIATE_TEST_CASE_P(
    DeleteTiming,
    PageStorageFullGCConcurrentTest,
    ::testing::Values(
        DeleteTiming::BeforeFullGCPrepare,
        DeleteTiming::BeforeFullGCCommit,
        DeleteTiming::AfterFullGCCommit //
        ),
    [](const ::testing::TestParamInfo<PageStorageFullGCConcurrentTest::ParamType> & param) {
        return String(magic_enum::enum_name(param.param));
    });

///////
/// PageStorageFullGCConcurrentTest2
///////

// The full GC start timing when run concurrently
// with creating ref page
enum class StartTiming
{
    BeforeCreateRef = 1,
    BeforeIncrRefCount,
    AfterIncrRefCount,
};
class PageStorageFullGCConcurrentTest2
    : public PageStorageTest
    , public testing::WithParamInterface<StartTiming>
{
public:
    PageStorageFullGCConcurrentTest2()
        : timing(GetParam())
    {}

    void SetUp() override { PageStorageTest::SetUp(); }

    SyncPointScopeGuard getSyncPoint() const
    {
        switch (timing)
        {
        case StartTiming::BeforeCreateRef:
            return SyncPointCtl::enableInScope("before_PageDirectory::applyRefEditRecord_create_ref");
        case StartTiming::BeforeIncrRefCount:
            return SyncPointCtl::enableInScope("before_PageDirectory::applyRefEditRecord_incr_ref_count");
        case StartTiming::AfterIncrRefCount:
            return SyncPointCtl::enableInScope("after_PageDirectory::applyRefEditRecord_incr_ref_count");
        }
    }

protected:
    StartTiming timing;
};

TEST_P(PageStorageFullGCConcurrentTest2, CreateRefPage)
try
{
    // always pick all blob file for full gc
    PageStorageConfig new_config;
    new_config.blob_heavy_gc_valid_rate = 1.0;
    page_storage->reloadSettings(new_config);

    PageIdU64 page_id1 = 101;
    PageIdU64 ref_page_id2 = 102;
    PageIdU64 ref_page_id3 = 103;
    PageIdU64 ref_page_id4 = 104;
    {
        WriteBatch batch;
        batch.putPage(page_id1, default_tag, getDefaultBuffer(), buf_sz);
        batch.putRefPage(ref_page_id2, page_id1);
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putRefPage(ref_page_id3, ref_page_id2);
        batch.delPage(ref_page_id2);
        page_storage->write(std::move(batch));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_ps_wal_compact);
    FailPointHelper::enableFailPoint(FailPoints::pause_before_full_gc_prepare);
    auto sp_full_gc_prepare = SyncPointCtl::enableInScope("before_PageDirectory::getEntriesByBlobIds_id_101");
    auto th_full_gc = std::async([&]() {
        // let's try full gc, it should rewrite the ref-pages into normal pages
        auto done_full_gc = page_storage->gcImpl(/* not_skip */ true, nullptr, nullptr);
        EXPECT_EQ(true, done_full_gc);
    });
    // let's begin full gc and stop before collecting for id=101
    sp_full_gc_prepare.waitAndPause();

    auto sp_foreground_write = getSyncPoint();
    auto th_foreground_write = std::async([&]() {
        WriteBatch batch;
        batch.putRefPage(ref_page_id4, ref_page_id3);
        page_storage->write(std::move(batch));
    });
    // let's create the ref page with sync_point
    sp_foreground_write.waitAndPause();


    // let's continue the full gc
    sp_full_gc_prepare.next();
    // ... then continue the foreground write
    sp_foreground_write.next();
    sp_full_gc_prepare.disable();
    sp_foreground_write.disable();

    // finish
    th_full_gc.get();
    th_foreground_write.get();

    // wal compact again
    page_storage->page_directory->tryDumpSnapshot(nullptr, true);

    LOG_INFO(log, "close and restore WAL from disk");
    page_storage.reset();

    // The living ref-pages (id3, id4) are rewrite into
    // normal pages.
    auto [wal, reader] = WALStore::create(String(NAME), file_provider, delegator, WALConfig::from(new_config));
    UNUSED(wal);
    size_t num_entries_on_wal = 0;
    bool exist_id3_normal_entry = false;
    bool exist_id4_normal_entry = false;
    while (reader->remained())
    {
        auto [_, s] = reader->next();
        if (s.has_value())
        {
            auto e = u128::Serializer::deserializeFrom(s.value(), nullptr);
            num_entries_on_wal += e.size();
            for (const auto & r : e.getRecords())
            {
                if (r.type == EditRecordType::VAR_ENTRY)
                {
                    if (r.page_id.low == ref_page_id3)
                        exist_id3_normal_entry = true;
                    else if (r.page_id.low == ref_page_id4)
                        exist_id4_normal_entry = true;
                }
                LOG_INFO(log, "{}", r);
            }
        }
    }
    ASSERT_TRUE(exist_id3_normal_entry);
    ASSERT_TRUE(exist_id4_normal_entry);
    ASSERT_EQ(num_entries_on_wal, 2);
}
CATCH

INSTANTIATE_TEST_CASE_P(
    StartTiming,
    PageStorageFullGCConcurrentTest2,
    ::testing::Values(
        StartTiming::BeforeCreateRef,
        StartTiming::BeforeIncrRefCount,
        StartTiming::AfterIncrRefCount //
        ),
    [](const ::testing::TestParamInfo<PageStorageFullGCConcurrentTest2::ParamType> & param) {
        return String(magic_enum::enum_name(param.param));
    });

} // namespace PS::V3::tests
} // namespace DB
