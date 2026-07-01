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
#include <Common/TiFlashMetrics.h>
#include <DataStreams/OneBlockInputStream.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileLocalStaging.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/Segment_fwd.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <ctime>
#include <memory>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char force_use_dmfile_format_v3[];
} // namespace FailPoints
namespace DM
{
namespace tests
{
namespace
{
// Keep in sync with gtests_dbms_main.cpp.
constexpr size_t s3_file_cache_pool_max_threads = 20;
constexpr size_t s3_file_cache_pool_max_free_threads = 10;
constexpr size_t s3_file_cache_pool_queue_size = 1000;

void reinitS3FileCachePool()
{
    S3FileCachePool::initialize(
        s3_file_cache_pool_max_threads,
        s3_file_cache_pool_max_free_threads,
        s3_file_cache_pool_queue_size);
}
} // namespace

class SegmentTestS3 : public DB::base::TiFlashStorageTestBasic
{
public:
    SegmentTestS3() = default;

public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        FailPointHelper::enableFailPoint(FailPoints::force_use_dmfile_format_v3);
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        TiFlashStorageTestBasic::SetUp();
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        global_context.getTMTContext().initS3GCManager(nullptr);
        if (global_context.getSharedContextDisagg()->remote_data_store == nullptr)
        {
            already_initialize_data_store = false;
            global_context.getSharedContextDisagg()->initRemoteDataStore(
                global_context.getFileProvider(),
                /*s3_enabled*/ true);
            ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store != nullptr);
        }
        else
        {
            already_initialize_data_store = true;
        }
        if (global_context.getWriteNodePageStorage() == nullptr)
        {
            already_initialize_write_ps = false;
            orig_mode = global_context.getPageStorageRunMode();
            global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
            global_context.tryReleaseWriteNodePageStorageForTest();
            global_context.initializeWriteNodePageStorageIfNeed(global_context.getPathPool());
        }
        else
        {
            already_initialize_write_ps = true;
        }
        table_columns = std::make_shared<ColumnDefines>();

        auto kvstore = db_context->getTMTContext().getKVStore();
        {
            auto meta_store = metapb::Store{};
            meta_store.set_id(100);
            kvstore->setStore(meta_store);
        }

        segment = buildFirstSegment();
        ASSERT_EQ(segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    void TearDown() override
    {
        shutdownWriteFileCache();

        FailPointHelper::disableFailPoint(FailPoints::force_use_dmfile_format_v3);
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        if (!already_initialize_data_store)
        {
            global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        }
        if (!already_initialize_write_ps)
        {
            global_context.setPageStorageRunMode(orig_mode);
        }
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

protected:
    SegmentPtr buildFirstSegment(
        const ColumnDefinesPtr & pre_define_columns = {},
        DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        page_id_allocator = std::make_shared<GlobalPageIdAllocator>();
        storage_pool = std::make_shared<StoragePool>(
            *db_context,
            NullspaceID,
            ns_id,
            *storage_path_pool,
            page_id_allocator,
            "test.t1");
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        return Segment::newSegment(
            Logger::get(),
            *dm_context,
            table_columns,
            RowKeyRange::newAll(false, 1),
            DELTA_MERGE_FIRST_SEGMENT_ID,
            0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns = *columns;

        dm_context = DMContext::createUnique(
            *db_context,
            storage_path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContext & dmContext() { return *dm_context; }

    void initWriteFileCache()
    {
        StorageRemoteCacheConfig file_cache_config{
            .dir = fmt::format("{}/wn_filecache", getTemporaryPath()),
            .capacity = 1 * 1000 * 1000 * 1000,
        };
        UInt16 vcores = 8;
        FileCache::initialize(
            db_context->getGlobalContext().getPathCapacity(),
            file_cache_config,
            vcores,
            db_context->getGlobalContext().getIORateLimiter());
    }

    static void shutdownWriteFileCache()
    {
        if (FileCache::instance() == nullptr)
            return;
        FileCache::shutdown();
        // FileCache::shutdown() tears down S3FileCachePool; restore it for other tests.
        reinitS3FileCachePool();
    }

    void setDtEnableWriteFileCache(bool enabled)
    {
        // DMContext stores global context, not session context.
        db_context->getGlobalContext().getSettingsRef().dt_enable_write_filecache = enabled;
    }

    static double stagingAttempt() { return GET_METRIC(tiflash_storage_write_filecache_staging, type_attempt).Value(); }

    static bool fileCacheHasMergedFile()
    {
        auto * file_cache = FileCache::instance();
        if (file_cache == nullptr)
            return false;
        for (const auto & file_seg : file_cache->getAll())
        {
            if (file_seg->getLocalFileName().contains(".merged"))
                return true;
        }
        return false;
    }

    void writeRows(size_t start, size_t end)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(start, end, false);
        segment->write(dmContext(), std::move(block), /* flush */ true);
    }

    void assertRemoteStableHasMergedSubFiles(const SegmentPtr & seg)
    {
        const auto files = seg->stable->getDMFiles();
        ASSERT_FALSE(files.empty());
        const auto & dmfile = files[0];
        ASSERT_TRUE(dmfile->useMetaV2());
        ASSERT_TRUE(dmfile->path().starts_with("s3://"));
        const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
        ASSERT_NE(dmfile_meta, nullptr);
        ASSERT_FALSE(dmfile_meta->merged_sub_file_infos.empty());
        const auto objects = collectMetaV2MergedFilesForLocalRead(
            dmfile,
            *table_columns,
            Logger::get("SegmentTestS3"),
            "SegmentTestS3");
        ASSERT_FALSE(objects.empty());
    }

    /// Create a remote MetaV2 stable on S3, then leave delta data for a follow-up mergeDelta.
    void writeRemoteStableWithDelta(size_t stable_end, size_t delta_end)
    {
        writeRows(0, stable_end);
        segment = segment->mergeDelta(dmContext(), tableColumns());
        if (delta_end > stable_end)
            writeRows(stable_end, delta_end);
    }

    SegmentPtr mergeDeltaToRemoteStable()
    {
        writeRemoteStableWithDelta(100, 100);
        return segment;
    }

    size_t readRows(const SegmentPtr & seg)
    {
        auto in = seg->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        return getInputStreamNRows(in);
    }

protected:
    /// all these var lives as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    TableID ns_id = 100;

    // the segment we are going to test
    SegmentPtr segment;

    bool already_initialize_data_store = false;
    bool already_initialize_write_ps = false;
    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;
};

TEST_F(SegmentTestS3, LogicalSplit)
try
{
    const size_t num_rows_write_per_batch = 100;
    {
        // write to segment and flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write_per_batch, false);
        segment->write(dmContext(), std::move(block), true);
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        auto all_dt_file_ids = storage_path_pool->getStableDiskDelegator().getAllRemoteDTFilesForGC();
        ASSERT_EQ(all_dt_file_ids.size(), 2);
    }


    auto [left, right] = segment->split(
        dmContext(),
        tableColumns(),
        /* use a calculated split point */ std::nullopt,
        Segment::SplitMode::Logical);
    ASSERT_TRUE(left != nullptr);
    ASSERT_TRUE(right != nullptr);

    {
        auto all_dt_file_ids = storage_path_pool->getStableDiskDelegator().getAllRemoteDTFilesForGC();
        ASSERT_EQ(all_dt_file_ids.size(), 2);
    }

    std::vector<SegmentPtr> ordered_segments = {left, right};
    segment = Segment::merge(dmContext(), tableColumns(), ordered_segments);

    auto wn_ps = dmContext().global_context.getWriteNodePageStorage();
    wn_ps->gc(/*not_skip*/ true);
    {
        auto valid_external_ids = wn_ps->page_directory->getAliveExternalIds(
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Data, ns_id));
        ASSERT_TRUE(valid_external_ids.has_value());
        ASSERT_EQ(valid_external_ids->size(), 1);
        auto all_dt_file_ids = storage_path_pool->getStableDiskDelegator().getAllRemoteDTFilesForGC();
        ASSERT_EQ(all_dt_file_ids.size(), 3);
    }
}
CATCH

TEST_F(SegmentTestS3, WriteFileCacheDisabledWithFileCache)
try
{
    initWriteFileCache();

    setDtEnableWriteFileCache(false);
    ASSERT_TRUE(FileCache::instance()->getAll().empty());

    const auto attempt_before = stagingAttempt();
    segment = mergeDeltaToRemoteStable();
    ASSERT_EQ(attempt_before, stagingAttempt());
    ASSERT_EQ(100, readRows(segment));
}
CATCH

TEST_F(SegmentTestS3, WriteFileCacheEnabledWithoutFileCache)
try
{
    ASSERT_EQ(FileCache::instance(), nullptr);
    setDtEnableWriteFileCache(true);

    segment = mergeDeltaToRemoteStable();
    ASSERT_EQ(0, stagingAttempt());
    ASSERT_EQ(100, readRows(segment));
}
CATCH

TEST_F(SegmentTestS3, WriteFileCacheMergeDeltaStaging)
try
{
    initWriteFileCache();

    setDtEnableWriteFileCache(true);
    ASSERT_TRUE(FileCache::instance()->getAll().empty());

    // Staging reads the existing remote stable; the first mergeDelta only creates stable.
    writeRemoteStableWithDelta(100, 200);
    assertRemoteStableHasMergedSubFiles(segment);

    const auto attempt_before = stagingAttempt();
    segment = segment->mergeDelta(dmContext(), tableColumns());

    ASSERT_GT(stagingAttempt(), attempt_before);
    ASSERT_TRUE(fileCacheHasMergedFile());
    ASSERT_EQ(200, readRows(segment));
}
CATCH

TEST_F(SegmentTestS3, WriteFileCachePrepareMergeStaging)
try
{
    initWriteFileCache();

    setDtEnableWriteFileCache(true);
    segment = mergeDeltaToRemoteStable();
    assertRemoteStableHasMergedSubFiles(segment);

    auto [left, right] = segment->split(
        dmContext(),
        tableColumns(),
        /*opt_split_at=*/std::nullopt,
        Segment::SplitMode::Logical);
    ASSERT_NE(left, nullptr);
    ASSERT_NE(right, nullptr);

    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(100, 200, false);
        right->write(dmContext(), std::move(block), /* flush */ true);
        right->flushCache(dmContext());
    }

    WriteBatches wbs(*storage_pool);
    auto left_snap = left->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
    auto right_snap = right->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
    ASSERT_NE(left_snap, nullptr);
    ASSERT_NE(right_snap, nullptr);

    const auto attempt_before = stagingAttempt();
    auto merged_stable
        = Segment::prepareMerge(dmContext(), tableColumns(), {left, right}, {left_snap, right_snap}, wbs).stable;
    ASSERT_NE(merged_stable, nullptr);
    ASSERT_GT(stagingAttempt(), attempt_before);
    ASSERT_TRUE(fileCacheHasMergedFile());
}
CATCH

TEST_F(SegmentTestS3, WriteFileCachePhysicalSplitStaging)
try
{
    initWriteFileCache();

    setDtEnableWriteFileCache(true);
    segment = mergeDeltaToRemoteStable();
    assertRemoteStableHasMergedSubFiles(segment);

    WriteBatches wbs(*storage_pool);
    auto segment_snap = segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
    ASSERT_NE(segment_snap, nullptr);

    const auto split_at = RowKeyValue::fromIntHandle(50);
    const auto attempt_before = stagingAttempt();
    auto split_info
        = segment->prepareSplit(dmContext(), tableColumns(), segment_snap, split_at, Segment::SplitMode::Physical, wbs);
    ASSERT_TRUE(split_info.has_value());
    ASSERT_GT(stagingAttempt(), attempt_before);
    ASSERT_TRUE(fileCacheHasMergedFile());
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
