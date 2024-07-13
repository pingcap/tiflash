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

#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/S3/FileCache.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace DB::DM
{

extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);

}

namespace DB::DM::tests
{

class SegmentReplaceStableData
    : public SegmentTestBasic
    , public testing::WithParamInterface</* unused */ bool>
{
protected:
    void SetUp() override
    {
        storage_version = STORAGE_FORMAT_CURRENT;
        STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V6;
        SegmentTestBasic::SetUp();
    }

    void TearDown() override
    {
        SegmentTestBasic::TearDown();
        STORAGE_FORMAT_CURRENT = storage_version;
    }

    void replaceSegmentStableWithNewMetaValue(PageIdU64 segment_id, String pk_additiona_data)
    {
        // For test purpose, we only replace the additional_data_for_test field
        // of the PK, as the change of the new metadata.

        auto [segment, snapshot] = getSegmentForRead(segment_id);
        RUNTIME_CHECK(segment != nullptr);

        auto files = snapshot->stable->getDMFiles();
        RUNTIME_CHECK(files.size() == 1);

        DMFiles new_dm_files;

        for (auto & file : files)
        {
            auto new_dm_file = DMFile::restore(
                dm_context->db_context.getFileProvider(),
                file->fileId(),
                file->pageId(),
                file->parentPath(),
                DMFileMeta::ReadMode::all(),
                file->metaVersion());

            auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
                .dm_file = new_dm_file,
                .file_provider = dm_context->db_context.getFileProvider(),
                .write_limiter = dm_context->db_context.getWriteLimiter(),
                .path_pool = storage_path_pool,
                .disagg_ctx = dm_context->db_context.getSharedContextDisagg(),
            });
            auto & column_stats = new_dm_file->meta->getColumnStats();
            RUNTIME_CHECK(column_stats.find(::DB::TiDBPkColumnID) != column_stats.end());
            column_stats[::DB::TiDBPkColumnID].additional_data_for_test = pk_additiona_data;

            new_dm_file->meta->bumpMetaVersion();
            iw->finalize();

            new_dm_files.emplace_back(new_dm_file);
        }

        // TODO: Support multiple DMFiles
        auto succeeded = replaceSegmentStableData(segment_id, new_dm_files[0]);
        RUNTIME_CHECK(succeeded);
    }

    UInt32 getSegmentStableMetaVersion(SegmentPtr segment)
    {
        auto files = segment->stable->getDMFiles();
        RUNTIME_CHECK(!files.empty());

        // TODO: Support multiple DMFiles
        auto file = files[0];

        auto meta_version = file->metaVersion();

        // Read again using a fresh DMFile restore, to ensure that this meta version is
        // indeed persisted.
        auto file2 = DMFile::restore(
            dm_context->db_context.getFileProvider(),
            file->fileId(),
            file->pageId(),
            file->parentPath(),
            DMFileMeta::ReadMode::all(),
            meta_version);
        RUNTIME_CHECK(file2 != nullptr);

        return meta_version;
    }

    UInt32 getSegmentStableMetaVersion(PageIdU64 segment_id)
    {
        auto [segment, snapshot] = getSegmentForRead(segment_id);
        RUNTIME_CHECK(segment != nullptr);
        UNUSED(snapshot);
        return getSegmentStableMetaVersion(segment);
    }

    String getSegmentStableMetaValue(SegmentPtr segment)
    {
        // For test purpose, we only get the additional_data_for_test field
        // of the PK, as a prove of the metadata.

        auto files = segment->stable->getDMFiles();
        RUNTIME_CHECK(!files.empty());

        auto file = files[0];
        auto column_stats = file->meta->getColumnStats();
        RUNTIME_CHECK(column_stats.find(::DB::TiDBPkColumnID) != column_stats.end());

        auto meta_value = column_stats[::DB::TiDBPkColumnID].additional_data_for_test;

        // Read again using a fresh DMFile restore, to ensure that this value is
        // indeed persisted.
        auto file2 = DMFile::restore(
            dm_context->db_context.getFileProvider(),
            file->fileId(),
            file->pageId(),
            file->parentPath(),
            DMFileMeta::ReadMode::all(),
            file->metaVersion());
        RUNTIME_CHECK(file2 != nullptr);

        column_stats = file2->meta->getColumnStats();
        RUNTIME_CHECK(column_stats.find(::DB::TiDBPkColumnID) != column_stats.end());
        RUNTIME_CHECK(column_stats[::DB::TiDBPkColumnID].additional_data_for_test == meta_value);

        return meta_value;
    }

    String getSegmentStableMetaValue(PageIdU64 segment_id)
    {
        auto [segment, snapshot] = getSegmentForRead(segment_id);
        RUNTIME_CHECK(segment != nullptr);
        UNUSED(snapshot);
        return getSegmentStableMetaValue(segment);
    }

    inline void assertPK(PageIdU64 segment_id, std::string_view expected_sequence)
    {
        auto left_handle = getSegmentHandle(segment_id, {});
        const auto * left_r = toColumnVectorDataPtr<Int64>(left_handle);
        auto expected_left_handle = genSequence<Int64>(expected_sequence);
        ASSERT_EQ(expected_left_handle.size(), left_r->size());
        ASSERT_TRUE(sequenceEqual(expected_left_handle.data(), left_r->data(), left_r->size()));
    }

private:
    StorageFormatVersion storage_version = STORAGE_FORMAT_CURRENT;
};

INSTANTIATE_TEST_CASE_P(
    DMFileMetaVersion,
    SegmentReplaceStableData,
    /* unused */ testing::Values(false));

TEST_P(SegmentReplaceStableData, ReplaceWithAnotherDMFile)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto block = prepareWriteBlock(/* from */ 0, /* to */ 10);
    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto new_dm_file = writeIntoNewDMFile(*dm_context, table_columns, input_stream, file_id, delegator.choosePath());

    ASSERT_FALSE(replaceSegmentStableData(DELTA_MERGE_FIRST_SEGMENT_ID, new_dm_file));
}
CATCH

TEST_P(SegmentReplaceStableData, Basic)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 100, /* start_at= */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 10, /* start_at= */ 200);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)");

    // Initial meta version should be 0
    ASSERT_EQ(0, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    // Create a new meta and replace
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "hello");
    // Data in delta does not change
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)");
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("hello", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    // Create a new meta and replace
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "foo");
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)");
    ASSERT_EQ(2, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("foo", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    // Write to delta after updating the meta should be fine.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 50, /* start_at= */ 500);
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)|[500,550)");
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)|[500,550)");

    // Rewrite stable should result in a fresh meta
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)|[200,210)|[500,550)");
    ASSERT_EQ(0, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
}
CATCH

TEST_P(SegmentReplaceStableData, LogicalSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 100, /* start_at= */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // Create a new meta and replace
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "bar");
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)");

    // Logical split
    auto right_segment_id = splitSegmentAt( //
        DELTA_MERGE_FIRST_SEGMENT_ID,
        /* split_at= */ 50,
        Segment::SplitMode::Logical);
    ASSERT_TRUE(right_segment_id.has_value());

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,50)");
    assertPK(*right_segment_id, "[50,100)");

    // The new segment should have the same meta
    ASSERT_EQ(1, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(*right_segment_id).c_str());

    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    // Rewrite stable
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,50)");
    assertPK(*right_segment_id, "[50,100)");

    ASSERT_EQ(1, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(*right_segment_id).c_str());

    ASSERT_EQ(0, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
}
CATCH

TEST_P(SegmentReplaceStableData, PhysicalSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 100, /* start_at= */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // Create a new meta and replace
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "bar");
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)");

    // Physical split
    auto right_segment_id = splitSegmentAt( //
        DELTA_MERGE_FIRST_SEGMENT_ID,
        /* split_at= */ 50,
        Segment::SplitMode::Physical);
    ASSERT_TRUE(right_segment_id.has_value());

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,50)");
    assertPK(*right_segment_id, "[50,100)");

    // Physical split will rewrite the stable, thus result in a fresh meta
    ASSERT_EQ(0, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("", getSegmentStableMetaValue(*right_segment_id).c_str());

    ASSERT_EQ(0, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
}
CATCH

TEST_P(SegmentReplaceStableData, UpdateMetaAfterLogicalSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 100, /* start_at= */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto right_segment_id = splitSegmentAt( //
        DELTA_MERGE_FIRST_SEGMENT_ID,
        /* split_at= */ 50,
        Segment::SplitMode::Logical);
    ASSERT_TRUE(right_segment_id.has_value());

    // The left and right segment shares the same stable.
    // However we should be able to update their meta independently,
    // as long as meta versions are different.

    ASSERT_EQ(0, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
    ASSERT_EQ(0, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("", getSegmentStableMetaValue(*right_segment_id).c_str());

    // Update left meta does not change right meta
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "bar");
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
    ASSERT_EQ(0, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("", getSegmentStableMetaValue(*right_segment_id).c_str());

    // Update right meta should fail, because right meta is still holding meta version 0
    // and will overwrite meta version 1.
    ASSERT_THROW({ replaceSegmentStableWithNewMetaValue(*right_segment_id, "foo"); }, DB::Exception);
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("bar", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());
    ASSERT_EQ(0, getSegmentStableMetaVersion(*right_segment_id));
    ASSERT_STREQ("", getSegmentStableMetaValue(*right_segment_id).c_str());
}
CATCH

TEST_P(SegmentReplaceStableData, RestoreSegment)
try
{
    // TODO with different storage format versions.

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* write_rows= */ 100, /* start_at= */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)");

    // Create a new meta and replace
    replaceSegmentStableWithNewMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID, "hello");
    assertPK(DELTA_MERGE_FIRST_SEGMENT_ID, "[0,100)");
    ASSERT_EQ(1, getSegmentStableMetaVersion(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_STREQ("hello", getSegmentStableMetaValue(DELTA_MERGE_FIRST_SEGMENT_ID).c_str());

    // Restore the segment from PageStorage, meta version should be correct.
    SegmentPtr restored_segment = Segment::restoreSegment(Logger::get(), *dm_context, DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(1, getSegmentStableMetaVersion(restored_segment));
    ASSERT_STREQ("hello", getSegmentStableMetaValue(restored_segment).c_str());
}
CATCH

class SegmentReplaceStableDataDisaggregated
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface</* enable_file_cache */ bool>
{
private:
    bool enable_file_cache = false;

public:
    SegmentReplaceStableDataDisaggregated() { enable_file_cache = GetParam(); }

public:
    void SetUp() override
    {
        storage_version = STORAGE_FORMAT_CURRENT;
        STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V6;

        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        TiFlashStorageTestBasic::SetUp();

        auto & global_context = TiFlashTestEnv::getGlobalContext();

        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store == nullptr);
        global_context.getSharedContextDisagg()->initRemoteDataStore(
            global_context.getFileProvider(),
            /*s3_enabled*/ true);
        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store != nullptr);

        ASSERT_TRUE(global_context.getWriteNodePageStorage() == nullptr);
        orig_mode = global_context.getPageStorageRunMode();
        global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
        global_context.tryReleaseWriteNodePageStorageForTest();
        global_context.initializeWriteNodePageStorageIfNeed(global_context.getPathPool());

        auto kvstore = db_context->getTMTContext().getKVStore();
        {
            auto meta_store = metapb::Store{};
            meta_store.set_id(100);
            kvstore->setStore(meta_store);
        }

        TiFlashStorageTestBasic::reload(DB::Settings());
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

        if (enable_file_cache)
        {
            StorageRemoteCacheConfig file_cache_config{
                .dir = fmt::format("{}/fs_cache", getTemporaryPath()),
                .capacity = 1 * 1000 * 1000 * 1000,
            };
            FileCache::initialize(global_context.getPathCapacity(), file_cache_config);
        }

        table_columns = DMTestEnv::getDefaultColumns();

        wn_dm_context = dmContext();
        wn_segment = Segment::newSegment(
            Logger::get(),
            *wn_dm_context,
            table_columns,
            RowKeyRange::newAll(false, 1),
            DELTA_MERGE_FIRST_SEGMENT_ID,
            0);
        ASSERT_EQ(wn_segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    void TearDown() override
    {
        if (enable_file_cache)
        {
            FileCache::shutdown();
        }

        auto & global_context = TiFlashTestEnv::getGlobalContext();
        // global_context.dropVectorIndexCache();
        global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        global_context.setPageStorageRunMode(orig_mode);

        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();

        STORAGE_FORMAT_CURRENT = storage_version;
    }

    SegmentSnapshotPtr createCNSnapshotFromWN(SegmentPtr wn_segment, const DMContext & wn_context)
    {
        auto snap = wn_segment->createSnapshot(wn_context, false, CurrentMetrics::DT_SnapshotOfRead);
        auto snap_proto = Remote::Serializer::serializeTo(
            snap,
            wn_segment->segmentId(),
            0,
            wn_segment->rowkey_range,
            {wn_segment->rowkey_range},
            dummy_mem_tracker);

        auto cn_segment = std::make_shared<Segment>(
            Logger::get(),
            /*epoch*/ 0,
            wn_segment->getRowKeyRange(),
            wn_segment->segmentId(),
            /*next_segment_id*/ 0,
            nullptr,
            nullptr);

        auto read_dm_context = dmContext();
        auto cn_segment_snap = Remote::Serializer::deserializeSegmentSnapshotFrom(
            *read_dm_context,
            /* store_id */ 100,
            0,
            /* table_id */ 100,
            snap_proto);

        return cn_segment_snap;
    }

protected:
    DMContextPtr dmContext(const ScanContextPtr & scan_context = nullptr)
    {
        return std::make_unique<DMContext>(
            *db_context,
            storage_path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef(),
            scan_context);
    }

protected:
    /// all these var lives as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;

    NamespaceID ns_id = 100;

    // the segment we are going to test
    SegmentPtr wn_segment;
    DMContextPtr wn_dm_context;

    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;

    MemTrackerWrapper dummy_mem_tracker = MemTrackerWrapper(0, root_of_query_mem_trackers.get());

private:
    StorageFormatVersion storage_version = STORAGE_FORMAT_CURRENT;
};

INSTANTIATE_TEST_CASE_P(
    DMFileMetaVersion,
    SegmentReplaceStableDataDisaggregated,
    /* enable_file_cache */ testing::Bool());

TEST_P(SegmentReplaceStableDataDisaggregated, Basic)
try
{
    // Prepare a stable data on WN
    {
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 100);
        wn_segment->write(*wn_dm_context, std::move(block), true);
        wn_segment = wn_segment->mergeDelta(*wn_dm_context, table_columns);
        ASSERT_TRUE(wn_segment != nullptr);
        ASSERT_TRUE(wn_segment->stable->getDMFiles()[0]->path().rfind("s3://") == 0);
    }

    // Prepare meta version 1
    SegmentPtr wn_segment_v1{};
    {
        auto file = wn_segment->stable->getDMFiles()[0];
        auto new_dm_file = DMFile::restore(
            wn_dm_context->db_context.getFileProvider(),
            file->fileId(),
            file->pageId(),
            file->parentPath(),
            DMFileMeta::ReadMode::all(),
            file->metaVersion());

        auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
            .dm_file = new_dm_file,
            .file_provider = wn_dm_context->db_context.getFileProvider(),
            .write_limiter = wn_dm_context->db_context.getWriteLimiter(),
            .path_pool = storage_path_pool,
            .disagg_ctx = wn_dm_context->db_context.getSharedContextDisagg(),
        });
        auto & column_stats = new_dm_file->meta->getColumnStats();
        RUNTIME_CHECK(column_stats.find(::DB::TiDBPkColumnID) != column_stats.end());
        column_stats[::DB::TiDBPkColumnID].additional_data_for_test = "tiflash_foo";

        new_dm_file->meta->bumpMetaVersion();
        iw->finalize();

        auto lock = wn_segment->mustGetUpdateLock();
        wn_segment_v1 = wn_segment->replaceStableMetaVersion(lock, *wn_dm_context, {new_dm_file});
        RUNTIME_CHECK(wn_segment_v1 != nullptr);
    }

    // Read meta v0 in CN
    {
        auto snapshot = createCNSnapshotFromWN(wn_segment, *wn_dm_context);
        ASSERT_TRUE(snapshot != nullptr);
        auto cn_files = snapshot->stable->getDMFiles();
        ASSERT_EQ(1, cn_files.size());
        ASSERT_EQ(0, cn_files[0]->metaVersion());
        ASSERT_STREQ("", cn_files[0]->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
    }

    // Read meta v1 in CN
    {
        auto snapshot = createCNSnapshotFromWN(wn_segment_v1, *wn_dm_context);
        ASSERT_TRUE(snapshot != nullptr);
        auto cn_files = snapshot->stable->getDMFiles();
        ASSERT_EQ(1, cn_files.size());
        ASSERT_EQ(1, cn_files[0]->metaVersion());
        ASSERT_STREQ(
            "tiflash_foo",
            cn_files[0]->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
    }

    // Read meta v0 again in CN
    {
        auto snapshot = createCNSnapshotFromWN(wn_segment, *wn_dm_context);
        ASSERT_TRUE(snapshot != nullptr);
        auto cn_files = snapshot->stable->getDMFiles();
        ASSERT_EQ(1, cn_files.size());
        ASSERT_EQ(0, cn_files[0]->metaVersion());
        ASSERT_STREQ("", cn_files[0]->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
    }
}
CATCH

} // namespace DB::DM::tests
