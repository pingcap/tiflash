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
#include <Storages/S3/S3Common.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <ctime>
#include <memory>

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

protected:
    /// all these var lives as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    NamespaceID ns_id = 100;

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

} // namespace tests
} // namespace DM
} // namespace DB
