// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileLocalIndexWriter.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/tests/gtest_dm_fulltext_index_utils.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>


namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace DB::DM::tests
{

class FullTextIndexDMFileTestBase
    : public FullTextIndexTestUtils
    , public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");
        auto delegator = path_pool->getStableDiskDelegator();
        auto paths = delegator.listPaths();
        RUNTIME_CHECK(paths.size() == 1);
        dm_file = DMFile::create(
            1,
            paths[0],
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            DMFileFormat::V3);

        DB::tests::TiFlashTestEnv::disableS3Config();

        reload();
    }

    // Update dm_context.
    void reload()
    {
        TiFlashStorageTestBasic::reload();

        *path_pool = db_context->getPathPool().withTable("test", "t1", false);
        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMFilePtr restoreDMFile()
    {
        auto dmfile_parent_path = dm_file->parentPath();
        auto dmfile = DMFile::restore(
            dbContext().getFileProvider(),
            dm_file->fileId(),
            dm_file->pageId(),
            dmfile_parent_path,
            DMFileMeta::ReadMode::all(),
            /* meta_version= */ 0);
        auto delegator = path_pool->getStableDiskDelegator();
        delegator.addDTFile(dm_file->fileId(), dmfile->getBytesOnDisk(), dmfile_parent_path);
        return dmfile;
    }

    Context & dbContext() { return *db_context; }

protected:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;

protected:
    String parent_path;
    DMFilePtr dm_file = nullptr;

protected:
    bool test_no_fts_column = false;

    static ColumnDefinesPtr getWriteColumns()
    {
        auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
        cols->emplace_back(cdFts());
        return cols;
    }

    void writeDMFile(const std::vector<String> & data)
    {
        const auto fts_cd = cdFts();
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, data.size());
        block.insert(createColumn<String>(data, fts_cd.name, fts_cd.id));
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *getWriteColumns());
        stream->writePrefix();
        stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }

    DMFilePtr buildIndex(TiDB::FullTextIndexDefinition definition)
    {
        auto build_info = DMFileLocalIndexWriter::getLocalIndexBuildInfo(indexInfo(definition), {dm_file});
        DMFileLocalIndexWriter iw(DMFileLocalIndexWriter::Options{
            .path_pool = path_pool,
            .index_infos = build_info.indexes_to_build,
            .dm_files = {dm_file},
            .dm_context = *dm_context,
        });
        auto new_dmfiles = iw.build();
        assert(new_dmfiles.size() == 1);
        return new_dmfiles[0];
    }

    BlockInputStreamPtr searchFromDMFile(FtsQueryInfoTopKOptions options, std::initializer_list<UInt8> mvcc_bitmap_init)
    {
        auto read_cols = std::make_shared<ColumnDefines>();
        read_cols->emplace_back(getExtraHandleColumnDefine(/* common_handle */ false));
        if (!test_no_fts_column)
            read_cols->emplace_back(cdFts());
        read_cols->emplace_back(FullTextIndexStreamCtx::VIRTUAL_SCORE_CD);

        DMFileBlockInputStreamBuilder builder(dbContext());

        auto fts_idx_ctx = FullTextIndexStreamCtx::createForStableOnlyTests(ftsQueryInfoTopK(options), read_cols);
        auto stream = builder.setFtsIndexQuery(fts_idx_ctx)
                          .build(
                              dm_file,
                              *read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        return FullTextIndexTestUtils::wrapFTSStream( //
            fts_idx_ctx,
            stream,
            std::make_shared<BitmapFilter>(mvcc_bitmap_init));
    }

    Block searchFromDMFileAndSort(FtsQueryInfoTopKOptions options, std::initializer_list<UInt8> mvcc_bitmap_init)
    {
        auto stream = searchFromDMFile(options, mvcc_bitmap_init);
        stream->readPrefix();
        auto block = stream->read();
        RUNTIME_CHECK(!stream->read());
        stream->readSuffix();

        if (block)
        {
            SortDescription sort;
            sort.emplace_back(FullTextIndexStreamCtx::VIRTUAL_SCORE_CD.name, -1, 0);
            sortBlock(block, sort);
        }

        return block;
    }

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns) const
    {
        RUNTIME_CHECK(columns.size() == 2);
        ColumnsWithTypeAndName ret{};
        ret.emplace_back(columns[0]);
        if (!test_no_fts_column)
            ret.emplace_back(columns[1]);
        return ret;
    }

    Strings createColumnNames() const
    {
        Strings ret{};
        ret.emplace_back(DMTestEnv::pk_name);
        if (!test_no_fts_column)
            ret.emplace_back(fts_column_name);
        return ret;
    }
};

class FullTextIndexDMFileTestWithNoIndex
    : public FullTextIndexDMFileTestBase
    , public testing::WithParamInterface<std::tuple</* no_fts_col */ bool, /* no_index*/ bool>>
{
public:
    FullTextIndexDMFileTestWithNoIndex() { std::tie(test_no_fts_column, test_no_index) = GetParam(); }

protected:
    bool test_no_index = false;
};

INSTANTIATE_TEST_CASE_P(
    FullTextIndex,
    FullTextIndexDMFileTestWithNoIndex,
    ::testing::Combine(testing::Bool(), testing::Bool()));

TEST_P(FullTextIndexDMFileTestWithNoIndex, OnePack)
try
{
    writeDMFile({
        "Who knew the people would adore me so much?",
        "Being too popular can be such a hassle",
        "The world is but a stage.",
    });
    dm_file = restoreDMFile();
    if (!test_no_index)
        dm_file = buildIndex(TiDB::FullTextIndexDefinition{
            .parser_type = "STANDARD_V1",
        });

    // Basic match
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 10},
            /* bitmap_filter */ {1, 1, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 2}),
                createColumn<String>({
                    "Who knew the people would adore me so much?",
                    "The world is but a stage.",
                }),
            }));
    }
    // Basic match with MVCC filter
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 10},
            /* bitmap_filter */ {0, 0, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2}),
                createColumn<String>({"The world is but a stage."}),
            }));

        stream = searchFromDMFile( //
            {.query = "the", .top_k = 10},
            /* bitmap_filter */ {0, 1, 0});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({}),
                createColumn<String>({}),
            }));

        stream = searchFromDMFile( //
            {.query = "the", .top_k = 10},
            /* bitmap_filter */ {0, 1, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2}),
                createColumn<String>({"The world is but a stage."}),
            }));
    }

    // Top K
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 1},
            /* bitmap_filter */ {1, 1, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2}),
                createColumn<String>({"The world is but a stage."}),
            }));
    }
    // Top K with MVCC filter
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 1},
            /* bitmap_filter */ {1, 1, 0});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0}),
                createColumn<String>({"Who knew the people would adore me so much?"}),
            }));

        stream = searchFromDMFile( //
            {.query = "the", .top_k = 1},
            /* bitmap_filter */ {0, 1, 0});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({}),
                createColumn<String>({}),
            }));
    }
}
CATCH

TEST_P(FullTextIndexDMFileTestWithNoIndex, OnePackWithDuplicate)
try
{
    writeDMFile({
        "Who knew the people would adore me so much?",
        "Being too popular can be such a hassle",
        "The world is but a stage.",
        "being too popular can be such a hassle",
        "the world is but a stage.",
    });
    dm_file = restoreDMFile();
    if (!test_no_index)
        dm_file = buildIndex(TiDB::FullTextIndexDefinition{
            .parser_type = "STANDARD_V1",
        });

    // Basic match
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 10},
            /* bitmap_filter */ {1, 1, 1, 1, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 2, 4}),
                createColumn<String>({
                    "Who knew the people would adore me so much?",
                    "The world is but a stage.",
                    "the world is but a stage.",
                }),
            }));
    }
    // Top K
    {
        auto stream = searchFromDMFile( //
            {.query = "the", .top_k = 2},
            /* bitmap_filter */ {1, 1, 1, 1, 1});
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2, 4}),
                createColumn<String>({
                    "The world is but a stage.",
                    "the world is but a stage.",
                }),
            }));
    }
}
CATCH


TEST_P(FullTextIndexDMFileTestWithNoIndex, OnePackRanked)
try
{
    writeDMFile(
        // clang-format off
        {
            /* 0 */ "machine learning machine learning machine learning",
            /* 1 */ "machine learning algorithms. Machine learning models. Machine learning optimization.",
            /* 2 */ "Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning ends.",
            /* 3 */ "Understanding machine learning: basics of learning from machines. Machine learning requires data.",
            /* 4 */ "machine learning",
            /* 5 */ "This 50-word document briefly mentions machine learning once. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. ",
            /* 6 */ "Learning machine learning machine. Learning machine applications.",
            /* 7 */ "Space Tourism: Risks and Opportunities - Private companies like SpaceX and Blue Origin are making suborbital flights accessible, but safety protocols remain a critical concern.",
            /* 8 */ "Sustainable Fashion Trends - Eco-friendly materials like mushroom leather and recycled polyester are reshaping the fashion industry’s environmental impact.",
            /* 9 */ "AI-powered tools are transforming medical imaging analysis. Deep learning algorithms now detect early-stage tumors with 95% accuracy.",
        } // clang-format on
    );
    dm_file = restoreDMFile();
    if (!test_no_index)
        dm_file = buildIndex(TiDB::FullTextIndexDefinition{
            .parser_type = "STANDARD_V1",
        });

    {
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 100},
            /* bitmap_filter */
            {
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            });
        ASSERT_COLUMN_EQ(createColumn<Int64>({2, 0, 6, 1, 3, 4, 5, 9}), block.safeGetByPosition(0));
    }
    {
        auto mvcc_bitmap = std::make_shared<BitmapFilter>(10, true);
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 100},
            /* bitmap_filter */
            {
                0,
                0,
                0,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            });
        ASSERT_COLUMN_EQ(createColumn<Int64>({6, 3, 4, 5, 9}), block.safeGetByPosition(0));
    }
    {
        auto mvcc_bitmap = std::make_shared<BitmapFilter>(10, true);
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 100},
            /* bitmap_filter */
            {
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            });
        ASSERT_TRUE(!block);
    }
    {
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 3},
            /* bitmap_filter */
            {
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            });
        ASSERT_COLUMN_EQ(createColumn<Int64>({2, 0, 6}), block.safeGetByPosition(0));
        if (!test_no_fts_column)
            ASSERT_COLUMN_EQ(
                createColumn<String>({
                    "Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used "
                    "in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning "
                    "(is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine "
                    "learning (is used in AI. Machine learning ends.",
                    "machine learning machine learning machine learning",
                    "Learning machine learning machine. Learning machine applications.",
                }),
                block.safeGetByPosition(1));
    }
    {
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 3},
            /* bitmap_filter */
            {
                0,
                0,
                0,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            });
        ASSERT_COLUMN_EQ(createColumn<Int64>({6, 3, 4}), block.safeGetByPosition(0));
    }
    {
        auto block = searchFromDMFileAndSort( //
            {.query = "machine learning", .top_k = 3},
            /* bitmap_filter */
            {
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            });
        ASSERT_TRUE(!block);
    }
}
CATCH


class FullTextIndexSegmentTestBase
    : public FullTextIndexTestUtils
    , public SegmentTestBasic
{
public:
    void SetUp() override
    {
        auto options = SegmentTestBasic::SegmentTestOptions{};
        if (enable_column_cache_long_term)
            options.pk_col_id = MutSup::extra_handle_id;
        SegmentTestBasic::SetUp(options);
    }

    BlockInputStreamPtr ftsQueryTopK(PageIdU64 segment_id, const String & query, UInt32 top_k)
    {
        auto read_cols = std::make_shared<ColumnDefines>();
        read_cols->emplace_back(getExtraHandleColumnDefine(/* common_handle */ false));
        if (!test_no_fts_column)
            read_cols->emplace_back(cdFts());
        read_cols->emplace_back(FullTextIndexStreamCtx::VIRTUAL_SCORE_CD);

        auto [segment_start_key, segment_end_key] = getSegmentKeyRange(segment_id);
        return read(
            segment_id,
            segment_start_key,
            segment_end_key,
            *read_cols,
            ftsQueryInfoTopK({.query = query, .top_k = top_k}));
    }

    BlockInputStreamPtr ftsQueryAll(PageIdU64 segment_id, const String & query)
    {
        return ftsQueryTopK(segment_id, query, std::numeric_limits<UInt32>::max());
    }

    BlockInputStreamPtr read(
        PageIdU64 segment_id,
        Int64 begin,
        Int64 end,
        ColumnDefines columns_to_read,
        FTSQueryInfoPtr fts_query)
    {
        auto range = buildRowKeyRange(begin, end, /* is_common_handle */ false);
        auto [segment, snapshot] = getSegmentForRead(segment_id);
        // load DMilePackFilterResult for each DMFile
        DMFilePackFilterResults pack_filter_results;
        pack_filter_results.reserve(snapshot->stable->getDMFiles().size());
        for (const auto & dmfile : snapshot->stable->getDMFiles())
        {
            auto result = DMFilePackFilter::loadFrom(
                *dm_context,
                dmfile,
                /*set_cache_if_miss*/ true,
                {range},
                EMPTY_RS_OPERATOR,
                /*read_pack*/ {});
            pack_filter_results.push_back(result);
        }
        auto stream = segment->getBitmapFilterInputStream(
            *dm_context,
            columns_to_read,
            snapshot,
            {range},
            std::make_shared<PushDownExecutor>(fts_query),
            pack_filter_results,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_SIZE);
        return stream;
    }

protected:
    Block prepareWriteBlockImpl(
        Int64 start_key,
        Int64 end_key,
        bool is_deleted,
        bool including_right_boundary,
        std::optional<UInt64> ts) override
    {
        auto block
            = SegmentTestBasic::prepareWriteBlockImpl(start_key, end_key, is_deleted, including_right_boundary, ts);
        block.insert(colString(fmt::format("[{}, {})", start_key, end_key), fts_column_name, fts_column_id));
        return block;
    }

    void prepareColumns(const ColumnDefinesPtr & columns) override
    {
        auto fts_cd = ColumnDefine(fts_column_id, fts_column_name, tests::typeFromString("StringV2"));
        columns->emplace_back(fts_cd);
    }

protected:
    bool test_no_fts_column = false;
    bool enable_column_cache_long_term = false;
    int pack_size = 10;

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns) const
    {
        RUNTIME_CHECK(columns.size() == 2);
        ColumnsWithTypeAndName ret{};
        ret.emplace_back(columns[0]);
        if (!test_no_fts_column)
            ret.emplace_back(columns[1]);
        return ret;
    }

    Strings createColumnNames() const
    {
        Strings ret{};
        ret.emplace_back(DMTestEnv::pk_name);
        if (!test_no_fts_column)
            ret.emplace_back(fts_column_name);
        return ret;
    }

    inline void assertStreamOut(BlockInputStreamPtr stream, std::string_view expected_sequence)
    {
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                colInt64(expected_sequence),
                colString(expected_sequence),
            }));
    }
};

enum class DataLocation
{
    InMemory,
    Tiny,
    Big,
    Stable,
    StableWithIndex,
};

class FullTextIndexSegmentDataLocationTest
    : public FullTextIndexSegmentTestBase
    , public testing::WithParamInterface<std::tuple<bool, DataLocation>>
{
public:
    FullTextIndexSegmentDataLocationTest() { std::tie(test_no_fts_column, data_location) = GetParam(); }

protected:
    DataLocation data_location = DataLocation::InMemory;
};


INSTANTIATE_TEST_CASE_P( //
    FullTextIndex,
    FullTextIndexSegmentDataLocationTest,
    ::testing::Combine(
        testing::Bool(),
        testing::Values(
            DataLocation::InMemory,
            DataLocation::Tiny,
            DataLocation::Big,
            DataLocation::Stable,
            DataLocation::StableWithIndex)));


TEST_P(FullTextIndexSegmentDataLocationTest, Basic)
try
{
    auto write = [&](UInt64 write_rows, UInt64 start_at) {
        switch (data_location)
        {
        case DataLocation::InMemory:
            writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, write_rows, start_at);
            break;
        case DataLocation::Tiny:
            writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, write_rows, start_at);
            flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
            break;
        case DataLocation::Big:
            ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, write_rows, start_at, /* clear */ false);
            flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
            break;
        case DataLocation::Stable:
            writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, write_rows, start_at);
            flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
            mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
            break;
        case DataLocation::StableWithIndex:
            writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, write_rows, start_at);
            flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
            mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
            ensureSegmentStableLocalIndex(DELTA_MERGE_FIRST_SEGMENT_ID, indexInfo());
            break;
        default:
            RUNTIME_CHECK_MSG(false, "Unexpected data location {}", magic_enum::enum_name(data_location));
        }
    };

    // No matter where the data is, FTSQuery will always perform a filter.

    write(5, /* at */ 0);
    auto stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    assertStreamOut(stream, "[0, 5)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "3");
    assertStreamOut(stream, "[3, 4)");

    write(5, /* at */ 0);
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    assertStreamOut(stream, "[0, 5)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "3");
    assertStreamOut(stream, "[3, 4)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "10");
    assertStreamOut(stream, "[0, 0)");

    write(5, /* at */ 10);
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    assertStreamOut(stream, "[0, 5)|[10, 15)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "3");
    assertStreamOut(stream, "[3, 4)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "10");
    assertStreamOut(stream, "[10, 11)");

    write(5, /* at */ -10);
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    if (data_location == DataLocation::Stable || data_location == DataLocation::StableWithIndex)
        assertStreamOut(stream, "[-10, -5)|[0, 5)|[10, 15)"); // Stable is sorted
    else
        assertStreamOut(stream, "[0, 5)|[10, 15)|[-10, -5)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "3");
    assertStreamOut(stream, "[3, 4)");
    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "10");
    if (data_location == DataLocation::Stable || data_location == DataLocation::StableWithIndex)
        assertStreamOut(stream, "[-10, -9)|[10, 11)");
    else
        assertStreamOut(stream, "[10, 11)|[-10, -9)");
}
CATCH


class FullTextIndexSegmentTest1
    : public FullTextIndexSegmentTestBase
    , public testing::WithParamInterface<bool>
{
public:
    FullTextIndexSegmentTest1() { test_no_fts_column = GetParam(); }
};


INSTANTIATE_TEST_CASE_P( //
    FullTextIndex,
    FullTextIndexSegmentTest1,
    testing::Bool());


TEST_P(FullTextIndexSegmentTest1, StableOnly)
try
{
    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ensureSegmentStableLocalIndex(DELTA_MERGE_FIRST_SEGMENT_ID, indexInfo());

    auto stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)"); // Empty

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    assertStreamOut(stream, "[0, 5)");

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "1 2");
    assertStreamOut(stream, "[1, 3)");

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "1");
    assertStreamOut(stream, "[1, 2)");
}
CATCH


TEST_P(FullTextIndexSegmentTest1, HybridStableAndDelta)
try
{
    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ensureSegmentStableLocalIndex(DELTA_MERGE_FIRST_SEGMENT_ID, indexInfo());

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 20);

    // ANNQuery will be effective to both stable and delta layer.

    auto stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "100");
    assertStreamOut(stream, "[0, 0)"); // Empty

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "3");
    assertStreamOut(stream, "[3, 4)");

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "20");
    assertStreamOut(stream, "[20, 21)");

    stream = ftsQueryAll(DELTA_MERGE_FIRST_SEGMENT_ID, "word");
    assertStreamOut(stream, "[0, 5)|[20, 30)");
}
CATCH


} // namespace DB::DM::tests
#endif
