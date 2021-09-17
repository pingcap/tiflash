#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>

#include "dm_basic_include.h"

namespace DB
{
namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
TEST(DMFileWriterFlags_test, SetClearFlags)
{
    using Flags = DMFileWriter::Flags;

    Flags flags;

    bool f = false;
    flags.setSingleFile(f);
    EXPECT_FALSE(flags.isSingleFile());

    f = true;
    flags.setSingleFile(f);
    EXPECT_TRUE(flags.isSingleFile());
}

String paramToString(const ::testing::TestParamInfo<DMFile::Mode> & info)
{
    const auto mode = info.param;

    String name;
    switch (mode)
    {
    case DMFile::Mode::SINGLE_FILE:
        name = "single_file";
        break;
    case DMFile::Mode::FOLDER:
        name = "folder";
        break;
    }
    return name;
}

using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

class DMFile_Test : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<DMFile::Mode>
{
public:
    DMFile_Test()
        : dm_file(nullptr)
    {}

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        parent_path = TiFlashStorageTestBasic::getTemporaryPath();

        auto mode = GetParam();
        bool single_file_mode = mode == DMFile::Mode::SINGLE_FILE;

        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_unique<StoragePool>("test.t1", *path_pool, *db_context, db_context->getSettingsRef());
        dm_file = DMFile::create(1, parent_path, single_file_mode);
        table_columns_ = std::make_shared<ColumnDefines>();
        column_cache_ = std::make_shared<ColumnCache>();

        reload();
    }

    // Update dm_context.
    void reload(const ColumnDefinesPtr & cols = DMTestEnv::getDefaultColumns())
    {
        TiFlashStorageTestBasic::reload();
        if (table_columns_ != cols)
            *table_columns_ = *cols;
        *path_pool = db_context->getPathPool().withTable("test", "t1", false);
        dm_context = std::make_unique<DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            /*hash_salt*/ 0,
            0,
            settings.not_compress_columns,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMFilePtr restoreDMFile()
    {
        auto file_id = dm_file->fileId();
        auto ref_id = dm_file->refId();
        auto parent_path = dm_file->parentPath();
        auto file_provider = dbContext().getFileProvider();
        return DMFile::restore(file_provider, file_id, ref_id, parent_path, DMFile::ReadMetaMode::all());
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns_;
    DeltaMergeStore::Settings settings;

protected:
    String parent_path;
    DMFilePtr dm_file;
    ColumnCachePtr column_cache_;
};


TEST_P(DMFile_Test, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 2);
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    /// Test restore the file from disk and read
    {
        dm_file = restoreDMFile();

        // Test dt property read success
        auto propertys = dm_file->getPackProperties();
        ASSERT_EQ(propertys.property_size(), 2);
        for (int i = 0; i < propertys.property_size(); i++)
        {
            auto & property = propertys.property(i);
            ASSERT_EQ((size_t)property.num_rows(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.gc_hint_version(), (size_t)block_propertys[i].effective_num_rows);
        }
    }
    {
        // Test read after restore
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_Test, GcFlag)
try
{
    // clean
    auto file_provider = dbContext().getFileProvider();
    auto id = dm_file->fileId();
    dm_file->remove(file_provider);
    dm_file.reset();

    auto mode = GetParam();
    bool single_file_mode = mode == DMFile::Mode::SINGLE_FILE;

    dm_file = DMFile::create(id, parent_path, single_file_mode);
    // Right after created, the fil is not abled to GC and it is ignored by `listAllInPath`
    EXPECT_FALSE(dm_file->canGC());
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto scanIds = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_TRUE(scanIds.empty());

    {
        // Write some data and finialize the file
        auto cols = DMTestEnv::getDefaultColumns();
        auto num_rows_write = 128UL;
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }

    // The file remains not able to GC
    ASSERT_FALSE(dm_file->canGC());
    options.only_list_can_gc = false;
    // Now the file can be scaned
    scanIds = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scanIds.size(), 1UL);
    EXPECT_EQ(*scanIds.begin(), id);
    options.only_list_can_gc = true;
    scanIds = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(scanIds.empty());

    // After enable GC, the file can be scaned with `can_gc=true`
    dm_file->enableGC();
    ASSERT_TRUE(dm_file->canGC());
    options.only_list_can_gc = false;
    scanIds = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scanIds.size(), 1UL);
    EXPECT_EQ(*scanIds.begin(), id);
    options.only_list_can_gc = true;
    scanIds = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scanIds.size(), 1UL);
    EXPECT_EQ(*scanIds.begin(), id);
}
CATCH

/// DMFile_Test.InterruptedDrop_0 and InterruptedDrop_1 test that if deleting file
/// is interrupted by accident, we can safely ignore those broken files.

TEST_P(DMFile_Test, InterruptedDrop_0)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    FailPointHelper::enableFailPoint(FailPoints::exception_before_dmfile_remove_encryption);
    auto file_provider = dbContext().getFileProvider();
    try
    {
        dm_file->remove(file_provider);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
            throw;
    }

    // The broken file is ignored
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto res = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(res.empty());
}
CATCH

TEST_P(DMFile_Test, InterruptedDrop_1)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    FailPointHelper::enableFailPoint(FailPoints::exception_before_dmfile_remove_from_disk);
    auto file_provider = dbContext().getFileProvider();
    try
    {
        dm_file->remove(file_provider);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
            throw;
    }

    // The broken file is ignored
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto res = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(res.empty());
}
CATCH

/// Test reading rows with some filters

TEST_P(DMFile_Test, ReadFilteredByHandle)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;

        DMFileBlockOutputStream::BlockProperty block_property;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges;
    ranges.emplace_back(HandleRange{0, span_per_part}); // only first part
    ranges.emplace_back(HandleRange{800, num_rows_write});
    ranges.emplace_back(HandleRange{256, 700}); //
    ranges.emplace_back(HandleRange::newNone()); // none
    ranges.emplace_back(HandleRange{0, num_rows_write}); // full range
    ranges.emplace_back(HandleRange::newAll()); // full range
    auto test_read_range = [&](const HandleRange & range) {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::fromHandleRange(range)}, // Filtered by read_range
            EMPTY_FILTER,
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = int(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(num_rows_write, //
                                        int(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                                            + (range.end % span_per_part ? span_per_part : 0));
        Int64 cur_pk = expect_first_pk;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++)
                    << "range: " << range.toDebugString() << ", cur_pk: " << cur_pk << ", first pk: " << expect_first_pk;
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "range: " << range.toDebugString() //
            << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    };

    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with range:" + range.toDebugString());
        test_read_range(range);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with range:" + range.toDebugString() + " after restoring DTFile");
        test_read_range(range);
    }
}
CATCH

namespace
{
RSOperatorPtr toRSFilter(const ColumnDefine & cd, const HandleRange & range)
{
    Attr attr = {cd.name, cd.id, cd.type};
    auto left = createGreaterEqual(attr, Field(range.start), -1);
    auto right = createLess(attr, Field(range.end), -1);
    return createAnd({left, right});
}
} // namespace

TEST_P(DMFile_Test, ReadFilteredByRoughSetFilter)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);

            auto col = i64_cd.type->createColumn();
            for (size_t i = pk_beg; i < pk_end; i++)
            {
                col->insert(toField(Int64(i)));
            }
            ColumnWithTypeAndName i64(std::move(col), i64_cd.type, i64_cd.name, i64_cd.id);
            block.insert(i64);

            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges;
    ranges.emplace_back(HandleRange{0, span_per_part}); // only first part
    ranges.emplace_back(HandleRange{800, num_rows_write});
    ranges.emplace_back(HandleRange{256, 700}); //
    ranges.emplace_back(HandleRange::newNone()); // none
    ranges.emplace_back(HandleRange{0, num_rows_write}); // full range
    ranges.emplace_back(HandleRange::newAll()); // full range
    auto test_read_filter = [&](const HandleRange & range) {
        // Filtered by rough set filter
        auto filter = toRSFilter(i64_cd, range);
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            filter, // Filtered by rough set filter
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = int(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(num_rows_write, //
                                        int(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                                            + (range.end % span_per_part ? span_per_part : 0));
        Int64 cur_pk = expect_first_pk;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(i64_cd.name));
            auto col = in.getByName(i64_cd.name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++)
                    << "range: " << range.toDebugString() << ", cur_pk: " << cur_pk << ", first pk: " << expect_first_pk;
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "range: " << range.toDebugString() //
            << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    };

    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with filter range:" + range.toDebugString());
        test_read_filter(range);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with filter range:" + range.toDebugString() + " after restoring DTFile");
        test_read_filter(range);
    }
}
CATCH

// Test rough filter with some unsupported operations
TEST_P(DMFile_Test, ReadFilteredByRoughSetFilterWithUnsupportedOperation)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);

            auto col = i64_cd.type->createColumn();
            for (size_t i = pk_beg; i < pk_end; i++)
            {
                col->insert(toField(Int64(i)));
            }
            ColumnWithTypeAndName i64(std::move(col), i64_cd.type, i64_cd.name, i64_cd.id);
            block.insert(i64);

            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    std::vector<std::pair<DM::RSOperatorPtr, size_t>> filters;
    DM::RSOperatorPtr one_part_filter = toRSFilter(i64_cd, HandleRange{0, span_per_part});
    // <filter, num_rows_should_read>
    filters.emplace_back(one_part_filter, span_per_part); // only first part
    // <filter, num_rows_should_read>
    // (first range) And (Unsuppported) -> should filter some chunks by range
    filters.emplace_back(createAnd({one_part_filter, createUnsupported("test", "test", false)}), span_per_part);
    // <filter, num_rows_should_read>
    // (first range) Or (Unsupported) -> should NOT filter any chunk
    filters.emplace_back(createOr({one_part_filter, createUnsupported("test", "test", false)}), num_rows_write);
    auto test_read_filter = [&](const DM::RSOperatorPtr & filter, const size_t num_rows_should_read) {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            filter, // Filtered by rough set filter
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = 0;
        Int64 expect_last_pk = num_rows_should_read;
        Int64 cur_pk = expect_first_pk;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(i64_cd.name));
            auto col = in.getByName(i64_cd.name);
            auto & c = col.column;
            for (size_t j = 0; j < c->size(); j++)
            {
                EXPECT_EQ(c->getInt(j), cur_pk++) << "cur_pk: " << cur_pk << ", first pk: " << expect_first_pk;
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "first: " << expect_first_pk << ", last: " << expect_last_pk;
    };

    for (size_t i = 0; i < filters.size(); ++i)
    {
        const auto & filter = filters[i].first;
        const auto num_rows_should_read = filters[i].second;
        SCOPED_TRACE("Test reading with idx: " + DB::toString(i) + ", filter range:" + filter->toDebugString());
        test_read_filter(filter, num_rows_should_read);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (size_t i = 0; i < filters.size(); ++i)
    {
        const auto & filter = filters[i].first;
        const auto num_rows_should_read = filters[i].second;
        SCOPED_TRACE("Test reading with idx: " + DB::toString(i) + ", filter range:" + filter->toDebugString() + " after restoring DTFile");
        test_read_filter(filter, num_rows_should_read);
    }
}
CATCH

TEST_P(DMFile_Test, ReadFilteredByPackIndices)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    std::vector<IdSet> test_sets;
    test_sets.emplace_back(IdSet{0});
    test_sets.emplace_back(IdSet{nparts - 1});
    test_sets.emplace_back(IdSet{nparts - 2, nparts - 1});
    test_sets.emplace_back(IdSet{1, 2});
    test_sets.emplace_back(IdSet{}); // filter all packs
    auto test_with_case_index = [&](const size_t test_index) {
        IdSetPtr id_set_ptr = nullptr; // Keep for not filter test
        if (test_index < test_sets.size())
            id_set_ptr = std::make_shared<IdSet>(test_sets[test_index]);

        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            EMPTY_FILTER,
            column_cache_,
            id_set_ptr);

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = 0, expect_last_pk = 0;
        if (id_set_ptr && !id_set_ptr->empty())
        {
            expect_first_pk = *(id_set_ptr->begin()) * span_per_part;
            auto last_id = *(id_set_ptr->rbegin());
            expect_last_pk = (last_id == nparts - 1) ? num_rows_write : (last_id + 1) * span_per_part;
        }
        else if (!id_set_ptr)
        {
            // not filter if it is nullptr
            expect_last_pk = num_rows_write;
        }

        Int64 cur_pk = expect_first_pk;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                EXPECT_EQ(c->getInt(i), cur_pk++) //
                    << "test index: " << test_index //
                    << ", cur_pk: " << cur_pk << ", first pk: " << expect_first_pk;
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "test index: " << test_index << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    };
    for (size_t test_index = 0; test_index <= test_sets.size(); test_index++)
    {
        SCOPED_TRACE("Test reading with index:" + DB::toString(test_index));
        test_with_case_index(test_index);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (size_t test_index = 0; test_index <= test_sets.size(); test_index++)
    {
        SCOPED_TRACE("Test reading with index:" + DB::toString(test_index) + " after restoring DTFile");
        test_with_case_index(test_index);
    }
}
CATCH

/// Test reading different column types

TEST_P(DMFile_Test, NumberTypes)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_col(2, "i64", typeFromString("Int64"));
    ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
    cols->push_back(i64_col);
    cols->push_back(f64_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        auto col = i64_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(Int64(i)));
        }
        ColumnWithTypeAndName i64(std::move(col), i64_col.type, i64_col.name, i64_col.id);

        col = f64_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(Float64(0.125)));
        }
        ColumnWithTypeAndName f64(std::move(col), f64_col.type, f64_col.name, f64_col.id);

        block.insert(i64);
        block.insert(f64);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(i64_col.name));
            ASSERT_TRUE(in.has(f64_col.name));
            auto i64_c = in.getByName(i64_col.name).column;
            auto f64_c = in.getByName(f64_col.name).column;
            ASSERT_EQ(i64_c->size(), f64_c->size());
            for (size_t i = 0; i < i64_c->size(); i++)
            {
                EXPECT_EQ(i64_c->getInt(i), cur_pk++);
                Field f = (*f64_c)[i];
                EXPECT_FLOAT_EQ(f.get<Float64>(), 0.125);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_Test, StringType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine fixed_str_col(2, "str", typeFromString("FixedString(5)"));
    cols->push_back(fixed_str_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        auto col = fixed_str_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(String("hello")));
        }
        ColumnWithTypeAndName str(std::move(col), fixed_str_col.type, fixed_str_col.name, fixed_str_col.id);

        block.insert(str);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(fixed_str_col.name));
            auto col = in.getByName(fixed_str_col.name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                Field value = (*c)[i];
                EXPECT_EQ(value.get<String>(), "hello");
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_Test, NullableType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    ColumnDefine nullable_col(2, "i32_null", typeFromString("Nullable(Int32)"));
    // Prepare columns
    cols->emplace_back(nullable_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        ColumnWithTypeAndName nullable_col({}, typeFromString("Nullable(Int32)"), "i32_null", 2);
        auto col = nullable_col.type->createColumn();
        for (size_t i = 0; i < 64; i++)
        {
            col->insert(toField(Int64(i)));
        }
        for (size_t i = 64; i < num_rows_write; i++)
        {
            col->insertDefault();
        }
        nullable_col.column = std::move(col);

        block.insert(nullable_col);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            ASSERT_TRUE(in.has(nullable_col.name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            auto ncol = in.getByName(nullable_col.name);
            auto & nc = ncol.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                // check nullable column
                {
                    const auto nested_col = typeid_cast<const ColumnNullable *>(nc.get());
                    auto nested = nested_col->getNestedColumnPtr();
                    if (cur_pk < 64)
                    {
                        EXPECT_FALSE(nested_col->isNullAt(i));
                        EXPECT_EQ(nested->getInt(i), cur_pk);
                    }
                    else
                    {
                        EXPECT_TRUE(nested_col->isNullAt(i));
                    }
                }
                // check pk
                EXPECT_EQ(c->getInt(i), cur_pk++);
            }
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, num_rows_write);
        stream->readSuffix();
    }
}
CATCH


INSTANTIATE_TEST_CASE_P(DTFileMode, //
                        DMFile_Test,
                        testing::Values(DMFile::Mode::FOLDER, DMFile::Mode::SINGLE_FILE),
                        paramToString);


/// DMFile test for clustered index
class DMFile_Clustered_Index_Test : public DB::base::TiFlashStorageTestBasic
    , //
                                    public testing::WithParamInterface<DMFile::Mode>
{
public:
    DMFile_Clustered_Index_Test()
        : dm_file(nullptr)
    {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        path = TiFlashStorageTestBasic::getTemporaryPath();

        auto mode = GetParam();
        bool single_file_mode = mode == DMFile::Mode::SINGLE_FILE;

        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t", false));
        storage_pool = std::make_unique<StoragePool>("test.t1", *path_pool, *db_context, DB::Settings());
        dm_file = DMFile::create(0, path, single_file_mode);
        table_columns_ = std::make_shared<ColumnDefines>();
        column_cache_ = std::make_shared<ColumnCache>();

        reload();
    }

    // Update dm_context.
    void reload(ColumnDefinesPtr cols = {})
    {
        TiFlashStorageTestBasic::reload();
        if (!cols)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

        *table_columns_ = *cols;

        dm_context = std::make_unique<DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            /*hash_salt*/ 0,
            0,
            settings.not_compress_columns,
            is_common_handle,
            rowkey_column_size,
            db_context->getSettingsRef());
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    String path;
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns_;
    DeltaMergeStore::Settings settings;

protected:
    DMFilePtr dm_file;
    ColumnCachePtr column_cache_;
    TableID table_id = 1;
    bool is_common_handle = true;
    size_t rowkey_column_size = 2;
};

TEST_P(DMFile_Clustered_Index_Test, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0,
                                                          num_rows_write / 2,
                                                          false,
                                                          2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          is_common_handle,
                                                          rowkey_column_size);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2,
                                                          num_rows_write,
                                                          false,
                                                          2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          is_common_handle,
                                                          rowkey_column_size);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(is_common_handle, rowkey_column_size)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 cur_pk = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                DMTestEnv::verifyClusteredIndexValue((*c)[i].get<String>(), cur_pk++, rowkey_column_size);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_Clustered_Index_Test, ReadFilteredByHandle)
try
{
    auto cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg,
                                                             pk_end,
                                                             false,
                                                             2,
                                                             EXTRA_HANDLE_COLUMN_NAME,
                                                             EXTRA_HANDLE_COLUMN_ID,
                                                             EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                             is_common_handle,
                                                             rowkey_column_size);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    struct QueryRangeInfo
    {
        QueryRangeInfo(const RowKeyRange & range_, Int64 start_, Int64 end_)
            : range(range_)
            , start(start_)
            , end(end_)
        {}
        RowKeyRange range;
        Int64 start, end;
    };
    std::vector<QueryRangeInfo> ranges;
    ranges.emplace_back(
        DMTestEnv::getRowKeyRangeForClusteredIndex(0, span_per_part, rowkey_column_size),
        0,
        span_per_part); // only first part
    ranges.emplace_back(DMTestEnv::getRowKeyRangeForClusteredIndex(800, num_rows_write, rowkey_column_size), 800, num_rows_write);
    ranges.emplace_back(DMTestEnv::getRowKeyRangeForClusteredIndex(256, 700, rowkey_column_size), 256, 700); //
    ranges.emplace_back(DMTestEnv::getRowKeyRangeForClusteredIndex(0, 0, rowkey_column_size), 0, 0); // none
    ranges.emplace_back(DMTestEnv::getRowKeyRangeForClusteredIndex(0, num_rows_write, rowkey_column_size), 0, num_rows_write); // full range
    ranges.emplace_back(DMTestEnv::getRowKeyRangeForClusteredIndex(
                            std::numeric_limits<Int64>::min(),
                            std::numeric_limits<Int64>::max(),
                            rowkey_column_size),
                        std::numeric_limits<Int64>::min(),
                        std::numeric_limits<Int64>::max()); // full range
    for (const auto & range : ranges)
    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
            RowKeyRanges{range.range}, // Filtered by read_range
            EMPTY_FILTER,
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = int(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(num_rows_write, //
                                        int(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                                            + (range.end % span_per_part ? span_per_part : 0));
        Int64 cur_pk = expect_first_pk;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(DMTestEnv::pk_name));
            auto col = in.getByName(DMTestEnv::pk_name);
            auto & c = col.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                DMTestEnv::verifyClusteredIndexValue((*c)[i].get<String>(), cur_pk++, rowkey_column_size);
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "range: " << range.range.toDebugString() //
            << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(DTFileMode, //
                        DMFile_Clustered_Index_Test,
                        testing::Values(DMFile::Mode::FOLDER, DMFile::Mode::SINGLE_FILE),
                        paramToString);


/// DDL test cases
class DMFile_DDL_Test : public DMFile_Test
{
public:
    /// Write some data into DMFile.
    /// return rows write, schema
    std::pair<size_t, ColumnDefines> prepareSomeDataToDMFile(bool i8_is_nullable = false)
    {
        size_t num_rows_write = 128;
        auto cols_before_ddl = DMTestEnv::getDefaultColumns();

        ColumnDefine i8_col(2, "i8", i8_is_nullable ? typeFromString("Nullable(Int8)") : typeFromString("Int8"));
        ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
        cols_before_ddl->push_back(i8_col);
        cols_before_ddl->push_back(f64_col);

        reload(cols_before_ddl);

        {
            // Prepare write
            Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

            auto col = i8_col.type->createColumn();
            for (size_t i = 0; i < num_rows_write; i++)
            {
                Field field; // Null by default
                if (!i8_is_nullable || (i8_is_nullable && i < num_rows_write / 2))
                    field = toField(Int64(i) * (-1 * (i % 2)));
                col->insert(field);
            }
            ColumnWithTypeAndName i64(std::move(col), i8_col.type, i8_col.name, i8_col.id);

            col = f64_col.type->createColumn();
            for (size_t i = 0; i < num_rows_write; i++)
            {
                col->insert(toField(Float64(0.125)));
            }
            ColumnWithTypeAndName f64(std::move(col), f64_col.type, f64_col.name, f64_col.id);

            block.insert(i64);
            block.insert(f64);

            auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols_before_ddl);
            DMFileBlockOutputStream::BlockProperty block_property;
            stream->writePrefix();
            stream->write(block, block_property);
            stream->writeSuffix();

            return {num_rows_write, *cols_before_ddl};
        }
    }
};

TEST_P(DMFile_DDL_Test, AddColumn)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we add new column after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    // A new string column
    ColumnDefine new_s_col(100, "s", typeFromString("String"));
    cols_after_ddl->emplace_back(new_s_col);
    // A new int64 column with default value 5
    ColumnDefine new_i_col_with_default(101, "i", typeFromString("Int64"));
    new_i_col_with_default.default_value = Field(Int64(5));
    cols_after_ddl->emplace_back(new_i_col_with_default);

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 row_number = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            ASSERT_TRUE(in.has(new_s_col.name));
            ASSERT_TRUE(in.has(new_i_col_with_default.name));
            {
                auto col = in.getByName(new_s_col.name);
                EXPECT_EQ(col.column_id, new_s_col.id);
                EXPECT_TRUE(col.type->equals(*new_s_col.type));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    Field value = (*c)[i];
                    ASSERT_EQ(value.getType(), Field::Types::String);
                    // Empty default value
                    ASSERT_EQ(value, new_s_col.type->getDefault());
                }
            }
            {
                auto col = in.getByName(new_i_col_with_default.name);
                EXPECT_EQ(col.column_id, new_i_col_with_default.id);
                EXPECT_TRUE(col.type->equals(*new_i_col_with_default.type));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    ASSERT_EQ(c->getInt(i), 5); // Should fill with default value
                }
            }
            {
                // Check old columns before ddl
                auto col = in.getByName("i8");
                EXPECT_EQ(col.column_id, 2L);
                EXPECT_TRUE(col.type->equals(*typeFromString("Int8")));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    EXPECT_EQ(c->getInt(i), Int64(row_number * (-1 * (row_number % 2))));
                    row_number++;
                }
            }
            {
                auto col = in.getByName("f64");
                EXPECT_EQ(col.column_id, 3L);
                EXPECT_TRUE(col.type->equals(*typeFromString("Float64")));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    Field value = (*c)[i];
                    EXPECT_FLOAT_EQ(value.get<Float64>(), 0.125);
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_DDL_Test, UpcastColumnType)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> int32, and its name to "i8_new" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Int32");
    new_col.name = "i32_new";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 row_number = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            {
                auto col = in.getByName(new_col.name);
                EXPECT_EQ(col.column_id, new_col.id);
                EXPECT_TRUE(col.type->equals(*new_col.type));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    auto value = c->getInt(Int64(i));
                    ASSERT_EQ(value, (Int64)(row_number * (-1 * (row_number % 2))));
                    row_number++;
                }
            }
            {
                // Check old columns before ddl
                auto col = in.getByName("f64");
                EXPECT_EQ(col.column_id, 3L);
                EXPECT_TRUE(col.type->equals(*typeFromString("Float64")));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    Field value = (*c)[i];
                    EXPECT_DOUBLE_EQ(value.get<Float64>(), 0.125);
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_DDL_Test, NotNullToNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> Nullable(int32), and its name to "i8_new" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Nullable(Int32)");
    new_col.name = "i32_nullable";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 row_number = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            {
                auto col = in.getByName(new_col.name);
                EXPECT_EQ(col.column_id, new_col.id);
                EXPECT_TRUE(col.type->equals(*new_col.type));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    auto value = (*c)[i];
                    ASSERT_FALSE(value.isNull());
                    ASSERT_EQ(value, (Int64)(row_number * (-1 * (row_number % 2))));
                    row_number++;
                }
            }
            {
                auto col = in.getByName("f64");
                EXPECT_EQ(col.column_id, 3L);
                EXPECT_TRUE(col.type->equals(*typeFromString("Float64")));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    Field value = (*c)[i];
                    EXPECT_DOUBLE_EQ(value.get<Float64>(), 0.125);
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DMFile_DDL_Test, NullToNotNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile(true);

    // Mock that we achange a column type from Nullable(int8) -> int32, and its name to "i32" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Nullable(Int8)")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Int32");
    new_col.name = "i32";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        Int64 row_number = 0;
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            {
                auto col = in.getByName(new_col.name);
                EXPECT_EQ(col.column_id, new_col.id);
                EXPECT_TRUE(col.type->equals(*new_col.type));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    auto value = (*c)[i];
                    if (i < num_rows_write / 2)
                    {
                        ASSERT_FALSE(value.isNull()) << " at row: " << i;
                        ASSERT_EQ(value, (Int64)(row_number * (-1 * (row_number % 2)))) << " at row: " << i;
                    }
                    else
                    {
                        ASSERT_FALSE(value.isNull()) << " at row: " << i;
                        ASSERT_EQ(value, (Int64)0) << " at row: " << i;
                    }
                    row_number++;
                }
            }
            {
                // Check old columns before ddl
                auto col = in.getByName("f64");
                EXPECT_EQ(col.column_id, 3L);
                EXPECT_TRUE(col.type->equals(*typeFromString("Float64")));
                auto c = col.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    Field value = (*c)[i];
                    EXPECT_DOUBLE_EQ(value.get<Float64>(), 0.125);
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(DTFileMode, //
                        DMFile_DDL_Test,
                        testing::Values(DMFile::Mode::FOLDER, DMFile::Mode::SINGLE_FILE),
                        paramToString);

} // namespace tests
} // namespace DM
} // namespace DB
