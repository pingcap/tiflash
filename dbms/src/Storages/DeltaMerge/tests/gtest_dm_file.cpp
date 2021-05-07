#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

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

using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr  = std::shared_ptr<DMFileBlockInputStream>;

class DMFile_Test : public ::testing::Test
{
public:
    DMFile_Test() : parent_path(DB::tests::TiFlashTestEnv::getTemporaryPath() + "/dm_file_tests"), dm_file(nullptr) {}

    static void SetUpTestCase()
    {
    }

    void SetUp() override
    {
        dropFiles();

        auto ctx       = DMTestEnv::getContext();
        auto settings  = DB::Settings();
        path_pool      = std::make_unique<StoragePathPool>(ctx.getPathPool().withTable("test", "t1", false));
        storage_pool   = std::make_unique<StoragePool>("test.t1", *path_pool, ctx, settings);
        dm_file        = DMFile::create(1, parent_path);
        db_context     = std::make_unique<Context>(DMTestEnv::getContext(settings));
        table_columns_ = std::make_shared<ColumnDefines>();
        column_cache_  = std::make_shared<ColumnCache>();

        reload();
    }

    void dropFiles()
    {
        if (Poco::File file(parent_path); file.exists())
        {
            file.remove(true);
        }
    }

    // Update dm_context.
    void reload(const ColumnDefinesPtr & cols = DMTestEnv::getDefaultColumns())
    {
        *table_columns_ = *cols;

        auto ctx   = DMTestEnv::getContext();
        *path_pool = ctx.getPathPool().withTable("test", "t1", false);
        dm_context = std::make_unique<DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            0,
            settings.not_compress_columns,
            db_context->getSettingsRef());
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    std::unique_ptr<Context>   db_context;
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool>     storage_pool;
    ColumnDefinesPtr                 table_columns_;
    DeltaMergeStore::Settings        settings;

protected:
    const String   parent_path;
    DMFilePtr      dm_file;
    ColumnCachePtr column_cache_;
};


TEST_F(DMFile_Test, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto  stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, 0);
        stream->write(block2, 0);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_Test, InterruptedDrop_0)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto  stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, 0);
        stream->write(block2, 0);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                        num_rows_read++;
                    }
                }
            }
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

    auto res = DMFile::listAllInPath(file_provider, parent_path, true);
    EXPECT_TRUE(res.empty());
}
CATCH

TEST_F(DMFile_Test, InterruptedDrop_1)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto  stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, 0);
        stream->write(block2, 0);
        stream->writeSuffix();
    }


    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                        num_rows_read++;
                    }
                }
            }
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

    auto res = DMFile::listAllInPath(file_provider, parent_path, true);
    EXPECT_TRUE(res.empty());
}
CATCH


TEST_F(DMFile_Test, ReadFilteredByHandle)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts         = 5;
    const Int64 span_per_part  = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto  pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block  = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, 0);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges;
    ranges.emplace_back(HandleRange{0, span_per_part}); // only first part
    ranges.emplace_back(HandleRange{800, num_rows_write});
    ranges.emplace_back(HandleRange{256, 700});          //
    ranges.emplace_back(HandleRange::newNone());         // none
    ranges.emplace_back(HandleRange{0, num_rows_write}); // full range
    ranges.emplace_back(HandleRange::newAll());          // full range
    for (const auto & range : ranges)
    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            range, // Filtered by read_range
            EMPTY_FILTER,
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = int(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk  = std::min(num_rows_write, //
                                        int(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                                            + (range.end % span_per_part ? span_per_part : 0));
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), expect_first_pk + Int64(i))
                            << "range: " << range.toDebugString() << ", i: " << i << ", first pk: " << expect_first_pk;
                        // std::cerr << c->getInt(i) << std::endl;
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "range: " << range.toDebugString()                       //
            << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    }
}
CATCH

namespace
{
RSOperatorPtr toRSFilter(const ColumnDefine & cd, const HandleRange & range)
{
    Attr attr  = {cd.name, cd.id, cd.type};
    auto left  = createGreaterEqual(attr, Field(range.start), -1);
    auto right = createLess(attr, Field(range.end), -1);
    return createAnd({left, right});
}
} // namespace

TEST_F(DMFile_Test, ReadFilteredByRoughSetFilter)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts         = 5;
    const Int64 span_per_part  = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto  pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block  = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);

            auto col = i64_cd.type->createColumn();
            for (size_t i = pk_beg; i < pk_end; i++)
            {
                col->insert(toField(Int64(i)));
            }
            ColumnWithTypeAndName i64(std::move(col), i64_cd.type, i64_cd.name, i64_cd.id);
            block.insert(i64);

            stream->write(block, 0);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges;
    ranges.emplace_back(HandleRange{0, span_per_part}); // only first part
    ranges.emplace_back(HandleRange{800, num_rows_write});
    ranges.emplace_back(HandleRange{256, 700});          //
    ranges.emplace_back(HandleRange::newNone());         // none
    ranges.emplace_back(HandleRange{0, num_rows_write}); // full range
    ranges.emplace_back(HandleRange::newAll());          // full range
    for (const auto & range : ranges)
    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            toRSFilter(i64_cd, range), // Filtered by rough set filter
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = int(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk  = std::min(num_rows_write, //
                                        int(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                                            + (range.end % span_per_part ? span_per_part : 0));
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == i64_cd.name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), expect_first_pk + Int64(i))
                            << "range: " << range.toDebugString() << ", i: " << i << ", first pk: " << expect_first_pk;
                        // std::cerr << c->getInt(i) << std::endl;
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "range: " << range.toDebugString()                  //
            << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    }
}
CATCH

// Test rough filter with some unsupported operations
TEST_F(DMFile_Test, ReadFilteredByRoughSetFilterWithUnsupportedOperation)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts         = 5;
    const Int64 span_per_part  = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto  pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block  = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);

            auto col = i64_cd.type->createColumn();
            for (size_t i = pk_beg; i < pk_end; i++)
            {
                col->insert(toField(Int64(i)));
            }
            ColumnWithTypeAndName i64(std::move(col), i64_cd.type, i64_cd.name, i64_cd.id);
            block.insert(i64);

            stream->write(block, 0);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    std::vector<std::pair<DM::RSOperatorPtr, size_t>> filters;
    DM::RSOperatorPtr                                 one_part_filter = toRSFilter(i64_cd, HandleRange{0, span_per_part});
    // <filter, num_rows_should_read>
    filters.emplace_back(one_part_filter, span_per_part); // only first part
    // <filter, num_rows_should_read>
    // (first range) And (Unsuppported) -> should filter some chunks by range
    filters.emplace_back(createAnd({one_part_filter, createUnsupported("test", "test", false)}), span_per_part);
    // <filter, num_rows_should_read>
    // (first range) Or (Unsupported) -> should NOT filter any chunk
    filters.emplace_back(createOr({one_part_filter, createUnsupported("test", "test", false)}), num_rows_write);
    for (size_t i = 0; i < filters.size(); i++)
    {
        const auto & filter               = filters[i].first;
        const auto   num_rows_should_read = filters[i].second;
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            filter, // Filtered by rough set filter
            column_cache_,
            IdSetPtr{});

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = 0;
        Int64 expect_last_pk  = num_rows_should_read;
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == i64_cd.name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), expect_first_pk + Int64(i)) << "i: " << i << ", first pk: " << expect_first_pk;
                        // std::cerr << c->getInt(i) << std::endl;
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "i: " << i << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    }
}
CATCH

TEST_F(DMFile_Test, ReadFilteredByPackIndices)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts         = 5;
    const Int64 span_per_part  = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto  pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block  = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, 0);
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
    for (size_t test_index = 0; test_index <= test_sets.size(); test_index++)
    {
        IdSetPtr id_set_ptr = nullptr; // Keep for not filter test
        if (test_index < test_sets.size())
            id_set_ptr = std::make_shared<IdSet>(test_sets[test_index]);

        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            EMPTY_FILTER,
            column_cache_,
            id_set_ptr);

        Int64 num_rows_read = 0;
        stream->readPrefix();
        Int64 expect_first_pk = 0, expect_last_pk = 0;
        if (id_set_ptr && !id_set_ptr->empty())
        {
            expect_first_pk = *(id_set_ptr->begin()) * span_per_part;
            auto last_id    = *(id_set_ptr->rbegin());
            expect_last_pk  = (last_id == nparts - 1) ? num_rows_write : (last_id + 1) * span_per_part;
        }
        else if (!id_set_ptr)
        {
            // not filter if it is nullptr
            expect_last_pk = num_rows_write;
        }

        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), expect_first_pk + Int64(i)) << "test index: " << test_index //
                                                                            << ", i: " << i << ", first pk: " << expect_first_pk;
                        // std::cerr << c->getInt(i) << std::endl;
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, expect_last_pk - expect_first_pk) //
            << "test index: " << test_index << ", first: " << expect_first_pk << ", last: " << expect_last_pk;
    }
}
CATCH

TEST_F(DMFile_Test, NumberTypes)
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
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == "i64")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                }
                else if (itr.name == "f64")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value;
                        c->get(i, value);
                        Float64 v = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_Test, StringType)
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
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == "str")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value;
                        c->get(i, value);
                        EXPECT_EQ(value.get<String>(), "hello");
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

TEST_F(DMFile_Test, NullableType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    {
        // Prepare columns
        ColumnDefine nullable_col(2, "i32_null", typeFromString("Nullable(Int32)"));
        cols->emplace_back(nullable_col);
    }

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        ColumnWithTypeAndName nullable_col({}, typeFromString("Nullable(Int32)"), "i32_null", 2);
        auto                  col = nullable_col.type->createColumn();
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
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                }
                else if (itr.column_id == 2)
                {
                    const auto col    = typeid_cast<const ColumnNullable *>(c.get());
                    auto       nested = col->getNestedColumnPtr();
                    for (size_t i = 0; i < col->size(); i++)
                    {
                        if (i < 64)
                        {
                            EXPECT_FALSE(col->isNullAt(i));
                            EXPECT_EQ(nested->getInt(i), Int64(i));
                        }
                        else
                        {
                            EXPECT_TRUE(col->isNullAt(i));
                        }
                    }
                }
            }
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, num_rows_write);
        stream->readSuffix();
    }
}
CATCH


/// DDL test cases
class DMFile_DDL_Test : public DMFile_Test
{
public:
    /// Write some data into DMFile.
    /// return rows write, schema
    std::pair<size_t, ColumnDefines> prepareSomeDataToDMFile(bool i8_is_nullable = false)
    {
        size_t num_rows_write  = 128;
        auto   cols_before_ddl = DMTestEnv::getDefaultColumns();

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
            stream->writePrefix();
            stream->write(block, 0);
            stream->writeSuffix();

            return {num_rows_write, *cols_before_ddl};
        }
    }
};

TEST_F(DMFile_DDL_Test, AddColumn)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we add new column after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl     = cols_before_ddl;
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
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            ASSERT_TRUE(in.has(new_s_col.name));
            ASSERT_TRUE(in.has(new_i_col_with_default.name));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_s_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_s_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_s_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value = (*c)[i];
                        ASSERT_EQ(value.getType(), Field::Types::String);
                        // Empty default value
                        ASSERT_EQ(value, new_s_col.type->getDefault());
                    }
                }
                else if (itr.name == new_i_col_with_default.name)
                {
                    EXPECT_EQ(itr.column_id, new_i_col_with_default.id);
                    EXPECT_TRUE(itr.type->equals(*new_i_col_with_default.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = c->getInt(i);
                        ASSERT_EQ(value, 5); // Should fill with default value
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "i8")
                {
                    EXPECT_EQ(itr.column_id, 2L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Int8")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i * (-1 * (i % 2))));
                    }
                }
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_DDL_Test, UpcastColumnType)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> int32, and its name to "i8_new" after ddl
    auto cols_after_ddl        = std::make_shared<ColumnDefines>();
    *cols_after_ddl            = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type         = typeFromString("Int32");
    new_col.name         = "i32_new";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = c->getInt(Int64(i));
                        ASSERT_EQ(value, (Int64)(i * (-1 * (i % 2))));
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_DDL_Test, NotNullToNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> Nullable(int32), and its name to "i8_new" after ddl
    auto cols_after_ddl        = std::make_shared<ColumnDefines>();
    *cols_after_ddl            = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type         = typeFromString("Nullable(Int32)");
    new_col.name         = "i32_nullable";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = (*c)[i];
                        ASSERT_FALSE(value.isNull());
                        ASSERT_EQ(value, (Int64)(i * (-1 * (i % 2))));
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_DDL_Test, NullToNotNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile(true);

    // Mock that we achange a column type from Nullable(int8) -> int32, and its name to "i32" after ddl
    auto cols_after_ddl        = std::make_shared<ColumnDefines>();
    *cols_after_ddl            = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Nullable(Int8)")));
    ColumnDefine new_col = old_col;
    new_col.type         = typeFromString("Int32");
    new_col.name         = "i32";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            column_cache_,
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = (*c)[i];
                        if (i < num_rows_write / 2)
                        {
                            ASSERT_FALSE(value.isNull()) << " at row: " << i;
                            ASSERT_EQ(value, (Int64)(i * (-1 * (i % 2)))) << " at row: " << i;
                        }
                        else
                        {
                            ASSERT_FALSE(value.isNull()) << " at row: " << i;
                            ASSERT_EQ(value, (Int64)0) << " at row: " << i;
                        }
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
