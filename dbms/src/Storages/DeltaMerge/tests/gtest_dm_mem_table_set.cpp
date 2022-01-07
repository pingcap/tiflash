#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/MemTableSet.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <memory>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{
class MemTableSet_test : public DB::base::TiFlashStorageTestBasic
{
public:
    MemTableSet_test()
        : storage_pool()
    {}

public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        table_columns_ = std::make_shared<ColumnDefines>();

        reload();
    }

protected:
    void reload(const ColumnDefinesPtr & pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_unique<StoragePool>("test.t1", *storage_path_pool, *db_context, db_context->getSettingsRef());
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns_ = *columns;

        dm_context_ = std::make_unique<DMContext>(*db_context,
                                                  *storage_path_pool,
                                                  *storage_pool,
                                                  0,
                                                  /*min_version_*/ 0,
                                                  settings.not_compress_columns,
                                                  false,
                                                  1,
                                                  db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns_; }

    DMContext & dmContext() { return *dm_context_; }

protected:
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePathPool> storage_path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns_;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context_;
};

TEST_F(MemTableSet_test, WriteRead)
{
    auto mem_table_set = std::make_shared<MemTableSet>();
    const size_t num_rows_write = 100;
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    SipHash block_hash;
    block.updateHash(block_hash);
    {
        mem_table_set->write(dmContext(), block, 0, num_rows_write);
    }


    {
        auto mem_table_snapshot = mem_table_set->createSnapshot();
        auto rows = mem_table_snapshot->getRows();
        ASSERT_EQ(rows, num_rows_write);

        auto mem_table_reader = std::make_shared<ColumnFileSetReader>(
            dmContext(),
            mem_table_snapshot,
            table_columns_,
            RowKeyRange::newAll(false, 1));
        auto columns = block.cloneEmptyColumns();
        size_t rows_read = mem_table_reader->readRows(columns, 0, num_rows_write, nullptr);
        ASSERT_EQ(rows_read, num_rows_write);
        Block result = block.cloneWithColumns(std::move(columns));
        ASSERT_EQ(result.rows(), num_rows_write);
        SipHash result_hash;
        result.updateHash(result_hash);
        ASSERT_EQ(block_hash.get64(), result_hash.get64());
    }
}
}
}
}