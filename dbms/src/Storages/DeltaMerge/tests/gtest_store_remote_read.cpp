#include <Flash/Disaggregated/PageTunnel.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/Page/PageDefines.h>

namespace DB::DM::tests
{


TEST_P(DeltaMergeStoreRWTest, StoreDisaggregatedRead)
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);

        store = reload(table_column_defines);
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col2:String for test
            block.insert(DB::tests::createColumn<String>(
                createNumberStrings(0, num_rows_write),
                col_str_define.name,
                col_str_define.id));
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_i8_define.name,
                col_i8_define.id));
        }

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
            store->write(*db_context, db_context->getSettingsRef(), block);
            break;
        default:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        }
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    {
        // read all columns from store
        // const auto & columns = store->getTableColumns();
        auto table_read_snap = store->buildRemoteReadSnapshot(
            *db_context,
            db_context->getSettingsRef(),
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            EMPTY_FILTER,
            /*num_streams=*/1,
            TRACING_NAME);

        for (const auto & seg_task : table_read_snap->getTasks())
        {
            auto delta_vs = seg_task->read_snapshot->delta;
            auto mem_table = delta_vs->getMemTableSetSnapshot();
            auto read_ids = getCFTinyIds(delta_vs->getPersistedFileSetSnapshot());
            auto tunnel = std::make_unique<PageTunnel>(seg_task, read_ids);
            auto packet = tunnel->readPacket();
            EXPECT_EQ(packet.pages_size(), read_ids.size());
            EXPECT_EQ(packet.chunks_size(), mem_table->getColumnFileCount());
        }
    }
}

} // namespace DB::DM::tests
