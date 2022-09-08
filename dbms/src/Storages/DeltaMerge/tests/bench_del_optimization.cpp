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

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

namespace DB
{
namespace FailPoints
{
extern const char non_del_optimization[];
extern const char skip_check_segment_update[];
} // namespace FailPoints
namespace DM
{
namespace tests
{

class DeltaMergeStoreTestForBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State & /*state*/) override
    {
        // dropDataOnDisk
        try
        {
            const auto * path = "/Users/hongyunyan/Desktop/tiflash/build_release/dbms/bench_hyy/";
            if (Poco::File file(path); file.exists())
            {
                file.remove(true);
            }
            tiFlashStorageTestBasicReload();

            store = reload();
        }
        catch (DB::Exception const & e)
        {
            std::cerr << "exception: " << e.what() << " " << e.message() << std::endl;
        }
    }

    DeltaMergeStorePtr
    reload(const ColumnDefinesPtr & pre_define_columns = {}, bool is_common_handle = false, size_t rowkey_column_size = 1)
    {
        tiFlashStorageTestBasicReload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(*db_context,
                                                                 false,
                                                                 "test",
                                                                 "DeltaMergeStoreTest",
                                                                 100,
                                                                 *cols,
                                                                 handle_column_define,
                                                                 is_common_handle,
                                                                 rowkey_column_size,
                                                                 DeltaMergeStore::Settings());
        return s;
    }

    void tiFlashStorageTestBasicReload(DB::Settings && db_settings = DB::Settings())
    {
        String path = "/Users/hongyunyan/Desktop/tiflash/build_release/dbms/bench_hyy/";
        Strings test_paths;
        test_paths.emplace_back(path);
        // test_paths.push_back(base::TiFlashStorageTestBasic::getTemporaryPath());
        db_context = std::make_unique<Context>(TiFlashTestEnv::getContext(db_settings, test_paths));
    }

protected:
    DeltaMergeStorePtr store;
    std::unique_ptr<Context> db_context;
};

static constexpr const char * pk_name = "_tidb_rowid";

std::set<int> createRandomNum(size_t min_value, size_t max_value, size_t num)
{
    std::set<int> res;

    std::random_device rd;
    std::default_random_engine eng(rd());
    std::uniform_int_distribution<int> distr(min_value, max_value - 1);

    while (res.size() < num)
    {
        int value = distr(eng);
        if (res.find(value) == res.end())
        {
            res.insert(value);
        }
    }

    return res;
}

Block createBlock(size_t rows, size_t columns, size_t delete_rows, size_t begin_value = 0)
{
    Block block;
    // 单独插入 handle/version/tag 列

    // int_pk_col

    block.insert(ColumnWithTypeAndName{
        DB::tests::makeColumn<Int64>(EXTRA_HANDLE_COLUMN_INT_TYPE, createNumbers<Int64>(begin_value, begin_value + rows)),
        EXTRA_HANDLE_COLUMN_INT_TYPE,
        pk_name,
        EXTRA_HANDLE_COLUMN_ID});
    // version_col
    block.insert(DB::tests::createColumn<UInt64>(
        std::vector<UInt64>(rows, 2),
        VERSION_COLUMN_NAME,
        VERSION_COLUMN_ID));

    // tag_col
    std::vector<UInt64> tags(rows, 0);
    auto delete_set = createRandomNum(0, rows, delete_rows);
    for (auto i : delete_set)
    {
        tags[i] = 1;
    }
    block.insert(DB::tests::createColumn<UInt8>(
        std::move(tags),
        TAG_COLUMN_NAME,
        TAG_COLUMN_ID));

    // other columns
    for (size_t i = 3; i < columns; ++i)
    {
        auto name = "column" + std::to_string(i);
        block.insert(DB::tests::createColumn<UInt64>(
            std::vector<UInt64>(rows, 200),
            name,
            i));
    }
    return block;
}

BENCHMARK_DEFINE_F(DeltaMergeStoreTestForBench, ReadWithoutDelOptimization)
(benchmark::State & state)
{
    auto block_rows = state.range(0);
    auto columns_num = state.range(1);
    auto delete_rows_num = state.range(2);
    auto file_num = state.range(3);

    (void)fiu_init(0);
    FailPointHelper::enableFailPoint(FailPoints::non_del_optimization);
    FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);

    auto begin_value = 0;
    Block block = createBlock(block_rows, columns_num, delete_rows_num, begin_value);
    begin_value += block_rows;

    ColumnDefines columns = getColumnDefinesFromBlock(block);

    ColumnDefinesPtr read_columns = std::make_shared<ColumnDefines>();
    for (auto col : columns)
    {
        if (col.id >= 3)
        {
            read_columns->emplace_back(std::move(col));
        }
    }

    store = reload(read_columns);
    store->write(*db_context, db_context->getSettingsRef(), block);

    for (int i = 1; i < file_num; i++)
    {
        Block block = createBlock(block_rows, columns_num, delete_rows_num, begin_value);
        store->write(*db_context, db_context->getSettingsRef(), block);
        begin_value += block_rows;
    }

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    store->mergeDeltaAll(*db_context);

    Block block2 = createBlock(block_rows, columns_num, delete_rows_num, block_rows);
    store->write(*db_context, db_context->getSettingsRef(), block2);

    for (auto _ : state)
    {
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             *read_columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             "",
                                             /* keep_order= */ false,
                                             /* is_fast_mode= */ false,
                                             /* expected_block_size= */ 1024,
                                             {},
                                             InvalidColumnID)[0];
        while (in->read()) {};
    }
    FailPointHelper::disableFailPoint(FailPoints::non_del_optimization);
    FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
}

BENCHMARK_DEFINE_F(DeltaMergeStoreTestForBench, ReadWithDelOptimization)
(benchmark::State & state)
{
    auto block_rows = state.range(0);
    auto columns_num = state.range(1);
    auto delete_rows_num = state.range(2);
    auto file_num = state.range(3);

    (void)fiu_init(0);
    FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);

    auto begin_value = 0;
    Block block = createBlock(block_rows, columns_num, delete_rows_num, begin_value);
    begin_value += block_rows;

    ColumnDefines columns = getColumnDefinesFromBlock(block);

    ColumnDefinesPtr read_columns = std::make_shared<ColumnDefines>();
    for (auto col : columns)
    {
        if (col.id >= 3)
        {
            read_columns->emplace_back(std::move(col));
        }
    }

    store = reload(read_columns);
    store->write(*db_context, db_context->getSettingsRef(), block);

    for (int i = 1; i < file_num; i++)
    {
        Block block = createBlock(block_rows, columns_num, delete_rows_num, begin_value);
        store->write(*db_context, db_context->getSettingsRef(), block);
        begin_value += block_rows;
    }

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    Block block2 = createBlock(block_rows, columns_num, delete_rows_num, begin_value);
    store->write(*db_context, db_context->getSettingsRef(), block2);

    for (auto _ : state)
    {
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             *read_columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             "",
                                             /* keep_order= */ false,
                                             /* is_fast_mode= */ false,
                                             /* expected_block_size= */ 1024,
                                             {},
                                             InvalidColumnID)[0];

        while (in->read()) {};
    }

    FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
}

BENCHMARK_DEFINE_F(DeltaMergeStoreTestForBench, DMFileReadWithDelOptimization)
(benchmark::State & state)
{
    auto block_rows = state.range(0);
    auto columns_num = state.range(1);
    auto delete_rows_num = state.range(2);

    Block block = createBlock(block_rows, columns_num, delete_rows_num);
    ColumnDefines columns = getColumnDefinesFromBlock(block);

    ColumnDefinesPtr read_columns = std::make_shared<ColumnDefines>(columns);

    store = reload(read_columns);

    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto [store_path, file_id] = store->preAllocateIngestFile();

    DMFileBlockOutputStream::Flags flags;
    flags.setSingleFile(false);

    auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());

    auto dmfile = writeIntoNewDMFile(
        *dm_context,
        std::make_shared<ColumnDefines>(columns),
        input_stream,
        file_id,
        store_path,
        flags);

    auto column_cache = std::make_shared<ColumnCache>();

    DMFileBlockInputStreamBuilder builder(*db_context);

    for (auto _ : state)
    {
        auto stream = builder.setColumnCache(column_cache)
                          .enableCleanRead(false, false, true, std::numeric_limits<UInt64>::max())
                          .build(dmfile, *read_columns, RowKeyRanges{RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())});
        stream->readPrefix();
        stream->read();
        stream->readSuffix();
    }
}

BENCHMARK_DEFINE_F(DeltaMergeStoreTestForBench, DMFileReadWithoutDelOptimization)
(benchmark::State & state)
{
    auto block_rows = state.range(0);
    auto columns_num = state.range(1);
    auto delete_rows_num = state.range(2);

    Block block = createBlock(block_rows, columns_num, delete_rows_num);
    ColumnDefines columns = getColumnDefinesFromBlock(block);

    ColumnDefinesPtr read_columns = std::make_shared<ColumnDefines>(columns);

    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto [store_path, file_id] = store->preAllocateIngestFile();

    DMFileBlockOutputStream::Flags flags;
    flags.setSingleFile(false);

    auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
    auto dmfile = writeIntoNewDMFile(
        *dm_context,
        std::make_shared<ColumnDefines>(columns),
        input_stream,
        file_id,
        store_path,
        flags);

    auto column_cache = std::make_shared<ColumnCache>();

    DMFileBlockInputStreamBuilder builder(*db_context);

    for (auto _ : state)
    {
        auto stream = builder.setColumnCache(column_cache)
                          .enableCleanRead(false, false, false, std::numeric_limits<UInt64>::max())
                          .build(dmfile, *read_columns, RowKeyRanges{RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())});
        stream->readPrefix();
        stream->read();
        stream->readSuffix();
    }
}

constexpr size_t num_iterations_test = 10000;

BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, DMFileReadWithoutDelOptimization)->Iterations(num_iterations_test)->Args({50000, 20, 0})->Args({50000, 15, 0})->Args({50000, 10, 0})->Args({50000, 8, 0})->Args({50000, 5, 0});
BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, DMFileReadWithDelOptimization)->Iterations(num_iterations_test)->Args({50000, 20, 0})->Args({50000, 15, 0})->Args({50000, 10, 0})->Args({50000, 8, 0})->Args({50000, 5, 0});

BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, ReadWithoutDelOptimization)->Iterations(num_iterations_test)->Args({5000, 20, 0, 1})->Args({5000, 15, 0, 1})->Args({5000, 10, 0, 1})->Args({5000, 8, 0, 1})->Args({5000, 5, 0, 1})->Args({5000, 20, 0, 10})->Args({5000, 15, 0, 10})->Args({5000, 10, 0, 10})->Args({5000, 8, 0, 10})->Args({5000, 5, 0, 10})->Args({5000, 20, 0, 20})->Args({5000, 15, 0, 20})->Args({5000, 10, 0, 20})->Args({5000, 8, 0, 20})->Args({5000, 5, 0, 20})->Args({5000, 20, 0, 30})->Args({5000, 15, 0, 30})->Args({5000, 10, 0, 30})->Args({5000, 8, 0, 30})->Args({5000, 5, 0, 30})->Args({5000, 20, 0, 50})->Args({5000, 15, 0, 50})->Args({5000, 10, 0, 50})->Args({5000, 8, 0, 50})->Args({5000, 5, 0, 50});
BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, ReadWithDelOptimization)->Iterations(num_iterations_test)->Args({5000, 20, 0, 1})->Args({5000, 15, 0, 1})->Args({5000, 10, 0, 1})->Args({5000, 8, 0, 1})->Args({5000, 5, 0, 1})->Args({5000, 20, 0, 10})->Args({5000, 15, 0, 10})->Args({5000, 10, 0, 10})->Args({5000, 8, 0, 10})->Args({5000, 5, 0, 10})->Args({5000, 20, 0, 20})->Args({5000, 15, 0, 20})->Args({5000, 10, 0, 20})->Args({5000, 8, 0, 20})->Args({5000, 5, 0, 20})->Args({5000, 20, 0, 30})->Args({5000, 15, 0, 30})->Args({5000, 10, 0, 30})->Args({5000, 8, 0, 30})->Args({5000, 5, 0, 30})->Args({5000, 20, 0, 50})->Args({5000, 15, 0, 50})->Args({5000, 10, 0, 50})->Args({5000, 8, 0, 50})->Args({5000, 5, 0, 50});

// BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, DMFileReadWithoutDelOptimization)->Iterations(num_iterations_test)->Args({50000, 5, 0});
//BENCHMARK_REGISTER_F(DeltaMergeStoreTestForBench, DMFileReadWithDelOptimization)->Iterations(num_iterations_test)->Args({50000, 5, 0});

} // namespace tests
} // namespace DM
} // namespace DB
