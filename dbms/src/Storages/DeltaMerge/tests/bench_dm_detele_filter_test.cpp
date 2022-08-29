// // Copyright 2022 PingCAP, Ltd.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// #include <benchmark/benchmark.h>
// #include <gtest/gtest.h>
// #include <Core/Block.h>
// #include <Columns/IColumn.h>
// #include <Columns/ColumnsCommon.h>
// #include <TestUtils/FunctionTestUtils.h>
// #include <Storages/DeltaMerge/DMDecoratorStreams.h>
// #include <DataStreams/BlocksListBlockInputStream.h>
// #include <Storages/DeltaMerge/tests/DMTestEnv.h>
// #include <Storages/DeltaMerge/RowKeyFilter.h>


// namespace DB
// {
// namespace DM
// {
// namespace tests {


// BlockInputStreamPtr genTwiceFilterInputStream(BlocksList & blocks, const ColumnDefines & columns, const RowKeyRanges& key_ranges)
// {
//     BlockInputStreamPtr stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
//     stream = std::make_shared<DMRowKeyFilterBlockInputStream<false>>(stream, key_ranges, 0);
//     stream = std::make_shared<DMDeleteFilterBlockInputStream>(stream, columns);
//     return stream;
// }

// BlockInputStreamPtr genOnceFilterInputStream(BlocksList & blocks, const ColumnDefines & columns, const RowKeyRanges& key_ranges)
// {
//     BlockInputStreamPtr stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
//     stream = std::make_shared<DMFilterAllBlockInputStream>(stream, columns, key_ranges, 0, "");
//     return stream;
// }



// // 构造几个 block，有的有 delete，有的是 插入，然后 flush compact merge，让他们一起用最后两个 filter 来过滤
// static constexpr const char * pk_name = "_tidb_rowid";

// std::set<int> createRandomNum(size_t min_value, size_t max_value, size_t num){
//     std::set<int> res;

//     std::random_device rd;
//     std::default_random_engine eng(rd());
//     std::uniform_int_distribution<int> distr(min_value, max_value - 1);

//     while (res.size() <  num) {
//         int value = distr(eng);
//         if (res.find(value) == res.end()) {
//             res.insert(value);
//         }
//     }

//     return res;
// }

// Block createBlock(size_t rows, size_t columns, size_t row_filter_rows, size_t delete_rows) {
//     Block block;
//     // 单独插入 handle/version/tag 列

//     // int_pk_col
//     block.insert(ColumnWithTypeAndName{
//                 DB::tests::makeColumn<Int64>(EXTRA_HANDLE_COLUMN_INT_TYPE, createNumbers<Int64>(0, rows)),
//                 EXTRA_HANDLE_COLUMN_INT_TYPE,
//                 pk_name,
//                 EXTRA_HANDLE_COLUMN_ID});
//     // version_col
//     block.insert(DB::tests::createColumn<UInt64>(
//         std::vector<UInt64>(rows, 2),
//         VERSION_COLUMN_NAME,
//         VERSION_COLUMN_ID));
    
//     // tag_col
//     std::vector<UInt64> tags(rows, 0);
//     auto delete_set = createRandomNum(row_filter_rows, rows, delete_rows);
//     for (auto i : delete_set) {
//         tags[i] = 1;
//     }
//     block.insert(DB::tests::createColumn<UInt8>(
//         std::move(tags),
//         TAG_COLUMN_NAME,
//         TAG_COLUMN_ID));

//     // other columns
//     for (size_t i = 3; i < columns; ++i) {
//         auto name = "column" + std::to_string(i);
//         block.insert(DB::tests::createColumn<UInt64>(
//                 std::vector<UInt64>(rows, 200),
//                 name,
//                 i));
//     }
//     return block;
// }

// constexpr size_t num_iterations_test = 100000000;

// static void benchFilterOriginBlockInputStream(benchmark::State & state)
// {
//     auto block_rows = state.range(0);
//     auto columns_num = state.range(1);
//     auto filter_count_first = state.range(2);
//     auto filter_count_second = state.range(3);

//     BlocksList blocks;

//     auto block = createBlock(block_rows, columns_num, filter_count_first, filter_count_second);

//     WriteBufferFromOwnString start_key_ss;
//     DB::EncodeInt64(filter_count_first, start_key_ss);

//     WriteBufferFromOwnString end_key_ss;
//     DB::EncodeInt64(block_rows, end_key_ss);

//     RowKeyRanges key_ranges{RowKeyRange(
//         RowKeyValue(false, std::make_shared<String>(start_key_ss.releaseStr()), /*int_val_*/ filter_count_first),
//         RowKeyValue(false, std::make_shared<String>(end_key_ss.releaseStr()), /*int_val_*/ block_rows),
//         false,
//         1)};

//     blocks.emplace_back(std::move(block));

//     ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

//     auto in = genTwiceFilterInputStream(blocks, columns, key_ranges);

//     std::vector<std::shared_ptr<DB::IBlockInputStream>> instream_vec(num_iterations_test, in);

//     int index = 0;
//     for (auto _ : state)
//     {
//         auto block = instream_vec[index]->read();
//         // if (index == 0){
//         //     std::cout << " block.rows() " << block.rows() << std::endl;
//         // }
//         index++;
//     }
// }

// static void benchFilterMergedBlockInputStream(benchmark::State & state)
// {
//     auto block_rows = state.range(0);
//     auto columns_num = state.range(1);
//     auto filter_count_first = state.range(2);
//     auto filter_count_second = state.range(3);

//     BlocksList blocks;

//     auto block = createBlock(block_rows, columns_num, filter_count_first, filter_count_second);

//     WriteBufferFromOwnString start_key_ss;
//     DB::EncodeInt64(filter_count_first, start_key_ss);

//     WriteBufferFromOwnString end_key_ss;
//     DB::EncodeInt64(block_rows, end_key_ss);

//     RowKeyRanges key_ranges{RowKeyRange(
//         RowKeyValue(false, std::make_shared<String>(start_key_ss.releaseStr()), /*int_val_*/ filter_count_first),
//         RowKeyValue(false, std::make_shared<String>(end_key_ss.releaseStr()), /*int_val_*/ block_rows),
//         false,
//         1)};
//     blocks.emplace_back(std::move(block));

//     ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

//     auto in = genOnceFilterInputStream(blocks, columns, key_ranges);

//     std::vector<std::shared_ptr<DB::IBlockInputStream>> instream_vec(num_iterations_test, in);

//     int index = 0;
//     for (auto _ : state)
//     {
//         auto block = instream_vec[index]->read();
//         // if (index == 0){
//         //     std::cout << " block.rows() " << block.rows() << std::endl;
//         // }
//         index++;
//     }
// }

// BENCHMARK(benchFilterOriginBlockInputStream)->Iterations(num_iterations_test)->Args({1000,5,10,10})->Args({1000,5,10,10})->Args({1000,5,100,100})->Args({1000,10,10,10})->Args({1000,30,10,10})->Args({500,10,10,10})->Args({5000,10,10,10});
// BENCHMARK(benchFilterMergedBlockInputStream)->Iterations(num_iterations_test)->Args({1000,5,10,10})->Args({1000,5,10,10})->Args({1000,5,100,100})->Args({1000,10,10,10})->Args({1000,30,10,10})->Args({500,10,10,10})->Args({5000,10,10,10});



// }
// }
// }