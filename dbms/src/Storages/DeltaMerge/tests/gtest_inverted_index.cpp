// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Index/InvertedIndex.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <random>
#include <type_traits>


namespace DB::DM::tests
{

template <typename T>
class InvertedIndexTest
{
public:
    static constexpr auto IndexFileName = "test.inverted_index";

    InvertedIndexTest() = default;

    ~InvertedIndexTest() = default;

    static void writeBlock(
        InvertedIndexBuilder<T> & builder,
        const DB::tests::InferredDataVector<T> & values,
        const DB::tests::InferredDataVector<UInt8> & del_marks)
    {
        auto col = DB::tests::createColumn<T>(values).column;
        auto del_mark_col = DB::tests::createColumn<UInt8>(del_marks).column;
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
        builder.addBlock(*col, del_mark, []() { return true; });
    }

    static InvertedIndexBuilder<T> createBuilder()
    {
        LocalIndexInfo index_info{
            0,
            0,
            std::make_shared<TiDB::InvertedIndexDefinition>(std::is_signed_v<T>, sizeof(T)),
        };
        return InvertedIndexBuilder<T>(index_info);
    }

    class SimpleTestCase
    {
        static void search(const InvertedIndexViewerPtr & viewer)
        {
            auto v_search = [&viewer](const UInt64 key, const String & expected) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(30, false);
                viewer->search(bitmap_filter, key);
                ASSERT_EQ(bitmap_filter->toDebugString(), expected);
            };
            v_search(1, "100000000010000000001000000000");
            v_search(2, "000000000001000000000111000000");
            v_search(3, "001000000000100000000000111000");
            v_search(4, "000000000000010000000000000111");
            v_search(5, "000010000000001000000000000000");
            v_search(6, "000000000000000100000000000000");
            v_search(7, "000000100000000010000000000000");
            v_search(8, "000000000000000001000000000000");
            v_search(9, "000000001000000000100000000000");
            v_search(10, "000000000000000000010000000000");
            v_search(11, "000000000000000000000000000000");

            auto v_search_range = [&viewer](const UInt64 start, const UInt64 end, const String & expected) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(30, false);
                viewer->searchRange(bitmap_filter, start, end);
                ASSERT_EQ(bitmap_filter->toDebugString(), expected);
            };
            v_search_range(1, 2, "100000000011000000001111000000");
            v_search_range(2, 3, "001000000001100000000111111000");
            v_search_range(10, 10, "000000000000000000010000000000");
            v_search_range(1, 11, "101010101011111111111111111111");
        }

    public:
        static void run()
        {
            {
                auto builder = createBuilder();
                writeBlock(builder, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
                writeBlock(builder, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
                writeBlock(builder, {1, 2, 2, 2, 3, 3, 3, 4, 4, 4}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
                builder.saveToFile(IndexFileName);
            }
            {
                auto viewer = std::make_shared<InvertedIndexMemoryViewer<T>>(IndexFileName);
                search(viewer);
            }
            {
                auto viewer = std::make_shared<InvertedIndexFileViewer<T>>(IndexFileName);
                search(viewer);
            }
            Poco::File(IndexFileName).remove();
        }
    };


    class LargeTestCase
    {
        static constexpr UInt32 block_size = 10000;
        static constexpr T block_count = 100;

        static void search(const InvertedIndexViewerPtr & viewer)
        {
            auto v_search = [&viewer](const UInt64 key, const size_t expected_count) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(block_size * block_count, false);
                viewer->search(bitmap_filter, key);
                ASSERT_EQ(bitmap_filter->count(), expected_count);
            };
            std::mt19937 generator;
            {
                std::uniform_int_distribution<T> distribution(0, block_count);
                for (UInt32 i = 0; i < 10; ++i)
                    v_search(distribution(generator), block_size);
            }
            {
                std::uniform_int_distribution<T> distribution(
                    std::numeric_limits<T>::min(),
                    std::numeric_limits<T>::max());
                for (UInt32 i = 0; i < 10; ++i)
                {
                    auto random_v = distribution(generator);
                    v_search(random_v, (random_v >= block_count || random_v < 0) ? 0 : block_size);
                }
            }
            auto v_search_range = [&viewer](const UInt64 start, const UInt64 end, const size_t expected_count) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(block_size * block_count, false);
                viewer->searchRange(bitmap_filter, start, end);
                ASSERT_EQ(bitmap_filter->count(), expected_count);
            };

            v_search_range(1, 2, 2 * block_size);
            v_search_range(2, 3, 2 * block_size);
            v_search_range(10, 10, block_size);
            v_search_range(71, 72, 2 * block_size);
            v_search_range(1, 99, 99 * block_size);
            v_search_range(0, 100, 100 * block_size);
            v_search_range(99, 104, 1 * block_size);
            v_search_range(100, 104, 0);
        }

        static void searchMultiThread(const InvertedIndexViewerPtr & viewer)
        {
            auto v_search = [&viewer](const UInt64 key, const size_t expected_count) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(block_size * block_count, false);
                viewer->search(bitmap_filter, key);
                ASSERT_EQ(bitmap_filter->count(), expected_count);
            };
            std::mt19937 generator;
            std::vector<std::thread> threads;
            {
                std::uniform_int_distribution<T> distribution(0, block_count);
                for (UInt32 i = 0; i < 10; ++i)
                {
                    threads.emplace_back([&v_search, &generator, &distribution]() {
                        auto random_v = distribution(generator);
                        v_search(random_v, block_size);
                    });
                }
            }
            {
                std::uniform_int_distribution<T> distribution(
                    std::numeric_limits<T>::min(),
                    std::numeric_limits<T>::max());
                for (UInt32 i = 0; i < 10; ++i)
                {
                    threads.emplace_back([&v_search, &generator, &distribution]() {
                        auto random_v = distribution(generator);
                        v_search(random_v, (random_v >= block_count || random_v < 0) ? 0 : block_size);
                    });
                }
            }

            auto v_search_range = [&viewer](const UInt64 start, const UInt64 end, const size_t expected_count) {
                auto bitmap_filter = std::make_shared<BitmapFilter>(block_size * block_count, false);
                viewer->searchRange(bitmap_filter, start, end);
                ASSERT_EQ(bitmap_filter->count(), expected_count);
            };
            threads.emplace_back([&v_search_range]() { v_search_range(1, 2, 2 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(2, 3, 2 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(10, 10, block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(71, 72, 2 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(1, 99, 99 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(0, 100, 100 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(99, 104, 1 * block_size); });
            threads.emplace_back([&v_search_range]() { v_search_range(100, 104, 0); });

            for (auto & thread : threads)
                thread.join();
        }

    public:
        static void run()
        {
            {
                auto builder = createBuilder();
                for (UInt32 i = 0; i < block_count; ++i)
                {
                    DB::tests::InferredDataVector<T> values(block_size, i);
                    DB::tests::InferredDataVector<UInt8> del_marks(block_size, 0);
                    writeBlock(builder, values, del_marks);
                }
                builder.saveToFile(IndexFileName);
            }
            {
                auto viewer = std::make_shared<InvertedIndexMemoryViewer<T>>(IndexFileName);
                search(viewer);
            }
            {
                auto viewer = std::make_shared<InvertedIndexFileViewer<T>>(IndexFileName);
                search(viewer);
            }
            Poco::File(IndexFileName).remove();
        }

        static void runMultiThread()
        {
            {
                auto builder = createBuilder();
                for (UInt32 i = 0; i < block_count; ++i)
                {
                    DB::tests::InferredDataVector<T> values(block_size, i);
                    DB::tests::InferredDataVector<UInt8> del_marks(block_size, 0);
                    writeBlock(builder, values, del_marks);
                }
                builder.saveToFile(IndexFileName);
            }
            {
                auto viewer = std::make_shared<InvertedIndexMemoryViewer<T>>(IndexFileName);
                searchMultiThread(viewer);
            }
            {
                auto viewer = std::make_shared<InvertedIndexFileViewer<T>>(IndexFileName);
                searchMultiThread(viewer);
            }
            Poco::File(IndexFileName).remove();
        }
    };
};

TEST(InvertedIndex, Simple)
try
{
    InvertedIndexTest<UInt8>::SimpleTestCase::run();
    InvertedIndexTest<UInt16>::SimpleTestCase::run();
    InvertedIndexTest<UInt32>::SimpleTestCase::run();
    InvertedIndexTest<UInt64>::SimpleTestCase::run();
    InvertedIndexTest<Int8>::SimpleTestCase::run();
    InvertedIndexTest<Int16>::SimpleTestCase::run();
    InvertedIndexTest<Int32>::SimpleTestCase::run();
    InvertedIndexTest<Int64>::SimpleTestCase::run();
}
CATCH

// Split the large test case into two parts to avoid long running time.

TEST(InvertedIndex, Large1)
try
{
    InvertedIndexTest<UInt8>::LargeTestCase::run();
    InvertedIndexTest<UInt16>::LargeTestCase::run();
    InvertedIndexTest<UInt32>::LargeTestCase::run();
    InvertedIndexTest<UInt64>::LargeTestCase::run();
}
CATCH

TEST(InvertedIndex, Large2)
try
{
    InvertedIndexTest<Int8>::LargeTestCase::run();
    InvertedIndexTest<Int16>::LargeTestCase::run();
    InvertedIndexTest<Int32>::LargeTestCase::run();
    InvertedIndexTest<Int64>::LargeTestCase::run();
}
CATCH

TEST(InvertedIndex, MultipleThreads1)
try
{
    InvertedIndexTest<UInt8>::LargeTestCase::runMultiThread();
    InvertedIndexTest<UInt16>::LargeTestCase::runMultiThread();
    InvertedIndexTest<UInt32>::LargeTestCase::runMultiThread();
    InvertedIndexTest<UInt64>::LargeTestCase::runMultiThread();
}
CATCH

TEST(InvertedIndex, MultipleThreads2)
try
{
    InvertedIndexTest<Int8>::LargeTestCase::runMultiThread();
    InvertedIndexTest<Int16>::LargeTestCase::runMultiThread();
    InvertedIndexTest<Int32>::LargeTestCase::runMultiThread();
    InvertedIndexTest<Int64>::LargeTestCase::runMultiThread();
}
CATCH

} // namespace DB::DM::tests
