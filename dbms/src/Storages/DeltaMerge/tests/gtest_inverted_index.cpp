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

    static void build()
    {
        LocalIndexInfo index_info{
            0,
            0,
            std::make_shared<TiDB::InvertedIndexDefinition>(std::is_signed_v<T>, sizeof(T)),
        };
        InvertedIndexBuilder<T> builder(index_info);
        auto write_block =
            [&builder](DB::tests::InferredDataVector<T> && values, DB::tests::InferredDataVector<UInt8> && del_marks) {
                auto col = DB::tests::createColumn<T>(std::move(values)).column;
                auto del_mark_col = DB::tests::createColumn<UInt8>(std::move(del_marks)).column;
                const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
                builder.addBlock(*col, del_mark, []() { return true; });
            };
        write_block({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
        write_block({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        write_block({1, 2, 2, 2, 3, 3, 3, 4, 4, 4}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        builder.saveToFile(IndexFileName);
    }

    static void search(const InvertedIndexViewerPtr & viewer)
    {
        auto v_search = [&viewer](const UInt32 key, const String & expected) {
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

        auto v_search_range = [&viewer](const UInt32 start, const UInt32 end, const String & expected) {
            auto bitmap_filter = std::make_shared<BitmapFilter>(30, false);
            viewer->searchRange(bitmap_filter, start, end);
            ASSERT_EQ(bitmap_filter->toDebugString(), expected);
        };
        v_search_range(1, 3, "100000000011000000001111000000");
        v_search_range(2, 4, "001000000001100000000111111000");
        v_search_range(10, 11, "000000000000000000010000000000");
        v_search_range(1, 12, "101010101011111111111111111111");
    }

    static void memorySearch()
    {
        auto viewer = std::make_shared<InvertedIndexMemoryViewer<T>>(IndexFileName);
        search(viewer);
    }

    static void fileSearch()
    {
        auto viewer = std::make_shared<InvertedIndexFileViewer<T>>(IndexFileName);
        search(viewer);
    }

    static void run()
    {
        build();
        memorySearch();
        fileSearch();

        Poco::File(IndexFileName).remove();
    }
};

TEST(InvertedIndex, Simple)
try
{
    InvertedIndexTest<UInt8>::run();
    InvertedIndexTest<UInt16>::run();
    InvertedIndexTest<UInt32>::run();
    InvertedIndexTest<UInt64>::run();
    InvertedIndexTest<Int8>::run();
    InvertedIndexTest<Int16>::run();
    InvertedIndexTest<Int32>::run();
    InvertedIndexTest<Int64>::run();
}
CATCH

} // namespace DB::DM::tests
