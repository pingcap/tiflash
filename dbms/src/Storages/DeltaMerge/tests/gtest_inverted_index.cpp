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
            std::make_shared<TiDB::InvertedIndexDefinition>(std::is_signed_v<T>, sizeof(T))};
        InvertedIndexBuilder<T> builder(index_info);
        {
            auto col = DB::tests::createColumn<T>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).column;
            auto del_mark_col = DB::tests::createColumn<UInt8>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1}).column;
            const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
            builder.addBlock(*col, del_mark, []() { return true; });
        }
        {
            auto col = DB::tests::createColumn<T>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).column;
            auto del_mark_col = DB::tests::createColumn<UInt8>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).column;
            const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
            builder.addBlock(*col, del_mark, []() { return true; });
        }
        {
            auto col = DB::tests::createColumn<T>({1, 2, 2, 2, 3, 3, 3, 4, 4, 4}).column;
            auto del_mark_col = DB::tests::createColumn<UInt8>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).column;
            const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
            builder.addBlock(*col, del_mark, []() { return true; });
        }
        builder.saveToFile(IndexFileName);
    }

    static void search(const InvertedIndexViewerPtr & viewer)
    {
        ASSERT_EQ(viewer->search(1).size(), 3);
        ASSERT_EQ(viewer->search(2).size(), 4);
        ASSERT_EQ(viewer->search(3).size(), 5);
        ASSERT_EQ(viewer->search(4).size(), 4);
        ASSERT_EQ(viewer->search(5).size(), 2);
        ASSERT_EQ(viewer->search(6).size(), 1);
        ASSERT_EQ(viewer->search(7).size(), 2);
        ASSERT_EQ(viewer->search(8).size(), 1);
        ASSERT_EQ(viewer->search(9).size(), 2);
        ASSERT_EQ(viewer->search(10).size(), 1);
        ASSERT_EQ(viewer->search(11).size(), 0);
        ASSERT_EQ(viewer->searchRange(1, 3).size(), 7);
        ASSERT_EQ(viewer->searchRange(2, 4).size(), 9);
        ASSERT_EQ(viewer->searchRange(10, 11).size(), 1);
        ASSERT_EQ(viewer->searchRange(1, 12).size(), 25);
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
