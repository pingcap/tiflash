// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::DM::tests
{

class FakeValueSpace
{
    using ValueSpace = FakeValueSpace;

public:
    static void removeFromInsert(UInt64 /*id*/) {}
};
using FakeValueSpacePtr = std::shared_ptr<FakeValueSpace>;
using FakeDeltaTree = DeltaTree<FakeValueSpace, 2, 10>;

class DeltaTreeTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        CurrentMemoryTracker::disableThreshold();
        memory_tracker = MemoryTracker::create();
        memory_tracker_setter.emplace(true, memory_tracker.get());
        ASSERT_EQ(current_memory_tracker->get(), 0);

        fake_tree = std::make_unique<FakeDeltaTree>();
        ASSERT_EQ(fake_tree->getBytes(), sizeof(FakeDeltaTree::Leaf));
        ASSERT_EQ(current_memory_tracker->get(), sizeof(FakeDeltaTree::Leaf));
    }

    void TearDown() override { DB::FailPointHelper::disableFailPoint(DB::FailPoints::delta_tree_create_node_fail); }

    std::unique_ptr<FakeDeltaTree> fake_tree;
    MemoryTrackerPtr memory_tracker;
    std::optional<MemoryTrackerSetter> memory_tracker_setter;
};

std::string treeToString(const FakeDeltaTree & tree)
{
    std::string result;
    std::string temp;
    for (auto it = tree.begin(), end = tree.end(); it != end; ++it)
    {
        temp = "";
        temp += "(";
        temp += std::to_string(it.getRid());
        temp += "|";
        temp += std::to_string(it.getSid());
        temp += "|";
        temp += DTType::DTTypeString(it.getMutation().isInsert());
        temp += "|";
        temp += DB::toString(it.getMutation().count());
        temp += "|";
        temp += DB::toString(it.getMutation().value);
        temp += "),";
        result += temp;
    }
    return result;
}

void checkCopy(FakeDeltaTree & tree)
{
    FakeDeltaTree copy(tree);
    ASSERT_EQ(treeToString(copy), treeToString(tree));
    tree.swap(copy);
}

TEST_F(DeltaTreeTest, Insert)
{
    auto & tree = *fake_tree;
    // insert 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }
    checkCopy(tree);

    // delete 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    checkCopy(tree);

    // insert 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }
    checkCopy(tree);

    // delete
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    checkCopy(tree);

    // delete
    for (int i = 5; i <= 8; ++i)
    {
        tree.addDelete(i);
        tree.checkAll();
    }
    checkCopy(tree);

    // delete
    tree.addDelete(30);
    tree.addDelete(30);
    tree.addDelete(30);
    tree.addDelete(63);
    tree.addDelete(64);
    tree.addDelete(64);
    tree.addDelete(30);
    tree.addDelete(30);
    tree.addDelete(0);
    tree.addDelete(0);
    tree.addDelete(0);
    tree.addDelete(0);
    tree.addDelete(1);
    tree.addDelete(64);

    checkCopy(tree);

    // insert
    for (int i = 0; i < 5; ++i)
    {
        tree.addInsert(i, i);
    }
    checkCopy(tree);
}

TEST_F(DeltaTreeTest, DeleteAfterInsert)
{
    auto & tree = *fake_tree;
    int batch_num = 100;

    std::string expected_result;
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
        expected_result += "(" + std::to_string(i) + "|0|INS|1|" + std::to_string(i) + "),";
        ASSERT_EQ(expected_result, treeToString(tree));
    }
    checkCopy(tree);

    expected_result = "";
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
        expected_result = "";
        for (int j = 0; j < batch_num - i - 1; j++)
        {
            expected_result += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(j + i + 1) + "),";
        }
        ASSERT_EQ(expected_result, treeToString(tree));
    }

    expected_result = "";
    ASSERT_EQ(expected_result, treeToString(tree));
    checkCopy(tree);

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expected_result = "";
        for (int j = 0; j <= i; j++)
        {
            expected_result += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expected_result, treeToString(tree));
    }
    checkCopy(tree);

    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expected_result = "";
        for (int j = 0; j < i; j++)
        {
            expected_result += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(batch_num - j - 1) + "),";
        }
        ASSERT_EQ(expected_result, treeToString(tree));
    }
    checkCopy(tree);
}

TEST_F(DeltaTreeTest, Delete1)
{
    auto & tree = *fake_tree;
    int batch_num = 100;

    // delete stable from begin to end with merge
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::string expected_result = "(0|0|DEL|" + DB::toString(batch_num) + "|0),";
    ASSERT_EQ(expected_result, treeToString(tree));
    checkCopy(tree);
}

TEST_F(DeltaTreeTest, Delete2)
{
    auto & tree = *fake_tree;
    int batch_num = 100;

    std::string expected_result;
    // delete stable from end to begin
    // this kind of delete behavior may be improved to trigger merge
    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expected_result = "";
        for (int j = i; j < batch_num; j++)
        {
            expected_result += "(" + std::to_string(i) + "|" + std::to_string(j) + "|DEL|1|0),";
        }

        ASSERT_EQ(expected_result, treeToString(tree));
    }
    checkCopy(tree);
}

TEST_F(DeltaTreeTest, InsertSkipDelete)
{
    auto & tree = *fake_tree;
    int batch_num = 100;
    tree.addDelete(0);
    std::string expected_result = "(0|0|DEL|1|0),";
    ASSERT_EQ(expected_result, treeToString(tree));

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expected_result = "(0|0|DEL|1|0),";
        for (int j = 0; j <= i; j++)
        {
            expected_result += "(" + std::to_string(j) + "|1|INS|1|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expected_result, treeToString(tree));
    }
    checkCopy(tree);
}


TEST_F(DeltaTreeTest, CreateNodeFailInCopyCtor)
try
{
    auto & tree = *fake_tree;
    // Create a tree for copy
    for (int i = 0; i < 1000; ++i)
    {
        tree.addInsert(i, i);
    }
    auto mem_usage_old = current_memory_tracker->get();
    ASSERT_EQ(mem_usage_old, tree.getBytes());

    DB::FailPointHelper::enableFailPoint(DB::FailPoints::delta_tree_create_node_fail);
    try
    {
        // Must throw DB::Exception
        FakeDeltaTree copy(tree);
    }
    catch (const Exception & e)
    {
        // Catch, check and return directly
        ASSERT_EQ(e.code(), ErrorCodes::FAIL_POINT_ERROR);
        ASSERT_EQ(e.message(), String("Failpoint delta_tree_create_node_fail is triggered"));
        auto mem_usage_current = current_memory_tracker->get();
        ASSERT_EQ(mem_usage_current, mem_usage_old);
        return;
    }
    FAIL() << "Should not come here";
}
CATCH

} // namespace DB::DM::tests
