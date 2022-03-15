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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/Tuple.h>
#include <gtest/gtest.h>

namespace DB
{
namespace DM
{
namespace tests
{
#define print(M) std::cout << "" #M ": " << M << std::endl

class FakeValueSpace;
using FakeDeltaTree = DeltaTree<FakeValueSpace, 2, 10>;

class FakeValueSpace
{
    using ValueSpace = FakeValueSpace;

public:
    void removeFromInsert(UInt64 id)
    {
        //
        std::cout << "remove : " << id << std::endl;
    }

    void removeFromModify(UInt64 id, size_t column_id)
    {
        //
        std::cout << "remove : " << id << ", column:" << column_id << std::endl;
    }

    UInt64 withModify(UInt64 old_tuple_id, const ValueSpace & /*modify_value_space*/, const RefTuple & tuple)
    {
        std::cout << "withModify, old_tuple_id:" << old_tuple_id << ", modifies:[";
        for (const auto & m : tuple.values)
        {
            std::cout << m.column << ",";
        }
        std::cout << "]" << std::endl;
        return old_tuple_id;
    }
};

using FakeValueSpacePtr = std::shared_ptr<FakeValueSpace>;

class DeltaTree_test : public ::testing::Test
{
protected:
    FakeDeltaTree tree;
};

void printTree(const FakeDeltaTree & tree)
{
    print(tree.getHeight());
    for (auto it = tree.begin(), end = tree.end(); it != end; ++it)
    {
        std::cout << "(" << it.getRid() << "|" << it.getSid() << "|" << DTType::DTTypeString(it.getMutation().isInsert()) << "|"
                  << DB::toString(it.getMutation().count()) << "|" << DB::toString(it.getMutation().value) << "),";
    }
    std::cout << std::endl;
}

std::string treeToString(const FakeDeltaTree & tree)
{
    std::string result = "";
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

TEST_F(DeltaTree_test, PrintSize)
{
    std::cout << "=====================\n";
    std::cout << "DT leaf node size: " << sizeof(DTLeaf<55, 20, 3>) << "\n";
    std::cout << "DT inter node size: " << sizeof(DTIntern<55, 20, 3>) << "\n";
    std::cout << "=====================\n";
}

TEST_F(DeltaTree_test, Insert)
{
    // insert 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }
    std::cout << "a====\n";
    printTree(tree);
    checkCopy(tree);

    // delete 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::cout << "b====\n";
    printTree(tree);
    checkCopy(tree);

    // insert 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }
    std::cout << "1111====\n";
    printTree(tree);
    checkCopy(tree);

    // delete
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::cout << "c====\n";
    printTree(tree);
    checkCopy(tree);

    // delete
    for (int i = 5; i <= 8; ++i)
    {
        tree.addDelete(i);
        tree.checkAll();
    }
    std::cout << "f====\n";
    printTree(tree);
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

    std::cout << "g111====\n";
    printTree(tree);
    checkCopy(tree);

    // insert
    for (int i = 0; i < 5; ++i)
    {
        tree.addInsert(i, i);
    }
    std::cout << "h====\n";
    printTree(tree);
    checkCopy(tree);
}

TEST_F(DeltaTree_test, DeleteAfterInsert)
{
    int batch_num = 100;

    std::string expectedResult;
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
        expectedResult += "(" + std::to_string(i) + "|0|INS|1|" + std::to_string(i) + "),";
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    checkCopy(tree);
    std::cout << "after many insert 1\n";

    expectedResult = "";
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j < batch_num - i - 1; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(j + i + 1) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }

    expectedResult = "";
    ASSERT_EQ(expectedResult, treeToString(tree));
    checkCopy(tree);
    std::cout << "after many delete 1\n";

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j <= i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    checkCopy(tree);
    std::cout << "after many insert 2\n";

    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j < i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|1|" + std::to_string(batch_num - j - 1) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    checkCopy(tree);
    std::cout << "after many delete 2\n";
}

TEST_F(DeltaTree_test, Delete1)
{
    int batch_num = 100;

    // delete stable from begin to end with merge
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::string expectedResult = "(0|0|DEL|" + DB::toString(batch_num) + "|0),";
    ASSERT_EQ(expectedResult, treeToString(tree));
    checkCopy(tree);
}

TEST_F(DeltaTree_test, Delete2)
{
    int batch_num = 100;

    std::string expectedResult;
    // delete stable from end to begin
    // this kind of delete behavior may be improved to trigger merge
    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expectedResult = "";
        for (int j = i; j < batch_num; j++)
        {
            expectedResult += "(" + std::to_string(i) + "|" + std::to_string(j) + "|DEL|1|0),";
        }

        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    checkCopy(tree);
}

TEST_F(DeltaTree_test, InsertSkipDelete)
{
    int batch_num = 100;
    tree.addDelete(0);
    std::string expectedResult = "(0|0|DEL|1|0),";
    ASSERT_EQ(expectedResult, treeToString(tree));

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expectedResult = "(0|0|DEL|1|0),";
        for (int j = 0; j <= i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|1|INS|1|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    checkCopy(tree);
}

} // namespace tests
} // namespace DM
} // namespace DB
