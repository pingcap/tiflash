#include <gtest/gtest.h>

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DummyValueSpace.h>
#include <Storages/DeltaMerge/Tuple.h>

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

void printTree(FakeDeltaTree & tree)
{
    print(tree.getHeight());
    for (auto it = tree.begin(), end = tree.end(); it != end; ++it)
    {
        std::cout << "(" << it.getRid() << "|" << it.getSid() << "|" << DTTypeString(it.getMutation().type) << "|"
                  << DB::toString(it.getMutation().value) << "),";
    }
    std::cout << std::endl;
}

std::string treeToString(FakeDeltaTree & tree)
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
        temp += DTTypeString(it.getMutation().type);
        temp += "|";
        temp += DB::toString(it.getMutation().value);
        temp += "),";
        result += temp;
    }
    return result;
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

    // delete 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::cout << "b====\n";
    printTree(tree);

    // insert 100 items
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }
    std::cout << "1111====\n";
    printTree(tree);

    // delete
    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }
    std::cout << "c====\n";
    printTree(tree);

    // modify
    tree.addModify(8, 0, 8);
    tree.addModify(8, 1, 8);
    tree.addModify(8, 0, 8);
    std::cout << "d====\n";
    printTree(tree);

    // modify
    tree.addModify(97, 0, 97);
    tree.addModify(97, 0, 97);
    tree.addModify(97, 8, 97);
    tree.addModify(97, 1, 97);
    std::cout << "e====\n";
    printTree(tree);

    // delete
    for (int i = 5; i <= 8; ++i)
    {
        tree.addDelete(i);
        tree.checkAll();
    }
    std::cout << "f====\n";
    printTree(tree);

    // modify
    for (int i = 5; i <= 8; ++i)
    {
        tree.addModify(i, 1, i);
        tree.checkAll();
    }
    std::cout << "g====\n";
    printTree(tree);

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

    // insert
    for (int i = 0; i < 5; ++i)
    {
        tree.addInsert(i, i);
    }
    std::cout << "h====\n";
    printTree(tree);
}

TEST_F(DeltaTree_test, DeleteAfterInsert)
{
    int batch_num = 100;

    std::string expectedResult;
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
        expectedResult += "(" + std::to_string(i) + "|0|INS|" + std::to_string(i) + "),";
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    std::cout << "after many insert 1\n";

    expectedResult = "";
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j < batch_num - i - 1; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|" + std::to_string(j + i + 1) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }

    expectedResult = "";
    ASSERT_EQ(expectedResult, treeToString(tree));
    std::cout << "after many delete 1\n";

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j <= i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    std::cout << "after many insert 2\n";

    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j < i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|0|INS|" + std::to_string(batch_num - j - 1) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
    std::cout << "after many delete 2\n";
}

TEST_F(DeltaTree_test, Delete1)
{
    int batch_num = 100;

    std::string expectedResult;
    // delete stable from begin to end with merge
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
        expectedResult = "(0|0|DEL|" + std::to_string(i + 1) + "),";
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
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
            expectedResult += "(" + std::to_string(i) + "|" + std::to_string(j) + "|DEL|1),";
        }

        ASSERT_EQ(expectedResult, treeToString(tree));
    }
}

TEST_F(DeltaTree_test, InsertSkipDelete)
{
    int batch_num = 100;
    tree.addDelete(0);
    std::string expectedResult = "(0|0|DEL|1),";
    ASSERT_EQ(expectedResult, treeToString(tree));

    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(0, i);
        tree.checkAll();
        expectedResult = "(0|0|DEL|1),";
        for (int j = 0; j <= i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|1|INS|" + std::to_string(i - j) + "),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
}

TEST_F(DeltaTree_test, DeleteAfterUpdate)
{
    int batch_num = 100;

    std::string expectedResult;
    std::string expectedResult2;
    // multiple update to the same row and same column
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addModify(i, 0, 2 * i);
        tree.checkAll();
        expectedResult = expectedResult2 + "(" + std::to_string(i) + "|" + std::to_string(i) + "|0|" + std::to_string(2 * i) + "),";
        ASSERT_EQ(expectedResult, treeToString(tree));

        tree.addModify(i, 0, 2 * i + 1);
        tree.checkAll();
        expectedResult2 = expectedResult2 + "(" + std::to_string(i) + "|" + std::to_string(i) + "|0|" + std::to_string(2 * i + 1) + "),";
        ASSERT_EQ(expectedResult2, treeToString(tree));
    }

    for (int i = batch_num - 1; i >= 0; --i)
    {
        tree.addDelete(i);
        tree.checkAll();
        expectedResult = "";
        for (int j = 0; j < i; j++)
        {
            expectedResult += "(" + std::to_string(j) + "|" + std::to_string(j) + "|0|" + std::to_string(2 * j + 1) + "),";
        }
        for (int j = i; j < batch_num; j++)
        {
            expectedResult += "(" + std::to_string(i) + "|" + std::to_string(j) + "|DEL|1),";
        }
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
}

TEST_F(DeltaTree_test, UpdateSkipDelete)
{
    tree.addDelete(0);
    std::string expectedResult = "(0|0|DEL|1),";
    ASSERT_EQ(expectedResult, treeToString(tree));

    tree.addModify(0, 0, 0);
    tree.checkAll();
    expectedResult = "(0|0|DEL|1),(0|1|0|0),";

    std::cout << expectedResult << std::endl;
    std::cout << treeToString(tree) << std::endl;
    ASSERT_EQ(expectedResult, treeToString(tree));
}

TEST_F(DeltaTree_test, InplaceUpdate)
{
    int         batch_num = 100;
    std::string expectedResult;
    for (int i = 0; i < batch_num; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
        expectedResult = expectedResult + "(" + std::to_string(i) + "|0|INS|" + std::to_string(i) + "),";
        ASSERT_EQ(expectedResult, treeToString(tree));
        tree.addModify(i, 0, i);
        tree.checkAll();
        ASSERT_EQ(expectedResult, treeToString(tree));
    }
}

} // namespace tests
} // namespace DM
} // namespace DB
