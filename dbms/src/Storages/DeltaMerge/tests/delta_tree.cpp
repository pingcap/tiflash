#include <cmath>
#include <iostream>

#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

#include <Storages/DeltaMerge/Tuple.h>
#include <Storages/DeltaMerge/ValueSpace.h>

#define psizeof(M) std::cout << "sizeof(" #M "): " << sizeof(M) << std::endl
#define print(M) std::cout << "" #M ": " << M << std::endl

using namespace DB;


class FakeValueSpace;
using MyDeltaTree = DeltaTree<FakeValueSpace, 2, 10>;

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

void print_sizes()
{
    psizeof(MyDeltaTree::Leaf);
    psizeof(MyDeltaTree::Intern);

    psizeof(DefaultDeltaTree::Leaf);
    psizeof(DefaultDeltaTree::Intern);
}

void printTree(MyDeltaTree & tree)
{
    print(tree.getHeight());
    for (auto it = tree.begin(), end = tree.end(); it != end; ++it)
    {
        std::cout << "(" << it.getRid() << "|" << it.getSid() << "|" << DTTypeString(it.getMutation().type) << "|"
                  << DB::toString(it.getMutation().value) << "),";
    }
    std::cout << std::endl;
}

void insertTest(MyDeltaTree & tree)
{
    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }

    std::cout << "a====\n";
    printTree(tree);

    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }

    std::cout << "b====\n";
    printTree(tree);

    for (int i = 0; i < 100; ++i)
    {
        tree.addInsert(i, i);
        tree.checkAll();
    }

    std::cout << "1111====\n";
    printTree(tree);

    for (int i = 0; i < 100; ++i)
    {
        tree.addDelete(0);
        tree.checkAll();
    }

    std::cout << "c====\n";
    printTree(tree);

    tree.addModify(8, 0, 8);
    tree.addModify(8, 1, 8);
    tree.addModify(8, 0, 8);

    std::cout << "d====\n";
    printTree(tree);

    tree.addModify(97, 0, 97);
    tree.addModify(97, 0, 97);
    tree.addModify(97, 8, 97);
    tree.addModify(97, 1, 97);

    std::cout << "e====\n";
    printTree(tree);

    for (int i = 5; i <= 8; ++i)
    {
        tree.addDelete(i);
        tree.checkAll();
    }

    std::cout << "f====\n";
    printTree(tree);

    for (int i = 5; i <= 8; ++i)
    {
        tree.addModify(i, 1, i);
        tree.checkAll();
    }

    std::cout << "g====\n";
    printTree(tree);

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

    for (int i = 0; i < 5; ++i)
    {
        tree.addInsert(i, i);
    }

    std::cout << "h====\n";
    printTree(tree);
}

int main(int, char **)
{
    print_sizes();
    FakeValueSpacePtr insert_vs = std::make_shared<FakeValueSpace>();
    FakeValueSpacePtr modify_vs = std::make_shared<FakeValueSpace>();
    MyDeltaTree       delta_tree(insert_vs, modify_vs);
    try
    {
        insertTest(delta_tree);
    }
    catch (const DB::Exception & ex)
    {
        std::cout << "Caught exception " << ex.displayText() << "\n";
    }
    catch (const std::exception & ex)
    {
        std::cout << "Caught exception " << ex.what() << "\n";
    }
    catch (...)
    {
        std::cout << "Caught unhandled exception\n";
    }

    return 0;
}
