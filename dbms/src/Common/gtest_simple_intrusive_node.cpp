#include <Common/SimpleIntrusiveNode.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
namespace
{

template <typename T, typename F>
auto getElems(T * head, F && getter)
{
    std::vector<decltype(getter(head))> elems;
    elems.push_back(getter(head));
    for (auto * next = head->next; next != head; next = next->next)
        elems.push_back(getter(next));
    return elems;
}

struct IntNode : public SimpleIntrusiveNode<IntNode>
{
    int v;
    
    explicit IntNode(int x) : SimpleIntrusiveNode<IntNode>(), v(x) {}
};

int getValue(const IntNode * node)
{
    return node->v;
}

std::vector<int> vec(std::initializer_list<int> l)
{
    return l;
}

TEST(SimpleIntrusiveNode, testSingleNode)
{
    IntNode n(0);
    ASSERT_EQ(n.isSingle(), true);
    ASSERT_EQ(getElems(&n, getValue), vec({0}));
}

TEST(SimpleIntrusiveNode, testOperateOnSelf)
{
    IntNode n(0);

    n.appendTo(&n);
    ASSERT_EQ(n.isSingle(), true);
    ASSERT_EQ(getElems(&n, getValue), vec({0}));

    n.prependTo(&n);
    ASSERT_EQ(n.isSingle(), true);
    ASSERT_EQ(getElems(&n, getValue), vec({0}));

    n.detach();
    ASSERT_EQ(n.isSingle(), true);
    ASSERT_EQ(getElems(&n, getValue), vec({0}));
}

TEST(SimpleIntrusiveNode, testAppendTo)
{
    IntNode n0(0), n1(1), n2(2);
    n1.appendTo(&n0);
    n2.appendTo(&n1);

    ASSERT_EQ(getElems(&n0, getValue), vec({0, 1, 2}));
}
} // namespace
} // namespace DB::tests

