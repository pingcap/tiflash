#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>

namespace DB::DM::tests
{
class Node
{
public:
    Node(uint64_t id_, bool e_)
        : id(id_)
        , e(e_)
    {}

    bool expired() const
    {
        return e;
    }
    uint64_t getId() const
    {
        return id;
    }
    void setExpire()
    {
        e = true;
    }

private:
    uint64_t id;
    bool e;
};

TEST(CircularScanList_test, Normal)
{
    CircularScanList<Node> lst;

    {
        ASSERT_EQ(lst.next(), nullptr);
        auto [unexpired, expired] = lst.count();
        ASSERT_EQ(unexpired, 0);
        ASSERT_EQ(expired, 0);
        ASSERT_EQ(lst.get(1), nullptr);
    }

    for (uint64_t i = 0; i < 10; i++)
    {
        lst.add(std::make_shared<Node>(i, false));
    }

    {
        auto [unexpired, expired] = lst.count();
        ASSERT_EQ(unexpired, 10);
        ASSERT_EQ(expired, 0);
    }

    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->getId(), i % 10);
    }

    lst.get(1)->setExpire();
    lst.get(3)->setExpire();
    lst.get(5)->setExpire();

    {
        auto [unexpired, expired] = lst.count();
        ASSERT_EQ(unexpired, 7);
        ASSERT_EQ(expired, 3);
    }

    const std::vector<uint64_t> unexpired_ids = {0, 2, 4, 6, 7, 8, 9};
    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->getId(), unexpired_ids[i % unexpired_ids.size()]);
    }

    {
        auto [unexpired, expired] = lst.count();
        ASSERT_EQ(unexpired, 7);
        ASSERT_EQ(expired, 0);
    }

    for (uint64_t id : unexpired_ids)
    {
        lst.get(id)->setExpire();
    }

    {
        auto [unexpired, expired] = lst.count();
        ASSERT_EQ(unexpired, 0);
        ASSERT_EQ(expired, 7);
    }

    ASSERT_EQ(lst.next(), nullptr);
}

TEST(CircularScanList_test, Expired)
{
    CircularScanList<Node> l;
    l.add(std::make_shared<Node>(1, false));

    ASSERT_EQ(l.next()->getId(), 1);
    ASSERT_EQ(l.next()->getId(), 1);

    l.next()->setExpire();

    ASSERT_EQ(l.next(), nullptr);
    ASSERT_EQ(l.next(), nullptr);
    l.add(std::make_shared<Node>(2, false));

    ASSERT_EQ(l.next()->getId(), 2);
    ASSERT_EQ(l.next()->getId(), 2);
}
} // namespace DB::DM::tests