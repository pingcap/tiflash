#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>

namespace DB::DM::tests
{
class Node
{
public:
    explicit Node(uint64_t id_)
        : id(id_)
        , table_id(1)
        , v(true)
    {}

    bool valid() const
    {
        return v;
    }
    uint64_t poolId() const
    {
        return id;
    }
    int64_t tableId() const
    {
        return table_id;
    }
    void setInvalid()
    {
        v = false;
    }

private:
    uint64_t id;
    int64_t table_id;
    bool v;
};

TEST(CircularScanListTest, Normal)
{
    CircularScanList<Node> lst;

    {
        ASSERT_EQ(lst.next(), nullptr);
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 0);
        ASSERT_EQ(lst.get(1), nullptr);
    }

    for (uint64_t i = 0; i < 10; i++)
    {
        lst.add(std::make_shared<Node>(i));
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 10);
        ASSERT_EQ(invalid, 0);
    }

    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->poolId(), i % 10);
    }

    lst.get(1)->setInvalid();
    lst.get(3)->setInvalid();
    lst.get(5)->setInvalid();

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 7);
        ASSERT_EQ(invalid, 3);
    }

    const std::vector<uint64_t> valid_ids = {0, 2, 4, 6, 7, 8, 9};
    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->poolId(), valid_ids[i % valid_ids.size()]);
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 7);
        ASSERT_EQ(invalid, 0);
    }

    for (uint64_t id : valid_ids)
    {
        lst.get(id)->setInvalid();
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 7);
    }

    ASSERT_EQ(lst.next(), nullptr);
}

TEST(CircularScanListTest, valid)
{
    CircularScanList<Node> l;
    l.add(std::make_shared<Node>(1));

    ASSERT_EQ(l.next()->poolId(), 1);
    ASSERT_EQ(l.next()->poolId(), 1);

    l.next()->setInvalid();

    ASSERT_EQ(l.next(), nullptr);
    ASSERT_EQ(l.next(), nullptr);
    l.add(std::make_shared<Node>(2));

    ASSERT_EQ(l.next()->poolId(), 2);
    ASSERT_EQ(l.next()->poolId(), 2);
}
} // namespace DB::DM::tests