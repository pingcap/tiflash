#include <Common/MPMCQueue.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace DB
{
namespace tests
{

class TestMPMCQueue: public ::testing::Test
{
protected:
    std::random_device rd;

    template <typename T>
    void testInitialize(Int64 capacity)
    {
        MPMCQueue<T> queue(capacity);
        EXPECT_EQ(queue.getStatus(), MPMCQueueStatus::NORMAL);
        testCannotTryPop(queue); /// will block if call `pop`

        queue.cancel();
        EXPECT_EQ(queue.getStatus(), MPMCQueueStatus::CANCELLED);
        testCannotPop(queue); /// won't block

        MPMCQueue<T> queue2(capacity);
        queue2.finish();
        EXPECT_EQ(queue2.getStatus(), MPMCQueueStatus::FINISHED);
        testCannotPop(queue2); /// won't block
    }

    template <typename T>
    void testCannotTryPush(MPMCQueue<T> & queue)
    {
        auto res = queue.tryPush(ValueHelper<T>::make(-1), std::chrono::microseconds(1));
        EXPECT_TRUE(!res);
    }

    template <typename T>
    void testCannotPop(MPMCQueue<T> & queue)
    {
        auto res = queue.pop();
        EXPECT_TRUE(!res.has_value());
    }

    template <typename T>
    void testCannotTryPop(MPMCQueue<T> & queue)
    {
        auto res = queue.tryPop(std::chrono::microseconds(1));
        EXPECT_TRUE(!res.has_value());
    }

    template <typename T>
    int testPushN(MPMCQueue<T> & queue, Int64 n, int start, bool full_after_push)
    {
        for (int i = 0; i < n; ++i)
            queue.push(ValueHelper<T>::make(start++));
        if (full_after_push)
            testCannotTryPush(queue);
        return start;
    }

    template <typename T>
    int testPopN(MPMCQueue<T> & queue, int n, int start, bool empty_after_pop)
    {
        for (int i = 0; i < n; ++i)
        {
            auto res = queue.pop();
            EXPECT_TRUE(res.has_value());
            EXPECT_TRUE(ValueHelper<T>::equal(res.value(), start++));
        }
        if (empty_after_pop)
            testCannotTryPop(queue);
        return start;
    }

    template <typename T>
    void testSequentialPushPop(Int64 capacity)
    {
        MPMCQueue<T> queue(capacity);
        int read_start = 0;
        int write_start = testPushN<T>(queue, capacity, 0, true);
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(1, capacity - 1);
        for (int i = 0; i < 100; ++i)
        {
            int pop_cnt = dist(mt);
            read_start = testPopN(queue, pop_cnt, read_start, false);
            write_start = testPushN<T>(queue, pop_cnt, write_start, true);
        }
        read_start = testPopN(queue, capacity, read_start, true);
        EXPECT_EQ(read_start, write_start);
    }

    template <typename T>
    struct ValueHelper;
};

template <>
struct TestMPMCQueue::ValueHelper<int>
{
    static int make(int v) { return v; }
    static bool equal(int a, int b) { return a == b; }
};

template <>
struct TestMPMCQueue::ValueHelper<std::unique_ptr<int>>
{
    static std::unique_ptr<int> make(int v) { return std::make_unique<int>(v); }
    static bool equal(const std::unique_ptr<int> & a, int b) { return a && *a == b; }
};

template <>
struct TestMPMCQueue::ValueHelper<std::shared_ptr<int>>
{
    static std::shared_ptr<int> make(int v) { return std::make_shared<int>(v); }
    static bool equal(const std::shared_ptr<int> & a, int b) { return a && *a == b; }
};

TEST_F(TestMPMCQueue, Int_Initialize)
try
{
    testInitialize<int>(10);
}
CATCH

TEST_F(TestMPMCQueue, UniquePtr_Int_Initialize)
try
{
    testInitialize<std::unique_ptr<int>>(10);
}
CATCH

TEST_F(TestMPMCQueue, SharedPtr_Int_Initialize)
try
{
    testInitialize<std::shared_ptr<int>>(10);
}
CATCH

TEST_F(TestMPMCQueue, Int_SequentialPushPop)
try
{
    testSequentialPushPop<int>(100);
}
CATCH

TEST_F(TestMPMCQueue, UniquePtr_Int_SequentialPushPop)
try
{
    testSequentialPushPop<std::unique_ptr<int>>(100);
}
CATCH

TEST_F(TestMPMCQueue, SharedPtr_Int_SequentialPushPop)
try
{
    testSequentialPushPop<std::shared_ptr<int>>(100);
}
CATCH

} // namespace tests
} // namespace DB

