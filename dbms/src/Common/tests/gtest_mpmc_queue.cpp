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
class TestMPMCQueue : public ::testing::Test
{
protected:
    std::random_device rd;

    template <typename T>
    struct ValueHelper;

    template <typename T>
    void testInitialize(Int64 capacity)
    {
        MPMCQueue<T> queue(capacity);
        ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::NORMAL);
        ASSERT_EQ(queue.size(), 0);
        testCannotTryPop(queue); /// will block if call `pop`

        queue.cancel();
        ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::CANCELLED);
        testCannotPop(queue); /// won't block

        MPMCQueue<T> queue2(capacity);
        queue2.finish();
        ASSERT_EQ(queue2.getStatus(), MPMCQueueStatus::FINISHED);
        testCannotPop(queue2); /// won't block
    }

    template <typename T>
    void testDuplicateFinishCancel()
    {
        {
            MPMCQueue<T> queue(1);
            queue.cancel();
            queue.finish();
            ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::CANCELLED);
        }
        {
            MPMCQueue<T> queue(1);
            queue.finish();
            queue.finish();
            ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::FINISHED);
        }
        {
            MPMCQueue<T> queue(1);
            queue.finish();
            queue.cancel();
            ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::FINISHED);
        }
        {
            MPMCQueue<T> queue(1);
            queue.cancel();
            queue.cancel();
            ASSERT_EQ(queue.getStatus(), MPMCQueueStatus::CANCELLED);
        }
    }

    template <typename T>
    void testCannotPush(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        auto res = queue.push(ValueHelper<T>::make(-1));
        auto new_size = queue.size();
        if (res)
            throw TiFlashTestException("Should push fail");
        if (old_size != new_size)
            throw TiFlashTestException(fmt::format("Size changed from {} to {} without push", old_size, new_size));
    }

    template <typename T>
    void testCannotTryPush(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        auto res = queue.tryPush(ValueHelper<T>::make(-1), std::chrono::microseconds(1));
        auto new_size = queue.size();
        if (res)
            throw TiFlashTestException("Should push fail");
        if (old_size != new_size)
            throw TiFlashTestException(fmt::format("Size changed from {} to {} without push", old_size, new_size));
    }

    template <typename T>
    void testCannotPop(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        auto res = queue.pop();
        auto new_size = queue.size();
        if (res.has_value())
            throw TiFlashTestException("Should pop fail");
        if (old_size != new_size)
            throw TiFlashTestException(fmt::format("Size changed from {} to {} without pop", old_size, new_size));
    }

    template <typename T>
    void testCannotTryPop(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        auto res = queue.tryPop(std::chrono::microseconds(1));
        auto new_size = queue.size();
        if (res.has_value())
            throw TiFlashTestException("Should pop fail");
        if (old_size != new_size)
            throw TiFlashTestException(fmt::format("Size changed from {} to {} without pop", old_size, new_size));
    }

    template <typename T>
    int testPushN(MPMCQueue<T> & queue, Int64 n, int start, bool full_after_push)
    {
        auto old_size = queue.size();
        for (int i = 0; i < n; ++i)
            queue.push(ValueHelper<T>::make(start++));
        auto new_size = queue.size();
        if (old_size + n != new_size)
            throw TiFlashTestException(fmt::format("Size mismatch: expect {} but found {}", old_size + n, new_size));
        if (full_after_push)
            testCannotTryPush(queue);
        return start;
    }

    template <typename T>
    int testPopN(MPMCQueue<T> & queue, int n, int start, bool empty_after_pop)
    {
        auto old_size = queue.size();
        for (int i = 0; i < n; ++i)
        {
            auto res = queue.pop();
            if (!res.has_value())
                throw TiFlashTestException("Should pop a value");
            int expect = start++;
            int actual = ValueHelper<T>::extract(res.value());
            if (actual != expect)
                throw TiFlashTestException(fmt::format("Value mismatch! actual: {}, expect: {}", actual, expect));
        }
        auto new_size = queue.size();
        if (old_size - n != new_size)
            throw TiFlashTestException(fmt::format("Size mismatch: expect {} but found {}", old_size - n, new_size));
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
        ASSERT_EQ(read_start, write_start);
    }

    template <typename T>
    void testFinishEmpty(Int64 capacity, int reader_cnt)
    {
        MPMCQueue<T> queue(capacity);
        std::vector<std::thread> readers;
        std::vector<UInt8> reader_results(reader_cnt, -1);

        auto read_func = [&](int i) {
            auto res = queue.pop();
            reader_results[i] = res.has_value();
        };

        for (int i = 0; i < reader_cnt; ++i)
            readers.emplace_back(read_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.finish();

        for (int i = 0; i < reader_cnt; ++i)
        {
            readers[i].join();
            ASSERT_EQ(reader_results[i], 0);
        }

        ASSERT_EQ(queue.size(), 0);

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.pop();
            ASSERT_TRUE(!res.has_value());
        }

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.push(ValueHelper<T>::make(i));
            ASSERT_TRUE(!res);
        }
    }

    template <typename T>
    void testCancelEmpty(Int64 capacity, int reader_cnt)
    {
        MPMCQueue<T> queue(capacity);
        std::vector<std::thread> readers;
        std::vector<UInt8> reader_results(reader_cnt, -1);

        auto read_func = [&](int i) {
            auto res = queue.pop();
            reader_results[i] = res.has_value();
        };

        for (int i = 0; i < reader_cnt; ++i)
            readers.emplace_back(read_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.cancel();

        for (int i = 0; i < reader_cnt; ++i)
        {
            readers[i].join();
            ASSERT_EQ(reader_results[i], 0);
        }

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.pop();
            ASSERT_TRUE(!res.has_value());
        }

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.push(ValueHelper<T>::make(i));
            ASSERT_TRUE(!res);
        }
    }

    template <typename T>
    void testCancelConcurrentPop(int reader_cnt)
    {
        MPMCQueue<T> queue(1);
        std::vector<std::thread> threads;
        std::vector<int> reader_results(reader_cnt, 0);

        auto read_func = [&](int i) {
            auto res = queue.pop();
            reader_results[i] += res.has_value();
        };

        for (int i = 0; i < reader_cnt; ++i)
            threads.emplace_back(read_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.cancel();

        for (int i = 0; i < reader_cnt; ++i)
        {
            threads[i].join();
            ASSERT_EQ(reader_results[i], 0);
        }

        testCannotPop(queue);
        testCannotPush(queue);
    }

    template <typename T>
    void testCancelConcurrentPush(int writer_cnt)
    {
        MPMCQueue<T> queue(1);
        queue.push(ValueHelper<T>::make(0));

        std::vector<std::thread> threads;
        std::vector<int> writer_results(writer_cnt, 0);

        auto write_func = [&](int i) {
            auto res = queue.push(ValueHelper<T>::make(i));
            writer_results[i] += res;
        };

        for (int i = 0; i < writer_cnt; ++i)
            threads.emplace_back(write_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.cancel();

        for (int i = 0; i < writer_cnt; ++i)
        {
            threads[i].join();
            ASSERT_EQ(writer_results[i], 0);
        }

        testCannotPop(queue);
        testCannotPush(queue);
    }

    template <typename T>
    void testPopAfterFinish(Int64 capacity)
    {
        MPMCQueue<T> queue(capacity);

        testPushN<T>(queue, capacity, 0, true);

        queue.finish();

        testPopN<T>(queue, capacity, 0, true);

        ASSERT_EQ(queue.size(), 0);
    }

    template <typename T>
    void testConcurrentPush(Int64 capacity, int writer_cnt)
    {
        MPMCQueue<T> queue(capacity);
        std::vector<std::thread> threads;
        std::vector<int> reader_results;
        std::vector<std::vector<int>> writer_results(writer_cnt);

        auto read_func = [&] {
            while (true)
            {
                auto res = queue.pop();
                if (res.has_value())
                    reader_results.push_back(ValueHelper<T>::extract(res.value()));
                else
                    break;
            }
        };

        auto write_func = [&](int i) {
            for (int x = i;; x += writer_cnt)
            {
                auto res = queue.push(ValueHelper<T>::make(x));
                if (res)
                    writer_results[i].push_back(x);
                else
                    break;
            }
        };

        threads.emplace_back(read_func);
        for (int i = 0; i < writer_cnt; ++i)
            threads.emplace_back(write_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue.finish();

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(queue.size(), 0);

        std::vector<int> all_writer_results = collect(writer_results);

        expectMonotonic(reader_results, writer_cnt);
        expectEqualAfterSort(reader_results, all_writer_results);
    }

    template <typename T>
    void testConcurrentPushPop(Int64 capacity, int reader_writer_cnt)
    {
        MPMCQueue<T> queue(capacity);
        std::vector<std::thread> threads;
        std::vector<std::vector<int>> reader_results(reader_writer_cnt);
        std::vector<std::vector<int>> writer_results(reader_writer_cnt);

        auto read_func = [&](int i) {
            while (true)
            {
                auto res = queue.pop();
                if (res.has_value())
                    reader_results[i].push_back(ValueHelper<T>::extract(res.value()));
                else
                    break;
            }
        };

        auto write_func = [&](int i) {
            for (int x = i;; x += reader_writer_cnt)
            {
                auto res = queue.push(ValueHelper<T>::make(x));
                if (res)
                    writer_results[i].push_back(x);
                else
                    break;
            }
        };

        for (int i = 0; i < reader_writer_cnt; ++i)
            threads.emplace_back(read_func, i);
        for (int i = 0; i < reader_writer_cnt; ++i)
            threads.emplace_back(write_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue.finish();

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(queue.size(), 0);

        std::vector<int> all_reader_results = collect(reader_results);
        std::vector<int> all_writer_results = collect(writer_results);

        expectEqualAfterSort(all_reader_results, all_writer_results);
    }

    static std::vector<int> collect(const std::vector<std::vector<int>> & vs)
    {
        std::vector<int> res;
        for (const auto & v : vs)
            for (int x : v)
                res.push_back(x);
        return res;
    }

    static void expectMonotonic(const std::vector<int> & data, int stream_cnt)
    {
        std::vector<std::vector<int>> streams(stream_cnt);
        for (int x : data)
        {
            int p = x % stream_cnt;
            if (streams[p].empty())
                ASSERT_EQ(x, p);
            else
                ASSERT_EQ(x, streams[p].back() + stream_cnt);
            streams[p].push_back(x);
        }
    }

    static void expectEqualAfterSort(std::vector<int> & a, std::vector<int> & b)
    {
        ASSERT_EQ(a.size(), b.size());
        sort(begin(a), end(a));
        sort(begin(b), end(b));
        ASSERT_EQ(a, b);
    }

    struct ThrowInjectable
    {
        ThrowInjectable() = default;
        ThrowInjectable(const ThrowInjectable &) = delete;
        ThrowInjectable(ThrowInjectable && rhs)
        {
            throwOrMove(std::move(rhs));
        }

        ThrowInjectable & operator=(const ThrowInjectable &) = delete;
        ThrowInjectable & operator=(ThrowInjectable && rhs)
        {
            if (this != &rhs)
                throwOrMove(std::move(rhs));
            return *this;
        }

        explicit ThrowInjectable(int /* just avoid potential overload resolution error */, bool throw_when_construct)
        {
            if (throw_when_construct)
                throw TiFlashTestException("Throw when construct");
        }

        explicit ThrowInjectable(std::atomic<bool> * throw_when_move_)
            : throw_when_move(throw_when_move_)
        {
        }

        void throwOrMove(ThrowInjectable && rhs)
        {
            if (rhs.throw_when_move && rhs.throw_when_move->load())
                throw TiFlashTestException("Throw when move");
            throw_when_move = rhs.throw_when_move;
            rhs.throw_when_move = nullptr;
        }

        std::atomic<bool> * throw_when_move = nullptr;
    };
};

template <>
struct TestMPMCQueue::ValueHelper<int>
{
    static int make(int v) { return v; }
    static int extract(int v) { return v; }
};

template <>
struct TestMPMCQueue::ValueHelper<std::unique_ptr<int>>
{
    static std::unique_ptr<int> make(int v) { return std::make_unique<int>(v); }
    static int extract(std::unique_ptr<int> & v) { return *v; }
};

template <>
struct TestMPMCQueue::ValueHelper<std::shared_ptr<int>>
{
    static std::shared_ptr<int> make(int v) { return std::make_shared<int>(v); }
    static int extract(std::shared_ptr<int> & v) { return *v; }
};

#define ADD_TEST_FOR(type_name, type, test_name, ...) \
    TEST_F(TestMPMCQueue, type_name##_##test_name)    \
    try                                               \
    {                                                 \
        test##test_name<type>(__VA_ARGS__);           \
    }                                                 \
    CATCH

#define ADD_TEST(test_name, ...)                                              \
    ADD_TEST_FOR(Int, int, test_name, __VA_ARGS__)                            \
    ADD_TEST_FOR(UniquePtr_Int, std::unique_ptr<int>, test_name, __VA_ARGS__) \
    ADD_TEST_FOR(SharedPtr_Int, std::shared_ptr<int>, test_name, __VA_ARGS__)

ADD_TEST(Initialize, 10);
ADD_TEST(DuplicateFinishCancel);
ADD_TEST(SequentialPushPop, 100);
ADD_TEST(FinishEmpty, 100, 4);
ADD_TEST(ConcurrentPush, 2, 4);
ADD_TEST(ConcurrentPushPop, 2, 4);
ADD_TEST(PopAfterFinish, 100);
ADD_TEST(CancelEmpty, 4, 4);
ADD_TEST(CancelConcurrentPop, 4);
ADD_TEST(CancelConcurrentPush, 4);

TEST_F(TestMPMCQueue, ExceptionSafe)
try
{
    MPMCQueue<ThrowInjectable> queue(10);

    std::atomic<bool> throw_when_move = true;
    ThrowInjectable x(&throw_when_move);

    try
    {
        queue.push(std::move(x));
        ASSERT_TRUE(false); // should throw
    }
    catch (const TiFlashTestException &)
    {
    }

    ASSERT_EQ(queue.size(), 0);

    throw_when_move.store(false);
    queue.push(std::move(x));
    ASSERT_EQ(queue.size(), 1);

    throw_when_move.store(true);
    try
    {
        queue.pop();
        ASSERT_TRUE(false); // should throw
    }
    catch (const TiFlashTestException &)
    {
    }
    ASSERT_EQ(queue.size(), 1);

    throw_when_move.store(false);
    auto res = queue.pop();
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().throw_when_move, &throw_when_move);
    ASSERT_EQ(queue.size(), 0);

    try
    {
        queue.emplace(0, true);
        ASSERT_TRUE(false); // should throw
    }
    catch (const TiFlashTestException &)
    {
    }
    ASSERT_EQ(queue.size(), 0);

    queue.emplace(0, false);
    ASSERT_EQ(queue.size(), 1);
}
CATCH

} // namespace tests
} // namespace DB
