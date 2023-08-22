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

#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Common/nocopyable.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace DB::tests
{
namespace
{
class MPMCQueueTest : public ::testing::Test
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
    void ensureOpFail(
        MPMCQueueResult op_ret,
        const MPMCQueue<T> & queue,
        size_t old_size,
        bool push,
        bool try_op,
        bool timed_op)
    {
        auto op = push ? "push" : "pop";
        auto throw_exp = [op] {
            throw TiFlashTestException(fmt::format("Should {} fail", op));
        };
        auto queue_status = queue.getStatus();
        auto new_size = queue.size();
        if (queue_status == MPMCQueueStatus::NORMAL)
        {
            if (timed_op && op_ret != MPMCQueueResult::TIMEOUT)
                throw_exp();
            if (try_op && ((push && op_ret != MPMCQueueResult::FULL) || (!push && op_ret != MPMCQueueResult::EMPTY)))
                throw_exp();
            if (!try_op && !timed_op && op_ret != MPMCQueueResult::OK)
                throw_exp();
        }
        else
        {
            if (static_cast<int>(op_ret) != static_cast<int>(queue_status))
                throw_exp();
        }
        if (old_size != new_size)
            throw TiFlashTestException(fmt::format("Size changed from {} to {} without {}", old_size, new_size, op));
    }

    template <typename T>
    void testCannotPush(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        ensureOpFail(queue.push(ValueHelper<T>::make(-1)), queue, old_size, true, false, false);
    }

    template <typename T>
    void testCannotTryPush(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        ensureOpFail(queue.tryPush(ValueHelper<T>::make(-1)), queue, old_size, true, true, false);
        ensureOpFail(
            queue.pushTimeout(ValueHelper<T>::make(-1), std::chrono::microseconds(1)),
            queue,
            old_size,
            true,
            false,
            true);
    }

    template <typename T>
    void testCannotPop(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        T res;
        ensureOpFail(queue.pop(res), queue, old_size, false, false, false);
    }

    template <typename T>
    void testCannotTryPop(MPMCQueue<T> & queue)
    {
        auto old_size = queue.size();
        T res;
        ensureOpFail(queue.tryPop(res), queue, old_size, false, true, false);
        ensureOpFail(queue.popTimeout(res, std::chrono::microseconds(1)), queue, old_size, false, false, true);
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
            T res;
            if (queue.pop(res) != MPMCQueueResult::OK)
                throw TiFlashTestException("Should pop a value");
            int expect = start++;
            int actual = ValueHelper<T>::extract(res);
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
        std::vector<MPMCQueueResult> reader_results(reader_cnt, MPMCQueueResult::EMPTY);

        auto read_func = [&](int i) {
            T res;
            reader_results[i] = queue.pop(res);
        };

        for (int i = 0; i < reader_cnt; ++i)
            readers.emplace_back(read_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.finish();

        for (int i = 0; i < reader_cnt; ++i)
        {
            readers[i].join();
            ASSERT_EQ(reader_results[i], MPMCQueueResult::FINISHED);
        }

        ASSERT_EQ(queue.size(), 0);

        for (int i = 0; i < 10; ++i)
        {
            T res;
            ASSERT_TRUE(queue.pop(res) == MPMCQueueResult::FINISHED);
        }

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.push(ValueHelper<T>::make(i));
            ASSERT_TRUE(res == MPMCQueueResult::FINISHED);
        }
    }

    template <typename T>
    void testCancelEmpty(Int64 capacity, int reader_cnt)
    {
        MPMCQueue<T> queue(capacity);
        std::vector<std::thread> readers;
        std::vector<MPMCQueueResult> reader_results(reader_cnt, MPMCQueueResult::EMPTY);

        auto read_func = [&](int i) {
            T res;
            reader_results[i] = queue.pop(res);
        };

        for (int i = 0; i < reader_cnt; ++i)
            readers.emplace_back(read_func, i);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queue.cancel();

        for (int i = 0; i < reader_cnt; ++i)
        {
            readers[i].join();
            ASSERT_EQ(reader_results[i], MPMCQueueResult::CANCELLED);
        }

        for (int i = 0; i < 10; ++i)
        {
            T res;
            ASSERT_TRUE(queue.pop(res) == MPMCQueueResult::CANCELLED);
        }

        for (int i = 0; i < 10; ++i)
        {
            auto res = queue.push(ValueHelper<T>::make(i));
            ASSERT_TRUE(res == MPMCQueueResult::CANCELLED);
        }
    }

    template <typename T>
    void testCancelConcurrentPop(int reader_cnt)
    {
        MPMCQueue<T> queue(1);
        std::vector<std::thread> threads;
        std::vector<int> reader_results(reader_cnt, 0);

        auto read_func = [&](int i) {
            T res;
            reader_results[i] += queue.pop(res) == MPMCQueueResult::OK;
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
            writer_results[i] += res == MPMCQueueResult::OK;
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
                T res;
                if (queue.pop(res) == MPMCQueueResult::OK)
                    reader_results.push_back(ValueHelper<T>::extract(res));
                else
                    break;
            }
        };

        auto write_func = [&](int i) {
            for (int x = i;; x += writer_cnt)
            {
                auto res = queue.push(ValueHelper<T>::make(x));
                if (res == MPMCQueueResult::OK)
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
                T res;
                if (queue.pop(res) == MPMCQueueResult::OK)
                    reader_results[i].push_back(ValueHelper<T>::extract(res));
                else
                    break;
            }
        };

        auto write_func = [&](int i) {
            for (int x = i;; x += reader_writer_cnt)
            {
                auto res = queue.push(ValueHelper<T>::make(x));
                if (res == MPMCQueueResult::OK)
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

        DISALLOW_COPY(ThrowInjectable);

        ThrowInjectable(ThrowInjectable && rhs) { throwOrMove(std::move(rhs)); }

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
        {}

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
struct MPMCQueueTest::ValueHelper<int>
{
    static int make(int v) { return v; }
    static int extract(int v) { return v; }
};

template <>
struct MPMCQueueTest::ValueHelper<std::unique_ptr<int>>
{
    static std::unique_ptr<int> make(int v) { return std::make_unique<int>(v); }
    static int extract(std::unique_ptr<int> & v) { return *v; }
};

template <>
struct MPMCQueueTest::ValueHelper<std::shared_ptr<int>>
{
    static std::shared_ptr<int> make(int v) { return std::make_shared<int>(v); }
    static int extract(std::shared_ptr<int> & v) { return *v; }
};

#define ADD_TEST_FOR(type_name, type, test_name, ...) \
    TEST_F(MPMCQueueTest, type_name##_##test_name)    \
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

TEST_F(MPMCQueueTest, ExceptionSafe)
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
        ThrowInjectable res;
        queue.pop(res);
        ASSERT_TRUE(false); // should throw
    }
    catch (const TiFlashTestException &)
    {
    }
    ASSERT_EQ(queue.size(), 1);

    throw_when_move.store(false);
    ThrowInjectable res;
    ASSERT_EQ(queue.pop(res), MPMCQueueResult::OK);
    ASSERT_EQ(res.throw_when_move, &throw_when_move);
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

struct Counter
{
    static int count;
    Counter() { ++count; }

    ~Counter() { --count; }
};
int Counter::count = 0;

TEST_F(MPMCQueueTest, objectsDestructed)
try
{
    {
        MPMCQueue<Counter> queue(100);
        queue.emplace();
        ASSERT_EQ(Counter::count, 1);

        {
            Counter cnt;
            queue.pop(cnt);
        }
        ASSERT_EQ(Counter::count, 0);

        queue.emplace();
        ASSERT_EQ(Counter::count, 1);
    }
    ASSERT_EQ(Counter::count, 0);
}
CATCH

TEST_F(MPMCQueueTest, AuxiliaryMemoryBound)
try
{
    size_t max_size = 10;
    Int64 auxiliary_memory_bound = 0;
    Int64 value;

    {
        /// case 1: no auxiliary memory usage bound
        MPMCQueue<Int64> queue(max_size);
        for (size_t i = 0; i < max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
    }

    {
        /// case 2: less auxiliary memory bound than the capacity bound
        size_t actual_max_size = 5;
        auxiliary_memory_bound = sizeof(Int64) * actual_max_size;
        MPMCQueue<Int64> queue(CapacityLimits(max_size, auxiliary_memory_bound), [](const Int64 &) {
            return sizeof(Int64);
        });
        for (size_t i = 0; i < actual_max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(actual_max_size) == MPMCQueueResult::FULL);
        /// after pop one element, the queue can be pushed again
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(actual_max_size) == MPMCQueueResult::OK);
    }

    {
        /// case 3: less capacity bound than the auxiliary memory bound
        auxiliary_memory_bound = sizeof(Int64) * (max_size * 10);
        MPMCQueue<Int64> queue(CapacityLimits(max_size, auxiliary_memory_bound), [](const Int64 &) {
            return sizeof(Int64);
        });
        for (size_t i = 0; i < max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
    }

    {
        /// case 4, auxiliary memory bound <= 0 means unbounded for auxiliary memory usage
        std::vector<Int64> bounds{0, -1};
        for (const auto & bound : bounds)
        {
            MPMCQueue<Int64> queue(CapacityLimits(max_size, bound), [](const Int64 &) { return 1024 * 1024; });
            for (size_t i = 0; i < max_size; i++)
                ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
            ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
        }
    }

    {
        /// case 5 even if the element's auxiliary memory is out of bound, at least one element can be pushed
        MPMCQueue<Int64> queue(CapacityLimits(max_size, 1), [](const Int64 &) { return 10; });
        ASSERT_TRUE(queue.tryPush(1) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(2) == MPMCQueueResult::FULL);
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::EMPTY);
        ASSERT_TRUE(queue.tryPush(1) == MPMCQueueResult::OK);
    }

    {
        /// case 6 after pop a huge element, more than one small push can be notified without further pop
        MPMCQueue<Int64> queue(CapacityLimits(max_size, 20), [](const Int64 & element) { return std::abs(element); });
        ASSERT_TRUE(queue.tryPush(100) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(5) == MPMCQueueResult::FULL);
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "thread_1", [&]() { queue.push(5); });
        thread_manager->schedule(false, "thread_2", [&]() { queue.push(6); });
        std::exception_ptr current_exception = nullptr;
        try
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            ASSERT_TRUE(queue.pop(value) == MPMCQueueResult::OK);
            ASSERT_EQ(value, 100);
            std::this_thread::sleep_for(std::chrono::seconds(5));
            ASSERT_EQ(queue.size(), 2);
        }
        catch (...)
        {
            current_exception = std::current_exception();
            queue.cancelWith("test failed");
        }
        thread_manager->wait();
        if (current_exception)
            std::rethrow_exception(current_exception);
    }
}
CATCH

} // namespace
} // namespace DB::tests
