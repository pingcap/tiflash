#include <gtest/gtest.h>

#include "Common/ElasticThreadPool.h"

namespace DB::tests
{
void scheduleTaskSet(size_t thd_set_size, std::vector<std::future<void>> & futures, ElasticThreadPool & pool, ElasticThreadPool::Job job)
{
    for (size_t i = 0; i < thd_set_size; i++)
    {
        futures.push_back(pool.schedule(job));
    }
}

TEST(ElasticThdPool, TestThdSet)
{
    size_t thd_set_size = 200;
    size_t init_pool_cap = 50;
    auto recycle_period = std::chrono::milliseconds(200);
    ElasticThreadPool pool(init_pool_cap, recycle_period); //shrink period: 200ms
    std::atomic<size_t> run_cnt = 0;
    std::atomic<size_t> output = 0;
    std::vector<std::future<void>> futures;
    //TaskSet: the computation won't begin until all threads are running and a trigger is fired
    scheduleTaskSet(thd_set_size, futures, pool, [&] {
        run_cnt++;
        while (run_cnt.load() != thd_set_size + 1)
            usleep(1);
        output++;
    });


    EXPECT_EQ(pool.getAvailableCnt(), (size_t)0);
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size);
    //fire the trigger
    scheduleTaskSet(1, futures, pool, [&] {
        run_cnt++;
    });
    waitTasks(futures);
    EXPECT_EQ(output.load(), thd_set_size);
    EXPECT_EQ(pool.getAvailableCnt(), thd_set_size + 1);
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size + 1);

    //wait background task to shrink
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(pool.getAvailableCnt(), init_pool_cap);
    EXPECT_EQ(pool.getAliveCnt(), init_pool_cap);
}

TEST(ElasticThdPool, TestMultiThdSet)
{
    size_t thd_set_size = 200;
    size_t init_pool_cap = 50;
    auto recycle_period = std::chrono::milliseconds(200);
    ElasticThreadPool pool(init_pool_cap, recycle_period);
    std::atomic<size_t> run_cnt_for_case2 = 0, end_fg_for_case1 = 0;
    std::atomic<size_t> output = 0;
    std::vector<std::future<void>> futures;
    //TaskSet1: keep running until end_fg_for_case1 is set.
    scheduleTaskSet(thd_set_size, futures, pool, [&] {
        while (!end_fg_for_case1)
            usleep(100);
    });
    EXPECT_EQ(pool.getAvailableCnt(), (size_t)0);
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size);
    std::vector<std::future<void>> futures2;
    //TaskSet2: the computation won't begin until all threads are running  and a trigger is fired
    scheduleTaskSet(thd_set_size, futures2, pool, [&] {
        run_cnt_for_case2++;
        while (run_cnt_for_case2.load() != thd_set_size + 1)
            usleep(1);
        output++;
    });
    EXPECT_EQ(pool.getAvailableCnt(), (size_t)0);
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size + thd_set_size);
    //fire the trigger
    scheduleTaskSet(1, futures2, pool, [&] {
        run_cnt_for_case2++;
    });
    waitTasks(futures2);
    EXPECT_EQ(output.load(), thd_set_size);
    EXPECT_EQ(pool.getAvailableCnt(), thd_set_size + 1);
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size + thd_set_size + 1);

    //wait background task to shrink
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(pool.getAvailableCnt(), pool.getIdleBufferSize());
    EXPECT_EQ(pool.getAliveCnt(), thd_set_size + pool.getIdleBufferSize());

    end_fg_for_case1 = true;
    waitTasks(futures);

    //wait background task to shrink
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(pool.getAvailableCnt(), init_pool_cap);
    EXPECT_EQ(pool.getAliveCnt(), init_pool_cap);
}

} // namespace DB::tests