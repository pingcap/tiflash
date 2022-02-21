#include <Common/DynamicThreadPool.h>
#include <Common/Stopwatch.h>

namespace DB
{
DynamicThreadPool::~DynamicThreadPool()
{
    for (auto & queue : fixed_queues)
        queue->finish();

    for (auto & thread : fixed_threads)
        thread.join();

    {
        std::unique_lock lock(dynamic_mutex);
        in_destructing = true;
        // do not need to detach node here
        for (auto * node = dynamic_idle_head.next; node != &dynamic_idle_head; node = node->next)
            node->cv.notify_one();
    }

    // TODO: maybe use a latch is more elegant.
    while (alive_dynamic_threads.load() != 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

DynamicThreadPool::ThreadCount DynamicThreadPool::threadCount() const
{
    ThreadCount cnt;
    cnt.fixed = fixed_threads.size();
    cnt.dynamic = alive_dynamic_threads.load();
    return cnt;
}

void DynamicThreadPool::init(size_t initial_size)
{
    fixed_queues.reserve(initial_size);
    fixed_threads.reserve(initial_size);
    for (size_t i = 0; i < initial_size; ++i)
    {
        fixed_queues.emplace_back(std::make_unique<Queue>(1)); // each Queue will only contain at most 1 task.
        idle_fixed_queues.push(fixed_queues.back().get());
        fixed_threads.emplace_back(ThreadFactory::newThread(false, "FixedThread", &DynamicThreadPool::fixedWork, this, i));
    }
}

void DynamicThreadPool::scheduleTask(TaskPtr task)
{
    if (!scheduledToFixedThread(task) && !scheduledToExistedDynamicThread(task))
        scheduledToNewDynamicThread(task);
}

bool DynamicThreadPool::scheduledToFixedThread(TaskPtr & task)
{
    Queue * queue = nullptr;
    if (idle_fixed_queues.pop(queue))
    {
        queue->push(std::move(task));
        return true;
    }
    return false;
}

bool DynamicThreadPool::scheduledToExistedDynamicThread(TaskPtr & task)
{
    std::unique_lock lock(dynamic_mutex);
    if (dynamic_idle_head.isSingle())
        return false;
    DynamicNode * node = dynamic_idle_head.next;
    // detach node to avoid assigning two tasks to node.
    node->detach();
    node->task = std::move(task);
    node->cv.notify_one();
    return true;
}

void DynamicThreadPool::scheduledToNewDynamicThread(TaskPtr & task)
{
    alive_dynamic_threads.fetch_add(1);
    std::thread t = ThreadFactory::newThread(false, "DynamicThread", &DynamicThreadPool::dynamicWork, this, std::move(task));
    t.detach();
}
std::atomic<int> cur_thread_num{0}, max_thread_num{0};
std::atomic<long> last_thd_ts{0};
void DynamicThreadPool::fixedWork(size_t index)
{
    Queue * queue = fixed_queues[index].get();
    while (true)
    {
        TaskPtr task;
        queue->pop(task);
        if (!task)
            break;
        long long now = StopWatchDetail::nanoseconds(CLOCK_MONOTONIC)/1000/1000/1000;
        cur_thread_num++;
        max_thread_num = std::max(cur_thread_num.load(), max_thread_num.load());
         if (last_thd_ts != now) {
            last_thd_ts = now;
            std::cerr<<"max_thd: "<<max_thread_num.load()<<" cur_thd: "<<cur_thread_num.load()<<std::endl;
        }
        task->execute();
        cur_thread_num--;

        idle_fixed_queues.push(queue);
    }
}

void DynamicThreadPool::dynamicWork(TaskPtr initial_task)
{
    cur_thread_num++;
    max_thread_num = std::max(cur_thread_num.load(), max_thread_num.load());
    long long now = StopWatchDetail::nanoseconds(CLOCK_MONOTONIC)/1000/1000/1000;
    if (last_thd_ts != now) {
        last_thd_ts = now;
        std::cerr<<"max_thd: "<<max_thread_num.load()<<" cur_thd: "<<cur_thread_num.load()<<std::endl;
    }
    initial_task->execute();
    cur_thread_num--;
    DynamicNode node;
    while (true)
    {
        {
            std::unique_lock lock(dynamic_mutex);
            if (in_destructing)
                break;
            // attach to just after head to reuse hot threads so that cold threads have chance to exit
            node.appendTo(&dynamic_idle_head);
            node.cv.wait_for(lock, dynamic_auto_shrink_cooldown);
            node.detach();
        }

        if (!node.task) // may be timeout or cancelled
            break;
        long long now = StopWatchDetail::nanoseconds(CLOCK_MONOTONIC)/1000/1000/1000;
        cur_thread_num++;
        max_thread_num = std::max(cur_thread_num.load(), max_thread_num.load());
        if (last_thd_ts != now) {
        last_thd_ts = now;
        std::cerr<<"max_thd: "<<max_thread_num.load()<<" cur_thd: "<<cur_thread_num.load()<<std::endl;
    }
        node.task->execute();
        cur_thread_num--;
        node.task.reset();
    }
    alive_dynamic_threads.fetch_sub(1);
}

std::unique_ptr<DynamicThreadPool> DynamicThreadPool::global_instance;
} // namespace DB
