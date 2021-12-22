#include <Common/DynamicThreadPool.h>

namespace DB
{
DynamicThreadPool::DynamicThreadPool(size_t initial_size)
    : idle_fixed_queues(initial_size)
{
    for (size_t i = 0; i < initial_size; ++i)
        fixed_queues.emplace_back(std::make_unique<Queue>(2)); // reserve 2 slots to avoid blocking: one schedule, one dtor.

    for (size_t i = 0; i < initial_size; ++i)
        fixed_threads.emplace_back(ThreadFactory(true, "FixedThread").newThread(&DynamicThreadPool::fixed_work, this, i));
}

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
    if (dynamic_idle_head.noFollowers())
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
    std::thread t = ThreadFactory(true, "DynamicThread").newThread(&DynamicThreadPool::dynamic_work, this, std::move(task));
    t.detach();
}

void DynamicThreadPool::fixed_work(size_t index)
{
    Queue * queue = fixed_queues[index].get();
    while (true)
    {
        idle_fixed_queues.push(queue);
        TaskPtr task;
        queue->pop(task);
        if (!task)
            break;
        task->execute();
    }
}

void DynamicThreadPool::dynamic_work(TaskPtr initial_task)
{
    initial_task->execute();

    static constexpr auto timeout = std::chrono::minutes(10);
    DynamicNode node;
    while (true)
    {
        {
            std::unique_lock lock(dynamic_mutex);
            if (in_destructing)
                break;
            node.prepend(&dynamic_idle_head); // prepend to reuse hot threads so that cold threads have chance to exit
            node.cv.wait_for(lock, timeout);
            node.detach();
        }

        if (!node.task) // may be timeout or cancelled
            break;
        node.task->execute();
        node.task.reset();
    }
    alive_dynamic_threads.fetch_sub(1);
}
} // namespace DB
