#include <Common/DynamicThreadPool.h>

namespace DB
{
struct DynamicThreadPool::DynamicNode
{
    DynamicNode()
    {
        next = this;
        prev = this;
    }

    /// valid when call on a single node whose next and prev is itself.
    void prepend(DynamicNode * head)
    {
        next = head->next;
        prev = head;
        head->next->prev = this;
        head->next = this;
    }

    /// valid when call on a single node whose next and prev is itself.
    void detach()
    {
        prev->next = next;
        next->prev = prev;
        next = this;
        prev = this;
    }

    bool noFollowers() const
    {
        return next == prev;
    }

    DynamicNode * next;
    DynamicNode * prev;
    std::condition_variable cv;
    TaskPtr task;
};

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
        for (auto * node = dynamic_idle_head->next; node != dynamic_idle_head.get(); node = node->next)
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
    dynamic_idle_head = std::make_unique<DynamicNode>();

    for (size_t i = 0; i < initial_size; ++i)
        fixed_queues.emplace_back(std::make_unique<Queue>(2)); // reserve 2 slots to avoid blocking: one schedule, one dtor.

    for (size_t i = 0; i < initial_size; ++i)
        fixed_threads.emplace_back(ThreadFactory(true, "FixedThread").newThread(&DynamicThreadPool::fixed_work, this, i));
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
    if (dynamic_idle_head->noFollowers())
        return false;
    DynamicNode * node = dynamic_idle_head->next;
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

    DynamicNode node;
    while (true)
    {
        {
            std::unique_lock lock(dynamic_mutex);
            if (in_destructing)
                break;
            node.prepend(dynamic_idle_head.get()); // prepend to reuse hot threads so that cold threads have chance to exit
            node.cv.wait_for(lock, dynamic_auto_shrink_cooldown);
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
