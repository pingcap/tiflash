#include <Common/FiberPool.hpp>
#include <iostream>
#include <thread>

using Queue = boost::fibers::buffered_channel<int>;

void run_in_fiber(Queue & queue)
{
    int v = 0;
    while (true)
    {
        boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
        auto ret = queue.push(v + 1);
        if (ret == boost::fibers::channel_op_status::closed)
            break;
        std::cout << "fiber pushed " << v + 1 << std::endl;
         ret = queue.pop(v);
        if (ret == boost::fibers::channel_op_status::closed)
            break;
        std::cout << "fiber got " << v << std::endl;
    }
    std::cout << "fiber exit" << std::endl;
}

void run_in_thread(Queue & queue)
{
    int v = 0;
    while (true)
    {
        auto ret = queue.pop(v);
        if (ret == boost::fibers::channel_op_status::closed)
            break;
        std::cout << "thread got " << v << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ret = queue.push(v + 1);
        if (ret == boost::fibers::channel_op_status::closed)
            break;
        std::cout << "thread pushed " << v + 1 << std::endl;
    }
    std::cout << "thread exit" << std::endl;
}

int main()
{
    Queue queue(2);
    boost::fibers::fiber f([&] { run_in_fiber(queue); });
    std::thread t([&] { run_in_thread(queue); });
    std::cout << "main thread start sleep" << std::endl;
    boost::this_fiber::sleep_for(std::chrono::seconds(3));
    std::cout << "main thread end sleep" << std::endl;
    queue.close();
    f.join();
    t.join();
}

