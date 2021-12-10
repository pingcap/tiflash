#include <Common/DynamicThreadPool.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
TEST_F(DynamicThreadPool, TestInterDependent)
try
{
    DynamicThreadPool pool(1);
    std::atomic<int> a = 0;

    auto f0 = pool.schedule([&] {
        while (true)
        {
            if (a.load())
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto f1 = pool.schedule([&] {
        a.store(1);
    });

    f0.wait();
    f1.wait();
}
CATCH
} // namespace DB::tests
