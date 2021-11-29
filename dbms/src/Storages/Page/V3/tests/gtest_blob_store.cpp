#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Storages/Page/V3/BlobStore.h>

namespace DB::PS::V3::tests
{

std::atomic_flag lock = ATOMIC_FLAG_INIT;

void f(int n)
{
    while(true)
    {
        while (lock.test(std::memory_order_relaxed))
        {
            sleep(1);
            printf("thread n : %d , stop in here.\n",n);    
        }
        // work
        sleep(1);
        printf("thread n : %d, running,\n",n);
    }
}

TEST(BlobStoreTest, test_atomic_flag)
{
    std::vector<std::thread> v;
    for (int n = 0; n < 2; ++n) {
        v.emplace_back(f , n);
    }
    sleep(5);
    lock.test_and_set(std::memory_order_acquire);
    printf("lock.test_and_set ! \n");
    sleep(5);
    lock.clear(std::memory_order_release);
    printf("lock.clear ! \n");
    sleep(5);

    // for (auto& t : v) {
    //     t.join();
    // }
}


TEST(BlobStatsTest, test_stats)
{
    BlobStore::BlobStats stats(100);
    stats.addSMMaxCaps(1);

    
    
}

}