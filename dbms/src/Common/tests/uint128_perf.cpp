#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <sched.h>
#endif

#include <algorithm>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <random>

#include <Poco/Exception.h>
#include <Core/Defines.h>
#include <common/UInt128.h>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

namespace
{
void setAffinity()
{
#if !defined(__APPLE__) && !defined(__FreeBSD__)
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);

    if (-1 == sched_setaffinity(0, sizeof(mask), &mask))
        throw Poco::Exception("Cannot set CPU affinity");
#else
    /** MacOS X by default have THREAD_AFFINITY_NULL
     *  See: https://developer.apple.com/library/content/releasenotes/Performance/RN-AffinityAPI/
     */
#endif
}

void fill_data(std::vector<DB::UInt128> & data, int count)
{
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<DB::UInt64> dist;

    data.resize(count);
    for (int i = 0; i < count; ++i)
        data[i] = DB::UInt128(dist(mt), dist(mt));
}

void sort_data(std::vector<DB::UInt128> & data)
{
    std::sort(begin(data), end(data));
}
} // namespace

int main(int /*argc*/, char ** argv)
{

#if !__x86_64__
    std::cerr << "Only for x86_64 arch" << std::endl;
#endif

    const int count = atoi(argv[1]);
    const int round = atoi(argv[2]);

    std::cerr << "Count: " << count
        << ", Round: " << round
        << "Start" << std::endl << std::endl;

    setAffinity();

    std::vector<DB::UInt128> data;
    for (int i = 0; i < round; ++i)
    {
        std::cerr << "Round " << i + 1;
        fill_data(data, count);
        auto start = std::chrono::high_resolution_clock::now();
        sort_data(data);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cerr << ", Time: " << duration.count() << " us." << std::endl;
    }

    std::cerr << std::endl << "End" << std::endl;

    return 0;
}

