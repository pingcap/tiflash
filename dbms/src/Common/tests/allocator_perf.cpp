#include <Common/Allocator.h>
#include <common/config_common.h>
#include <fmt/format.h>
#include <fmt/printf.h>

#include <chrono>
#include <cstdlib>

static constexpr size_t KiB = 1024;
static constexpr size_t MiB = 1024 * KiB;
static constexpr size_t GiB = 1024 * MiB;

template <bool clear_memory>
bool run_perf_test(const int type)
{
    fmt::print("Running with clear_memory: {}\n", clear_memory);

    Allocator<clear_memory> alloc;

    if (type == 1)
    {
        auto a = std::chrono::high_resolution_clock::now();
        size_t size = 50 * MiB;
        auto * p = alloc.alloc(size);
        size_t old_size = size;
        for (; size < 1 * GiB; size += 50 * MiB)
        {
            p = alloc.realloc(p, old_size, size);
            old_size = size;
        }
        alloc.free(p, old_size);
        auto b = std::chrono::high_resolution_clock::now();
        fmt::print("50mb+50mb+..       ok. cost {}ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count());
    }

    if (type == 2)
    {
        auto a = std::chrono::high_resolution_clock::now();
        size_t size = 1;
        auto * p = alloc.alloc(size);
        size_t old_size = size;
        for (; size < 1 * GiB; size *= 2)
        {
            p = alloc.realloc(p, old_size, size);
            old_size = size;
        }
        alloc.free(p, old_size);
        auto b = std::chrono::high_resolution_clock::now();
        fmt::print("1,2,4,8,..,1G      ok. cost {}ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count());
    }

    if (type == 3)
    {
        auto a = std::chrono::high_resolution_clock::now();
        size_t size = 1 * GiB;
        auto * p = alloc.alloc(size);
        size_t old_size = size;
        for (; size > 1000; size /= 2)
        {
            p = alloc.realloc(p, old_size, size);
            old_size = size;
        }
        alloc.free(p, old_size);
        auto b = std::chrono::high_resolution_clock::now();
        fmt::print("1gb,512mb,128mb,.. ok. cost {}ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count());
    }
    return true;
}

void dump_profile(int type)
{
    fmt::print(
        R"raw(USE_JEMALLOC={}
USE_TCMALLOC={}
USE_MIMALLOC={}
type={}
)raw",
        USE_JEMALLOC,
        USE_TCMALLOC,
        USE_MIMALLOC,
        type);
}

int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        fmt::print(stderr, "Usage: {} type\n", argv[0]);
        return -1;
    }

    const int type = strtol(argv[1], nullptr, 10);
    dump_profile(type);
    run_perf_test<false>(type);
    run_perf_test<true>(type);
    return 0;
}
