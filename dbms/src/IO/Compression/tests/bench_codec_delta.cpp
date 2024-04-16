#include <Core/Defines.h>
#include <IO/Compression/CompressionCodecDelta.h>
#include <benchmark/benchmark.h>

#include <vector>

namespace DB::bench
{

template <typename T>
static void codecDeltaOrdinaryBM(benchmark::State & state)
{
    std::vector<T> v(DEFAULT_MERGE_BLOCK_SIZE);
    std::iota(v.begin(), v.end(), 0);
    for (auto _ : state)
    {
        CompressionCodecDelta codec(sizeof(T));
        std::vector<T> dest(DEFAULT_MERGE_BLOCK_SIZE);
        codec.ordinaryCompress(
            reinterpret_cast<const char *>(v.data()),
            v.size() * sizeof(T),
            reinterpret_cast<char *>(dest.data()));
    }
}

static void codecDeltaOrdinaryUInt32BM(benchmark::State & state)
{
    codecDeltaOrdinaryBM<UInt32>(state);
}

static void codecDeltaOrdinaryUInt64BM(benchmark::State & state)
{
    codecDeltaOrdinaryBM<UInt64>(state);
}

static void codecDeltaSpecializedUInt64BM(benchmark::State & state)
{
    std::vector<UInt64> v(DEFAULT_MERGE_BLOCK_SIZE);
    std::iota(v.begin(), v.end(), 0);
    for (auto _ : state)
    {
        CompressionCodecDelta codec(sizeof(UInt64));
        std::vector<UInt64> dest(DEFAULT_MERGE_BLOCK_SIZE);
        codec.specializedUInt64Compress(
            reinterpret_cast<const char *>(v.data()),
            v.size() * sizeof(UInt64),
            reinterpret_cast<char *>(dest.data()));
    }
}

static void codecDeltaSpecializedUInt32BM(benchmark::State & state)
{
    std::vector<UInt32> v(DEFAULT_MERGE_BLOCK_SIZE);
    std::iota(v.begin(), v.end(), 0);
    for (auto _ : state)
    {
        CompressionCodecDelta codec(sizeof(UInt32));
        std::vector<UInt32> dest(DEFAULT_MERGE_BLOCK_SIZE);
        codec.specializedUInt32Compress(
            reinterpret_cast<const char *>(v.data()),
            v.size() * sizeof(UInt32),
            reinterpret_cast<char *>(dest.data()));
    }
}

BENCHMARK(codecDeltaSpecializedUInt64BM);
BENCHMARK(codecDeltaOrdinaryUInt64BM);
BENCHMARK(codecDeltaSpecializedUInt32BM);
BENCHMARK(codecDeltaOrdinaryUInt32BM);

} // namespace DB::bench