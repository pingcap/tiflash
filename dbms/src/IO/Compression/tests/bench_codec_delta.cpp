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
        char dest[sizeof(T) * DEFAULT_MERGE_BLOCK_SIZE + 1];
        codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(T), dest);
        codec.ordinaryDecompress(
            dest,
            sizeof(T) * DEFAULT_MERGE_BLOCK_SIZE + 1,
            reinterpret_cast<char *>(v.data()),
            v.size() * sizeof(T));
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
        char dest[sizeof(UInt64) * DEFAULT_MERGE_BLOCK_SIZE + 1];
        codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(UInt64), dest);
        codec.doDecompressData(
            dest,
            sizeof(UInt64) * DEFAULT_MERGE_BLOCK_SIZE + 1,
            reinterpret_cast<char *>(v.data()),
            v.size() * sizeof(UInt64));
    }
}

static void codecDeltaSpecializedUInt32BM(benchmark::State & state)
{
    std::vector<UInt32> v(DEFAULT_MERGE_BLOCK_SIZE);
    std::iota(v.begin(), v.end(), 0);
    for (auto _ : state)
    {
        CompressionCodecDelta codec(sizeof(UInt32));
        char dest[sizeof(UInt32) * DEFAULT_MERGE_BLOCK_SIZE + 1];
        codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(UInt32), dest);
        codec.doDecompressData(
            dest,
            sizeof(UInt32) * DEFAULT_MERGE_BLOCK_SIZE + 1,
            reinterpret_cast<char *>(v.data()),
            v.size() * sizeof(UInt32));
    }
}

BENCHMARK(codecDeltaSpecializedUInt64BM);
BENCHMARK(codecDeltaOrdinaryUInt64BM);
BENCHMARK(codecDeltaSpecializedUInt32BM);
BENCHMARK(codecDeltaOrdinaryUInt32BM);

} // namespace DB::bench