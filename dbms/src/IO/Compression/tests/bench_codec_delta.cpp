// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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