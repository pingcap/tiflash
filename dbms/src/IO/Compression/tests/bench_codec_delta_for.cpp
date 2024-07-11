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
#include <IO/Compression/CompressionCodecDeltaFOR.h>
#include <benchmark/benchmark.h>

#include <magic_enum.hpp>
#include <vector>

namespace DB::bench
{

template <typename T>
static void codecDeltaForOrdinaryBM(benchmark::State & state)
{
    std::vector<T> v(DEFAULT_MERGE_BLOCK_SIZE);
    for (auto & i : v)
        i = random();
    auto data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(T)).value();
    CompressionCodecDeltaFOR codec(data_type);
    char dest[sizeof(T) * DEFAULT_MERGE_BLOCK_SIZE + 1];
    for (auto _ : state)
    {
        auto compressed_size
            = codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(T), dest);
        codec.ordinaryDecompress(dest, compressed_size, reinterpret_cast<char *>(v.data()), v.size() * sizeof(T));
    }
}

static void codecDeltaForOrdinaryUInt32BM(benchmark::State & state)
{
    codecDeltaForOrdinaryBM<UInt32>(state);
}

static void codecDeltaForOrdinaryUInt64BM(benchmark::State & state)
{
    codecDeltaForOrdinaryBM<UInt64>(state);
}

static void codecDeltaForSpecializedUInt64BM(benchmark::State & state)
{
    std::vector<UInt64> v(DEFAULT_MERGE_BLOCK_SIZE);
    for (auto & i : v)
        i = random();
    auto data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(UInt64)).value();
    CompressionCodecDeltaFOR codec(data_type);
    char dest[sizeof(UInt64) * DEFAULT_MERGE_BLOCK_SIZE + 1];
    for (auto _ : state)
    {
        auto compressed_size
            = codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(UInt64), dest);
        codec.doDecompressData(dest, compressed_size, reinterpret_cast<char *>(v.data()), v.size() * sizeof(UInt64));
    }
}

static void codecDeltaForSpecializedUInt32BM(benchmark::State & state)
{
    std::vector<UInt32> v(DEFAULT_MERGE_BLOCK_SIZE);
    for (auto & i : v)
        i = random();
    auto data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(UInt32)).value();
    CompressionCodecDeltaFOR codec(data_type);
    char dest[sizeof(UInt32) * DEFAULT_MERGE_BLOCK_SIZE + 1];
    for (auto _ : state)
    {
        auto compressed_size
            = codec.doCompressData(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(UInt32), dest);
        codec.doDecompressData(dest, compressed_size, reinterpret_cast<char *>(v.data()), v.size() * sizeof(UInt32));
    }
}

BENCHMARK(codecDeltaForSpecializedUInt64BM);
BENCHMARK(codecDeltaForOrdinaryUInt64BM);
BENCHMARK(codecDeltaForSpecializedUInt32BM);
BENCHMARK(codecDeltaForOrdinaryUInt32BM);

} // namespace DB::bench
