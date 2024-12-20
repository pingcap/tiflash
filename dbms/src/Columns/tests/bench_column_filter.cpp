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


#include <Columns/ColumnVector.h>
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/typeid_cast.h>
#include <benchmark/benchmark.h>

#include <random>

using namespace DB;

namespace bench
{
IColumn::Filter createRandomFilter(size_t n, size_t set_n)
{
    assert(n >= set_n);

    IColumn::Filter filter(set_n, 1);
    filter.resize_fill_zero(n, 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(filter.begin(), filter.end(), g);
    return filter;
}

ColumnPtr filterSSE2(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    const auto & data = typeid_cast<const ColumnVector<Int64> *>(col.get())->getData();
    size_t size = col->size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnVector<Int64>::create();
    using Container = ColumnVector<Int64>::Container;
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const Int64 * data_pos = &data[0];

#if __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse)
    {
        int mask
            = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
        }
        else if (0xFFFF == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            for (size_t i = 0; i < SIMD_BYTES; ++i)
                if (filt_pos[i])
                    res_data.push_back(data_pos[i]);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
#endif

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

ColumnPtr filterAVX2(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    const auto & data = typeid_cast<const ColumnVector<Int64> *>(col.get())->getData();
    size_t size = col->size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnVector<Int64>::create();
    using Container = ColumnVector<Int64>::Container;
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const Int64 * data_pos = &data[0];

    filterImpl(filt_pos, filt_end, data_pos, res_data);

    return res;
}

enum class FilterVersion
{
    SSE2,
    AVX2,
};

template <typename... Args>
void columnFilter(benchmark::State & state, Args &&... args)
{
    auto [version, n, set_percent] = std::make_tuple(std::move(args)...);
    auto mut_col = ColumnVector<Int64>::create();
    auto & v = mut_col->getData();
    v.resize(n);
    std::iota(v.begin(), v.end(), 0);
    auto set_n = n * set_percent;
    auto filter = createRandomFilter(n, set_n);

    ColumnPtr col = std::move(mut_col);

    if (version == FilterVersion::SSE2)
    {
        for (auto _ : state)
        {
            auto t = filterSSE2(col, filter, set_n * sizeof(Int64));
            benchmark::DoNotOptimize(t);
        }
    }
    else
    {
        for (auto _ : state)
        {
            auto t = filterAVX2(col, filter, set_n * sizeof(Int64));
            benchmark::DoNotOptimize(t);
        }
    }
}

BENCHMARK_CAPTURE(columnFilter, sse2_00, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.00);
BENCHMARK_CAPTURE(columnFilter, avx2_00, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.00);
BENCHMARK_CAPTURE(columnFilter, sse2_01, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.01);
BENCHMARK_CAPTURE(columnFilter, avx2_01, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.01);
BENCHMARK_CAPTURE(columnFilter, sse2_10, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.10);
BENCHMARK_CAPTURE(columnFilter, avx2_10, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.10);
BENCHMARK_CAPTURE(columnFilter, sse2_20, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.20);
BENCHMARK_CAPTURE(columnFilter, avx2_20, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.20);
BENCHMARK_CAPTURE(columnFilter, sse2_30, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.30);
BENCHMARK_CAPTURE(columnFilter, avx2_30, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.30);
BENCHMARK_CAPTURE(columnFilter, sse2_40, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.40);
BENCHMARK_CAPTURE(columnFilter, avx2_40, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.40);
BENCHMARK_CAPTURE(columnFilter, sse2_50, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.50);
BENCHMARK_CAPTURE(columnFilter, avx2_50, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.50);
BENCHMARK_CAPTURE(columnFilter, sse2_60, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.60);
BENCHMARK_CAPTURE(columnFilter, avx2_60, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.60);
BENCHMARK_CAPTURE(columnFilter, sse2_70, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.70);
BENCHMARK_CAPTURE(columnFilter, avx2_70, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.70);
BENCHMARK_CAPTURE(columnFilter, sse2_80, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.80);
BENCHMARK_CAPTURE(columnFilter, avx2_80, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.80);
BENCHMARK_CAPTURE(columnFilter, sse2_90, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.90);
BENCHMARK_CAPTURE(columnFilter, avx2_90, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.90);
BENCHMARK_CAPTURE(columnFilter, sse2_99, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 0.99);
BENCHMARK_CAPTURE(columnFilter, avx2_99, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.99);
BENCHMARK_CAPTURE(columnFilter, sse2_100, FilterVersion::SSE2, DEFAULT_BLOCK_SIZE, 1.00);
BENCHMARK_CAPTURE(columnFilter, avx2_100, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 1.00);

} // namespace bench
