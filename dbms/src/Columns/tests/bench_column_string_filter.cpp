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


#include <Columns/ColumnString.h>
#include <Columns/ColumnUtil.h>
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/typeid_cast.h>
#include <benchmark/benchmark.h>

#include <random>

using namespace DB;

namespace bench::String
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

struct ResultOffsetsBuilder
{
    IColumn::Offsets & res_offsets;
    IColumn::Offset current_src_offset = 0;

    explicit ResultOffsetsBuilder(IColumn::Offsets * res_offsets_)
        : res_offsets(*res_offsets_)
    {}

    void reserve(size_t result_size_hint) { res_offsets.reserve(result_size_hint); }

    void insertChunk(
        size_t n,
        const IColumn::Offset * src_offsets_pos,
        bool first,
        IColumn::Offset chunk_offset,
        size_t chunk_size)
    {
        const auto offsets_size_old = res_offsets.size();
        res_offsets.resize(offsets_size_old + n);
        inline_memcpy(&res_offsets[offsets_size_old], src_offsets_pos, n * sizeof(IColumn::Offset));

        if (!first)
        {
            /// difference between current and actual offset
            const auto diff_offset = chunk_offset - current_src_offset;

            if (diff_offset > 0)
            {
                auto * res_offsets_pos = &res_offsets[offsets_size_old];

                /// adjust offsets
                for (size_t i = 0; i < n; ++i)
                    res_offsets_pos[i] -= diff_offset;
            }
        }
        current_src_offset += chunk_size;
    }
};

ColumnPtr filterAVX2(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    auto res = ColumnString::create();

    const auto & src = typeid_cast<const ColumnString &>(*col);
    const auto & src_elems = src.getChars();
    const auto & src_offsets = src.getOffsets();

    auto & res_elems = res->getChars();
    auto & res_offsets = res->getOffsets();

    const size_t size = src_offsets.size();
    if (size != filt.size())
        throw Exception(
            fmt::format("size of filter {} doesn't match size of column {}", filt.size(), size),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ResultOffsetsBuilder result_offsets_builder(&res_offsets);

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);

        result_offsets_builder.reserve(result_size_hint);

        if (result_size_hint < 1000000000 && src_elems.size() < 1000000000) /// Avoid overflow.
            res_elems.reserve((result_size_hint * src_elems.size() + size - 1) / size);
    }

    const UInt8 * filt_pos = filt.data();
    const auto * filt_end = filt_pos + size;

    const auto * offsets_pos = src_offsets.data();
    const auto * offsets_begin = offsets_pos;

    /// copy n arrays from ending at *end_offset_ptr
    const auto copy_chunk = [&](const IColumn::Offset * offset_ptr, size_t n) {
        const auto first = offset_ptr == offsets_begin;

        const auto chunk_offset = first ? 0 : offset_ptr[-1];
        const auto chunk_size = offset_ptr[n - 1] - chunk_offset;

        result_offsets_builder.insertChunk(n, offset_ptr, first, chunk_offset, chunk_size);

        /// copy elements for n arrays at once
        const auto elems_size_old = res_elems.size();
        res_elems.resize(elems_size_old + chunk_size);
        inline_memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(UInt8));
    };

    const auto * filt_end_aligned = filt_pos + size / FILTER_SIMD_BYTES * FILTER_SIMD_BYTES;
    while (filt_pos < filt_end_aligned)
    {
        auto mask = ToBits64(filt_pos);
        if likely (0 != mask)
        {
            if (const auto prefix_to_copy = prefixToCopy(mask); 0xFF != prefix_to_copy)
            {
                copy_chunk(offsets_pos, prefix_to_copy);
            }
            else
            {
                if (const auto suffix_to_copy = suffixToCopy(mask); 0xFF != suffix_to_copy)
                {
                    copy_chunk(offsets_pos + FILTER_SIMD_BYTES - suffix_to_copy, suffix_to_copy);
                }
                else
                {
                    while (mask)
                    {
                        size_t index = std::countr_zero(mask);
                        copy_chunk(offsets_pos + index, 1);
                        mask &= mask - 1;
                    }
                }
            }
        }

        filt_pos += FILTER_SIMD_BYTES;
        offsets_pos += FILTER_SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            copy_chunk(offsets_pos, 1);

        ++filt_pos;
        ++offsets_pos;
    }

    return res;
}

ColumnPtr filterCurrent(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    auto res = ColumnString::create();

    const auto & src = typeid_cast<const ColumnString &>(*col);
    const auto & src_elems = src.getChars();
    const auto & src_offsets = src.getOffsets();

    auto & res_elems = res->getChars();
    auto & res_offsets = res->getOffsets();

    filterArraysImpl<UInt8>(src_elems, src_offsets, res_elems, res_offsets, filt, result_size_hint);

    return res;
}

enum class FilterVersion
{
    AVX2,
    Current,
};

template <typename... Args>
void columnStringFilter(benchmark::State & state, Args &&... args)
{
    auto [version, n, set_percent] = std::make_tuple(std::move(args)...);
    auto mut_col = ColumnString::create();
    auto & src_chars = mut_col->getChars();
    auto & src_offsets = mut_col->getOffsets();
    src_chars.resize(n);
    src_offsets.resize(n);
    std::fill(src_offsets.begin(), src_offsets.end(), 'a');
    std::iota(src_offsets.begin(), src_offsets.end(), 0);
    auto set_n = n * set_percent;
    auto filter = createRandomFilter(n, set_n);

    ColumnPtr col = std::move(mut_col);

    if (version == FilterVersion::AVX2)
    {
        for (auto _ : state)
        {
            auto t = filterAVX2(col, filter, set_n);
            benchmark::DoNotOptimize(t);
        }
    }
    else
    {
        for (auto _ : state)
        {
            auto t = filterCurrent(col, filter, set_n);
            benchmark::DoNotOptimize(t);
        }
    }
}

BENCHMARK_CAPTURE(columnStringFilter, avx2_00, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.00);
BENCHMARK_CAPTURE(columnStringFilter, cur_00, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.00);
BENCHMARK_CAPTURE(columnStringFilter, avx2_01, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.01);
BENCHMARK_CAPTURE(columnStringFilter, cur_01, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.01);
BENCHMARK_CAPTURE(columnStringFilter, avx2_10, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.10);
BENCHMARK_CAPTURE(columnStringFilter, cur_10, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.10);
BENCHMARK_CAPTURE(columnStringFilter, avx2_20, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.20);
BENCHMARK_CAPTURE(columnStringFilter, cur_20, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.20);
BENCHMARK_CAPTURE(columnStringFilter, avx2_30, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.30);
BENCHMARK_CAPTURE(columnStringFilter, cur_30, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.30);
BENCHMARK_CAPTURE(columnStringFilter, avx2_40, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.40);
BENCHMARK_CAPTURE(columnStringFilter, cur_40, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.40);
BENCHMARK_CAPTURE(columnStringFilter, avx2_50, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.50);
BENCHMARK_CAPTURE(columnStringFilter, cur_50, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.50);
BENCHMARK_CAPTURE(columnStringFilter, avx2_60, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.60);
BENCHMARK_CAPTURE(columnStringFilter, cur_60, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.60);
BENCHMARK_CAPTURE(columnStringFilter, avx2_70, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.70);
BENCHMARK_CAPTURE(columnStringFilter, cur_70, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.70);
BENCHMARK_CAPTURE(columnStringFilter, avx2_80, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.80);
BENCHMARK_CAPTURE(columnStringFilter, cur_80, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.80);
BENCHMARK_CAPTURE(columnStringFilter, avx2_90, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.90);
BENCHMARK_CAPTURE(columnStringFilter, cur_90, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.90);
BENCHMARK_CAPTURE(columnStringFilter, avx2_99, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 0.99);
BENCHMARK_CAPTURE(columnStringFilter, cur_99, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 0.99);
BENCHMARK_CAPTURE(columnStringFilter, avx2_100, FilterVersion::AVX2, DEFAULT_BLOCK_SIZE, 1.00);
BENCHMARK_CAPTURE(columnStringFilter, cur_100, FilterVersion::Current, DEFAULT_BLOCK_SIZE, 1.00);

} // namespace bench::String
