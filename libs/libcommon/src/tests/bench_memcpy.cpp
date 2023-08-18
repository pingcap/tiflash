// Copyright 2023 PingCAP, Inc.
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

#include <benchmark/benchmark.h>
#include <common/memcpy.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "../../libmemcpy/folly/FollyMemcpy.h"
#include "../../libmemcpy/memcpy.h"

namespace bench
{

void * _internal_avx2_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    return mem_utils::avx2_inline_memcpy(dst_, src_, size);
}
void * _internal_sse2_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    return sse2_inline_memcpy(dst_, src_, size);
}

template <size_t min, size_t max, size_t align, bool hot, size_t loop_cnt_>
class MemUtilsCopy : public benchmark::Fixture
{
protected:
    static const size_t loop_cnt = loop_cnt_;

    std::string dst_buffer;
    std::string src_buffer;
    std::vector<size_t> sizes;
    std::vector<size_t> dst_offsets;
    std::vector<size_t> src_offsets;

    void SetUp(const ::benchmark::State & /*state*/) override
    {
        size_t src_buffer_size
            = (sysconf(_SC_PAGE_SIZE) * std::ceil(static_cast<double>(max + 2 * align) / sysconf(_SC_PAGE_SIZE)));
        size_t dst_buffer_size;
        if (hot)
        {
            dst_buffer_size = src_buffer_size;
        }
        else
        {
            dst_buffer_size = 1024 * 1024 * 1024; // 1 GiB
        }
        dst_buffer.resize(dst_buffer_size);
        memset(dst_buffer.data(), 'd', dst_buffer.size());
        src_buffer.resize(src_buffer_size);
        memset(src_buffer.data(), 's', src_buffer.size());

        std::default_random_engine gen;
        sizes.resize(4095);
        std::uniform_int_distribution<size_t> size_dist(min, max);
        for (auto & size : sizes)
        {
            size = size_dist(gen);
        }

        src_offsets.resize(4096);
        dst_offsets.resize(4096);
        std::uniform_int_distribution<size_t> src_offset_dist(0, (src_buffer_size - max) / align);
        std::uniform_int_distribution<size_t> dst_offset_dist(0, (dst_buffer_size - max) / align);
        for (size_t i = 0; i < src_offsets.size(); i++)
        {
            src_offsets[i] = align * src_offset_dist(gen);
            dst_offsets[i] = align * dst_offset_dist(gen);
        }
    }
};

#define BENCH_MEM_COPY(id, name, fn_memcpy, iters)                       \
    BENCHMARK_DEFINE_F(id, name)                                         \
    (benchmark::State & state)                                           \
    {                                                                    \
        for (auto _ : state)                                             \
        {                                                                \
            size_t size_idx = 0;                                         \
            size_t offset_idx = 0;                                       \
            for (unsigned int i = 0; i < loop_cnt; i++)                  \
            {                                                            \
                if (size_idx + 1 == sizes.size())                        \
                    size_idx = 0;                                        \
                if (offset_idx >= src_offsets.size())                    \
                    offset_idx = 0;                                      \
                void * dst = &dst_buffer[dst_offsets[offset_idx]];       \
                const void * src = &src_buffer[src_offsets[offset_idx]]; \
                volatile size_t size = sizes[size_idx];                  \
                fn_memcpy(dst, src, size);                               \
                size_idx++;                                              \
                offset_idx++;                                            \
            }                                                            \
        }                                                                \
    }                                                                    \
    BENCHMARK_REGISTER_F(id, name)->Iterations(iters);

#define BENCH_MEM_COPY_ALL_IMPL(id, min, max, align, hot, loop_cnt, iters)     \
    using id = MemUtilsCopy<min, max, align, hot, loop_cnt>;                   \
    BENCH_MEM_COPY(id, stl_mempy, std::memcpy, iters)                          \
    BENCH_MEM_COPY(id, inline_clickhouse_memcpy, legacy::inline_memcpy, iters) \
    BENCH_MEM_COPY(id, sse2_memcpy, sse2_inline_memcpy, iters)                 \
    BENCH_MEM_COPY(id, avx2_memcpy, mem_utils::avx2_inline_memcpy, iters)      \
    BENCH_MEM_COPY(id, folly_memcpy, __folly_memcpy, iters)

#define BENCH_MEM_IMPL_ID(min, max, align, hot, loop_cnt) MemUtilsCopy##_##min##_##max##_##align##_##hot##_##loop_cnt

#define BENCH_MEM_COPY_ALL(min, max, align, hot, loop_cnt, iters) \
    BENCH_MEM_COPY_ALL_IMPL(BENCH_MEM_IMPL_ID(min, max, align, hot, loop_cnt), min, max, align, hot, loop_cnt, iters)

BENCH_MEM_COPY_ALL(1, 20, 3, true, 20000, 500);
BENCH_MEM_COPY_ALL(1, 40, 3, true, 20000, 500);
BENCH_MEM_COPY_ALL(1, 80, 3, true, 20000, 500);
BENCH_MEM_COPY_ALL(1, 200, 3, true, 20000, 500);
BENCH_MEM_COPY_ALL(1, 2000, 3, true, 20000, 500);


} // namespace bench