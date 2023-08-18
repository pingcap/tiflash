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
#include <common/defines.h>
#include <common/mem_utils.h>
#include <common/mem_utils_opt.h>

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>


namespace bench
{

constexpr size_t RESERVE_OFFSET = 200;
constexpr size_t TEST_ALIGN_SIZE = 64;
static_assert(RESERVE_OFFSET > TEST_ALIGN_SIZE * 2);
constexpr char DEFAULT_INIT_CHAR = '0';
constexpr char DEFAULT_TEST_CHAR = '1';
static constexpr size_t TEST_ALIGN_OFF_1 = 15;
static constexpr size_t TEST_ALIGN_OFF_2 = 31;

static_assert(TEST_ALIGN_SIZE > TEST_ALIGN_OFF_1);
static_assert(TEST_ALIGN_SIZE > TEST_ALIGN_OFF_2);

static constexpr bool varify_res = false;

template <size_t max_src_size>
class MemUtilsEqual : public benchmark::Fixture
{
protected:
    std::string inner_data1;
    std::string inner_data2;
    std::string_view data1;
    std::string_view data2;

public:
    static constexpr size_t max_size = max_src_size;

    void SetUp(const ::benchmark::State & /*state*/) override
    {
        inner_data1.resize(max_size + RESERVE_OFFSET, DEFAULT_INIT_CHAR);
        inner_data2 = inner_data1;

        {
            const auto * src = reinterpret_cast<const char *>(
                (size_t(inner_data1.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                + TEST_ALIGN_OFF_1); // start address not aligned
            size_t size = inner_data1.data() + inner_data1.size() - src;
            data1 = {src, size};
        }

        {
            const auto * src = reinterpret_cast<const char *>(
                (size_t(inner_data2.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                + TEST_ALIGN_OFF_2); // start address not aligned
            size_t size = inner_data2.data() + inner_data2.size() - src;
            data2 = {src, size};
        }
    }
};

template <size_t max_src_size>
class MemUtilsCmp : public benchmark::Fixture
{
protected:
    std::string inner_data1;
    std::string inner_data2;
    std::string_view data1;
    std::string_view data2;

public:
    static constexpr size_t max_size = max_src_size;

    void SetUp(const ::benchmark::State & /*state*/) override
    {
        inner_data1.resize(max_size + RESERVE_OFFSET, DEFAULT_INIT_CHAR);
        inner_data2 = inner_data1;

        {
            const auto * src = reinterpret_cast<const char *>(
                (size_t(inner_data1.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                + TEST_ALIGN_OFF_1); // start address not aligned
            data1 = {src, max_size};
        }

        {
            auto * src = reinterpret_cast<char *>(
                (size_t(inner_data2.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                + TEST_ALIGN_OFF_2); // start address not aligned
            src[max_size - 1] = DEFAULT_TEST_CHAR;
            data2 = {src, max_size};
        }
    }
};

template <size_t max_cnt, size_t max_src_size, size_t max_needle_size>
class MemUtilsStrStr : public benchmark::Fixture
{
protected:
    std::vector<std::string> inner_data1;
    std::vector<std::string> inner_data2;

    std::vector<std::string_view> data1;
    std::vector<std::string_view> data2;

public:
    static constexpr int check_char = -1;

    void SetUp(const ::benchmark::State & /*state*/) override
    {
        inner_data1.resize(max_cnt);
        inner_data2.resize(max_cnt);
        data1.resize(max_cnt);
        data2.resize(max_cnt);

        for (size_t i = 0; i < max_cnt; ++i)
        {
            {
                auto & inner_data = inner_data1[i];
                inner_data.resize(max_src_size + RESERVE_OFFSET, DEFAULT_INIT_CHAR);
                auto * src = reinterpret_cast<char *>(
                    (size_t(inner_data.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                    + TEST_ALIGN_OFF_1); // start address not aligned
                size_t size = max_src_size;
                data1[i] = {src, size};

                char * bg = src + size - max_needle_size;
                if (max_needle_size > 1 && size / 2 + 1 != size - max_needle_size)
                    src[size / 2] = check_char; //set one char
                memset(bg, check_char, max_needle_size);
            }
            {
                auto & inner_data = inner_data2[i];
                inner_data.resize(max_needle_size + RESERVE_OFFSET, check_char);
                auto * src = reinterpret_cast<char *>(
                    (size_t(inner_data.data()) + TEST_ALIGN_SIZE - 1) / TEST_ALIGN_SIZE * TEST_ALIGN_SIZE
                    + TEST_ALIGN_OFF_2); // start address not aligned
                size_t size = max_needle_size;
                data2[i] = {src, size};
            }
        }
    }
};


ALWAYS_INLINE static inline bool stl_mem_eq(const char * p1, const char * p2, size_t n)
{
    return std::memcmp(p1, p2, n) == 0; // call bcmp@plt
}
ALWAYS_INLINE static inline int stl_mem_cmp(const char * p1, const char * p2, size_t n)
{
    return std::memcmp(p1, p2, n); // call memcmp@plt
}

NO_INLINE size_t stl_str_find(std::string_view s, std::string_view p)
{
    return s.find(p); // call memchr@plt -> bcmp@plt
}

// volatile value is used to prevent compiler optimization for fixed context

#define BENCH_MEM_EQ(name1, name2, func, loop_cnt, iter_cnt)                                 \
    BENCHMARK_DEFINE_F(name1, name2)                                                         \
    (benchmark::State & state)                                                               \
    {                                                                                        \
        [[maybe_unused]] volatile size_t _volatile_flags = 1;                                \
        [[maybe_unused]] volatile size_t cnt = max_size;                                     \
        for (auto _ : state)                                                                 \
        {                                                                                    \
            for (size_t i = 0; i < (loop_cnt); ++i)                                          \
            {                                                                                \
                _volatile_flags = func(data1.data(), data2.data(), cnt > i ? cnt - i : cnt); \
                if constexpr (varify_res)                                                    \
                {                                                                            \
                    if (unlikely(!_volatile_flags))                                          \
                        exit(-1);                                                            \
                }                                                                            \
            }                                                                                \
        }                                                                                    \
    }                                                                                        \
    BENCHMARK_REGISTER_F(name1, name2)->Iterations(iter_cnt);

#define BENCH_MEM_CMP(name1, name2, func, loop_cnt, iter_cnt)                        \
    BENCHMARK_DEFINE_F(name1, name2)                                                 \
    (benchmark::State & state)                                                       \
    {                                                                                \
        [[maybe_unused]] volatile int _volatile_flags = 1;                           \
        [[maybe_unused]] volatile size_t cnt = max_size;                             \
        for (auto _ : state)                                                         \
        {                                                                            \
            for (size_t i = 0; i < (loop_cnt); ++i)                                  \
            {                                                                        \
                size_t ori = cnt;                                                    \
                size_t n = ori > i ? ori - i : ori;                                  \
                size_t diff = ori - n;                                               \
                _volatile_flags = func(data1.data() + diff, data2.data() + diff, n); \
                if constexpr (varify_res)                                            \
                {                                                                    \
                    if (unlikely(!(_volatile_flags < 0)))                            \
                    {                                                                \
                        exit(-1);                                                    \
                    }                                                                \
                }                                                                    \
            }                                                                        \
        }                                                                            \
    }                                                                                \
    BENCHMARK_REGISTER_F(name1, name2)->Iterations(iter_cnt);

#define BENCH_MEM_STRSTR(name1, name2, func, iter_cnt)                        \
    BENCHMARK_DEFINE_F(name1, name2)                                          \
    (benchmark::State & state)                                                \
    {                                                                         \
        [[maybe_unused]] volatile size_t _volatile_flags = 0;                 \
        for (auto _ : state)                                                  \
        {                                                                     \
            for (size_t i = 0; i < data1.size(); ++i)                         \
            {                                                                 \
                _volatile_flags = func(data1[i], data2[i]);                   \
                if constexpr (varify_res)                                     \
                {                                                             \
                    if (_volatile_flags != data1[i].size() - data2[i].size()) \
                    {                                                         \
                        exit(-1);                                             \
                    }                                                         \
                }                                                             \
            }                                                                 \
        }                                                                     \
    }                                                                         \
    BENCHMARK_REGISTER_F(name1, name2)->Iterations(iter_cnt);


#define BENCH_MEM_EQ_ALL_IMPL(id, max_src_size, loop_cnt, iter_cnt)                            \
    using id = MemUtilsEqual<max_src_size>;                                                    \
    BENCH_MEM_EQ(id, stl_mem_eq, stl_mem_eq, loop_cnt, iter_cnt)                               \
    BENCH_MEM_EQ(id, mem_utils_memoryEqual_avx512, mem_utils::memoryEqual, loop_cnt, iter_cnt) \
    BENCH_MEM_EQ(id, avx2_mem_equal, mem_utils::avx2_mem_equal, loop_cnt, iter_cnt)

#define BENCH_MEM_EQ_IMPL_ID(max_src_size, loop_cnt, iter_cnt) MemUtilsEqual##_##max_src_size##_##loop_cnt

#define BENCH_MEM_EQ_ALL(max_src_size, loop_cnt, iter_cnt) \
    BENCH_MEM_EQ_ALL_IMPL(BENCH_MEM_EQ_IMPL_ID(max_src_size, loop_cnt, iter_cnt), max_src_size, loop_cnt, iter_cnt)

#define BENCH_MEM_CMP_ALL_IMPL(id, max_src_size, loop_cnt, iter_cnt) \
    using id = MemUtilsCmp<max_src_size>;                            \
    BENCH_MEM_CMP(id, stl_mem_cmp, stl_mem_cmp, loop_cnt, iter_cnt)  \
    BENCH_MEM_CMP(id, avx2_mem_cmp, mem_utils::avx2_mem_cmp, loop_cnt, iter_cnt)

#define BENCH_MEM_CMP_IMPL_ID(max_src_size, loop_cnt, iter_cnt) MemUtilsCmp##_##max_src_size##_##loop_cnt

#define BENCH_MEM_CMP_ALL(max_src_size, loop_cnt, iter_cnt) \
    BENCH_MEM_CMP_ALL_IMPL(BENCH_MEM_CMP_IMPL_ID(max_src_size, loop_cnt, iter_cnt), max_src_size, loop_cnt, iter_cnt)

#define BENCH_MEM_STRSTR_ALL(max_cnt, max_src_size, max_needle_size, iter_cnt)                                  \
    using MemUtilsStrStr##_##max_src_size##_##max_needle_size                                                   \
        = MemUtilsStrStr<max_cnt, max_src_size, max_needle_size>;                                               \
    BENCH_MEM_STRSTR(MemUtilsStrStr##_##max_src_size##_##max_needle_size, stl_str_find, stl_str_find, iter_cnt) \
    BENCH_MEM_STRSTR(MemUtilsStrStr##_##max_src_size##_##max_needle_size, avx2_strstr, mem_utils::avx2_strstr, iter_cnt)

#define BENCH_MEM_EQ_LOOP 20

BENCH_MEM_EQ_ALL(13, BENCH_MEM_EQ_LOOP, 2000)
BENCH_MEM_EQ_ALL(65, BENCH_MEM_EQ_LOOP, 2000)
BENCH_MEM_EQ_ALL(100, BENCH_MEM_EQ_LOOP, 500)
BENCH_MEM_EQ_ALL(10000, BENCH_MEM_EQ_LOOP, 500)
BENCH_MEM_EQ_ALL(100000, BENCH_MEM_EQ_LOOP, 500)
BENCH_MEM_EQ_ALL(1000000, BENCH_MEM_EQ_LOOP, 200)

#define BENCH_MEM_CMP_LOOP 20

BENCH_MEM_CMP_ALL(2, BENCH_MEM_CMP_LOOP, 2000)
BENCH_MEM_CMP_ALL(13, BENCH_MEM_CMP_LOOP, 2000)
BENCH_MEM_CMP_ALL(65, BENCH_MEM_CMP_LOOP, 2000)
BENCH_MEM_CMP_ALL(100, BENCH_MEM_CMP_LOOP, 500)
BENCH_MEM_CMP_ALL(10000, BENCH_MEM_CMP_LOOP, 500)
BENCH_MEM_CMP_ALL(100000, BENCH_MEM_CMP_LOOP, 500)
BENCH_MEM_CMP_ALL(1000000, BENCH_MEM_CMP_LOOP, 200)

BENCH_MEM_STRSTR_ALL(512, 1024, 1, 100);
BENCH_MEM_STRSTR_ALL(512, 1024, 7, 100);
BENCH_MEM_STRSTR_ALL(512, 1024, 15, 100);
BENCH_MEM_STRSTR_ALL(512, 1024, 31, 100);
BENCH_MEM_STRSTR_ALL(512, 1024, 63, 100);

BENCH_MEM_STRSTR_ALL(512, 80, 1, 100);
BENCH_MEM_STRSTR_ALL(512, 80, 7, 100);
BENCH_MEM_STRSTR_ALL(512, 80, 15, 100);
BENCH_MEM_STRSTR_ALL(512, 80, 31, 100);

} // namespace bench
