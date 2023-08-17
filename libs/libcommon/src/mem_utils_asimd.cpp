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

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
#include <arm_neon.h>
#include <common/mem_utils.h>

#include <cassert>
#include <cstdint>
namespace mem_utils::_detail
{
namespace
{
__attribute__((always_inline, pure)) inline bool checkU64(uint64x2_t value)
{
    auto result = value[0] & value[1];
    return result == 0xFFFF'FFFF'FFFF'FFFFu;
}

__attribute__((always_inline, pure)) inline uint64x2_t compareConvert(uint8x16_t a, uint8x16_t b)
{
    return vreinterpretq_u64_u8(vceqq_u8(a, b));
}


// the following part follow the same logic of AVX2/AVX512, check comments of AVX2/AVX512 implementation
// to get detailed explanation
__attribute__((always_inline, pure)) inline bool memoryEqualASIMDx1(const char * p1, const char * p2)
{
    auto p1_ = reinterpret_cast<const uint8_t *>(p1);
    auto p2_ = reinterpret_cast<const uint8_t *>(p2);
    return checkU64(compareConvert(vld1q_u8(p1_), vld1q_u8(p2_)));
}

__attribute__((always_inline, pure)) inline bool memoryEqualASIMDx8(const char * p1, const char * p2)
{
    auto p1_ = reinterpret_cast<const uint8_t *>(p1);
    auto p2_ = reinterpret_cast<const uint8_t *>(p2);
    auto vec_length = sizeof(uint8x16_t);
    uint8x16_t lhs[]
        = {vld1q_u8(p1_ + vec_length * 0),
           vld1q_u8(p1_ + vec_length * 1),
           vld1q_u8(p1_ + vec_length * 2),
           vld1q_u8(p1_ + vec_length * 3),
           vld1q_u8(p1_ + vec_length * 4),
           vld1q_u8(p1_ + vec_length * 5),
           vld1q_u8(p1_ + vec_length * 6),
           vld1q_u8(p1_ + vec_length * 7)};
    uint8x16_t rhs[]
        = {vld1q_u8(p2_ + vec_length * 0),
           vld1q_u8(p2_ + vec_length * 1),
           vld1q_u8(p2_ + vec_length * 2),
           vld1q_u8(p2_ + vec_length * 3),
           vld1q_u8(p2_ + vec_length * 4),
           vld1q_u8(p2_ + vec_length * 5),
           vld1q_u8(p2_ + vec_length * 6),
           vld1q_u8(p2_ + vec_length * 7)};
    uint64x2_t compared[] = {
        compareConvert(lhs[0], rhs[0]),
        compareConvert(lhs[1], rhs[1]),
        compareConvert(lhs[2], rhs[2]),
        compareConvert(lhs[3], rhs[3]),
        compareConvert(lhs[4], rhs[4]),
        compareConvert(lhs[5], rhs[5]),
        compareConvert(lhs[6], rhs[6]),
        compareConvert(lhs[7], rhs[7]),
    };
    uint64x2_t combined1[] = {
        vandq_u64(compared[0], compared[1]),
        vandq_u64(compared[2], compared[3]),
        vandq_u64(compared[4], compared[5]),
        vandq_u64(compared[6], compared[7]),
    };
    uint64x2_t combined2[] = {
        vandq_u64(combined1[0], combined1[1]),
        vandq_u64(combined1[2], combined1[3]),
    };
    uint64x2_t combined3 = vandq_u64(combined2[0], combined2[1]);
    return checkU64(combined3);
}

__attribute__((always_inline, pure)) inline bool memoryEqualASIMDx4(const char * p1, const char * p2)
{
    auto p1_ = reinterpret_cast<const uint8_t *>(p1);
    auto p2_ = reinterpret_cast<const uint8_t *>(p2);
    auto vec_length = sizeof(uint8x16_t);
    uint8x16_t lhs[]
        = {vld1q_u8(p1_ + vec_length * 0),
           vld1q_u8(p1_ + vec_length * 1),
           vld1q_u8(p1_ + vec_length * 2),
           vld1q_u8(p1_ + vec_length * 3)};
    uint8x16_t rhs[]
        = {vld1q_u8(p2_ + vec_length * 0),
           vld1q_u8(p2_ + vec_length * 1),
           vld1q_u8(p2_ + vec_length * 2),
           vld1q_u8(p2_ + vec_length * 3)};
    uint64x2_t compared[]
        = {compareConvert(lhs[0], rhs[0]),
           compareConvert(lhs[1], rhs[1]),
           compareConvert(lhs[2], rhs[2]),
           compareConvert(lhs[3], rhs[3])};
    uint64x2_t combined1[] = {vandq_u64(compared[0], compared[1]), vandq_u64(compared[2], compared[3])};
    uint64x2_t combined2 = vandq_u64(combined1[0], combined1[1]);
    return checkU64(combined2);
}
} // namespace

__attribute__((pure)) bool memoryEqualASIMD(const char * p1, const char * p2, size_t size)
{
    // AARCH64's movemask is complicated, so it is worthwhile to enable to loop unit
    // By experiments, it reduces the time further by 1/6
    while (size >= 128)
    {
        // prefetch memory improves throughput significantly on AARCH64 platform
        __builtin_prefetch(p1 + 128);
        __builtin_prefetch(p2 + 128);
        if (memoryEqualASIMDx8(p1, p2))
        {
            p1 += 128;
            p2 += 128;
            size -= 128;
        }
        else
            return false;
    }

    while (size >= 64)
    {
        // prefetch memory improves throughput significantly on AARCH64 platform
        __builtin_prefetch(p1 + 64);
        __builtin_prefetch(p2 + 64);
        if (memoryEqualASIMDx4(p1, p2))
        {
            p1 += 64;
            p2 += 64;
            size -= 64;
        }
        else
            return false;
    }

    switch ((size % 64) / 16)
    {
    case 3:
        if (!memoryEqualASIMDx1(p1 + 32, p2 + 32))
            return false;
        [[fallthrough]];
    case 2:
        if (!memoryEqualASIMDx1(p1 + 16, p2 + 16))
            return false;
        [[fallthrough]];
    case 1:
        if (!memoryEqualASIMDx1(p1, p2))
            return false;
        [[fallthrough]];
    case 0:
        break;
    }

    p1 += (size % 64) / 16 * 16;
    p2 += (size % 64) / 16 * 16;

    switch (size % 16)
    {
    case 15:
        if (p1[14] != p2[14])
            return false;
        [[fallthrough]];
    case 14:
        if (p1[13] != p2[13])
            return false;
        [[fallthrough]];
    case 13:
        if (p1[12] != p2[12])
            return false;
        [[fallthrough]];
    case 12:
        if (unalignedLoad<uint32_t>(p1 + 8) == unalignedLoad<uint32_t>(p2 + 8))
            goto l8;
        else
            return false;
    case 11:
        if (p1[10] != p2[10])
            return false;
        [[fallthrough]];
    case 10:
        if (p1[9] != p2[9])
            return false;
        [[fallthrough]];
    case 9:
        if (p1[8] != p2[8])
            return false;
    l8:
        [[fallthrough]];
    case 8:
        return unalignedLoad<uint64_t>(p1) == unalignedLoad<uint64_t>(p2);
    case 7:
        if (p1[6] != p2[6])
            return false;
        [[fallthrough]];
    case 6:
        if (p1[5] != p2[5])
            return false;
        [[fallthrough]];
    case 5:
        if (p1[4] != p2[4])
            return false;
        [[fallthrough]];
    case 4:
        return unalignedLoad<uint32_t>(p1) == unalignedLoad<uint32_t>(p2);
    case 3:
        if (p1[2] != p2[2])
            return false;
        [[fallthrough]];
    case 2:
        return unalignedLoad<uint16_t>(p1) == unalignedLoad<uint16_t>(p2);
    case 1:
        if (p1[0] != p2[0])
            return false;
        [[fallthrough]];
    case 0:
        break;
    }

    return true;
}

template <size_t N>
__attribute__((always_inline, pure)) inline bool compareArrayASIMD(
    const uint8x16_t (&data)[N],
    uint8x16_t filled_vector)
{
    static_assert(N >= 1 && N <= 4, "compare array can only be used within range");

    uint8x16_t compared [[maybe_unused]][N - 1]{};

    if constexpr (N >= 4)
        compared[2] = vceqq_u8(filled_vector, data[3]);
    if constexpr (N >= 3)
        compared[1] = vceqq_u8(filled_vector, data[2]);
    if constexpr (N >= 2)
        compared[0] = vceqq_u8(filled_vector, data[1]);

    auto combined = vceqq_u8(filled_vector, data[0]);

    if constexpr (N >= 4)
        combined = vandq_u8(combined, compared[2]);
    if constexpr (N >= 3)
        combined = vandq_u8(combined, compared[1]);
    if constexpr (N >= 2)
        combined = vandq_u8(combined, compared[0]);

    auto mask = vreinterpretq_u64_u8(combined);
    return (mask[0] & mask[1]) == 0xFFFF'FFFF'FFFF'FFFF;
}

// even though ASIMD instruction does not distinguish aligned or unaligned loading
// it is a good choice to keep it aligned here.
// see: https://stackoverflow.com/questions/45714535/performance-of-unaligned-simd-load-store-on-aarch64
__attribute__((pure)) bool memoryIsByteASIMD(const void * data, const size_t size, std::byte target)
{
    static constexpr size_t vector_length = sizeof(uint8x16_t);
    static constexpr size_t group_size = vector_length * 4;
    size_t remaining = size;
    auto filled_vector = vdupq_n_u8(static_cast<uint8_t>(target));
    auto current_address = reinterpret_cast<const uint8_t *>(data);

    if (!compareArrayASIMD<1>({vld1q_u8(current_address)}, filled_vector))
    {
        return false;
    }

    auto numeric_address = reinterpret_cast<uintptr_t>(data);
    auto alignment_offset = (-numeric_address) & (vector_length - 1);
    current_address = reinterpret_cast<const uint8_t *>(data) + alignment_offset;
    remaining -= alignment_offset;

    while (remaining >= group_size)
    {
        __builtin_prefetch(current_address + group_size);
        if (compareArrayASIMD(
                {
                    vld1q_u8(current_address + 0 * vector_length),
                    vld1q_u8(current_address + 1 * vector_length),
                    vld1q_u8(current_address + 2 * vector_length),
                    vld1q_u8(current_address + 3 * vector_length),
                },
                filled_vector))
        {
            remaining -= group_size;
            current_address += group_size;
        }
        else
        {
            return false;
        }
    }

    auto tail = vld1q_u8(reinterpret_cast<const uint8_t *>(data) + size - vector_length);
    assert(remaining / vector_length <= 3);
    bool result = true;
    switch (remaining / vector_length)
    {
    case 3:
        result = compareArrayASIMD<4>(
            {vld1q_u8(current_address + 0 * vector_length),
             vld1q_u8(current_address + 1 * vector_length),
             vld1q_u8(current_address + 2 * vector_length),
             tail},
            filled_vector);
        break;
    case 2:
        result = compareArrayASIMD<3>(
            {vld1q_u8(current_address + 0 * vector_length), vld1q_u8(current_address + 1 * vector_length), tail},
            filled_vector);
        break;
    case 1:
        result = compareArrayASIMD<2>({vld1q_u8(current_address + 0 * vector_length), tail}, filled_vector);
        break;
    case 0:
        result = compareArrayASIMD<1>({tail}, filled_vector);
        break;
    }
    return result;
}

} // namespace mem_utils::_detail

#endif