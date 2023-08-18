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

#if __SSE2__
#include <common/mem_utils.h>

#include <cassert>
#include <cstdint>
namespace mem_utils::_detail
{
using VectorType = __m128i;
template <size_t N>
__attribute__((always_inline, pure)) inline bool compareArraySSE2(const VectorType (&data)[N], VectorType filled_vector)
{
    static_assert(N >= 1 && N <= 4, "compare array can only be used within range");

    VectorType compared [[maybe_unused]][N - 1]{};

    if constexpr (N >= 4)
        compared[2] = _mm_cmpeq_epi8(filled_vector, data[3]);
    if constexpr (N >= 3)
        compared[1] = _mm_cmpeq_epi8(filled_vector, data[2]);
    if constexpr (N >= 2)
        compared[0] = _mm_cmpeq_epi8(filled_vector, data[1]);

    auto combined = _mm_cmpeq_epi8(filled_vector, data[0]);

    if constexpr (N >= 4)
        combined = _mm_and_si128(combined, compared[2]);
    if constexpr (N >= 3)
        combined = _mm_and_si128(combined, compared[1]);
    if constexpr (N >= 2)
        combined = _mm_and_si128(combined, compared[0]);

    auto mask = _mm_movemask_epi8(combined);
    return mask == 0xFFFF;
}

// The function checks whether the memory is filled with target byte with SIMD acceleration,
// assuming that the size is always larger than the vector length.
// There are several three stages of the whole procedure:
//
//    header                         tail
//    |____|                        |____|
//      |____|____|____|....|____|____|
//
// - at the beginning, one vector is read and compared at first;
// - then, the main loop starts at the aligned memory boundary (may intersect with header and tail);
//   checking 4 * vector_length for each iteration;
// - finally, one more vector right at the ending position is compared;
//
// These three tiles cover the hole memory area.
__attribute__((pure)) bool memoryIsByteSSE2(const void * data, size_t size, std::byte target)
{
    static constexpr size_t vector_length = sizeof(VectorType);
    static constexpr size_t group_size = vector_length * 4;
    size_t remaining = size;
    auto filled_vector = _mm_set1_epi8(static_cast<char>(target));
    const auto * current_address = reinterpret_cast<const VectorType *>(data);
    const auto * byte_address = reinterpret_cast<const uint8_t *>(data);

    if (!compareArraySSE2<1>({_mm_loadu_si128(current_address)}, filled_vector))
    {
        return false;
    }

    auto numeric_address = reinterpret_cast<uintptr_t>(data);
    auto alignment_offset = (-numeric_address) & (vector_length - 1);
    current_address = reinterpret_cast<const VectorType *>(byte_address + alignment_offset);
    remaining -= alignment_offset;

    while (remaining >= group_size)
    {
        if (compareArraySSE2<4>(
                {
                    _mm_load_si128(current_address + 0),
                    _mm_load_si128(current_address + 1),
                    _mm_load_si128(current_address + 2),
                    _mm_load_si128(current_address + 3),
                },
                filled_vector))
        {
            remaining -= group_size;
            current_address += 4;
        }
        else
        {
            return false;
        }
    }

    auto tail = _mm_loadu_si128(reinterpret_cast<const VectorType *>(byte_address + size - vector_length));
    assert(remaining / vector_length <= 3);
    bool result = true;
    switch (remaining / vector_length)
    {
    case 3:
        result = compareArraySSE2<4>(
            {_mm_load_si128(current_address + 0),
             _mm_load_si128(current_address + 1),
             _mm_load_si128(current_address + 2),
             tail},
            filled_vector);
        break;
    case 2:
        result = compareArraySSE2<3>(
            {_mm_load_si128(current_address + 0), _mm_load_si128(current_address + 1), tail},
            filled_vector);
        break;
    case 1:
        result = compareArraySSE2<2>({_mm_load_si128(current_address + 0), tail}, filled_vector);
        break;
    case 0:
        result = compareArraySSE2<1>({tail}, filled_vector);
        break;
    }
    return result;
}
} // namespace mem_utils::_detail
#endif