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

#pragma once

#include <Common/PODArray.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#include <immintrin.h>
#endif

namespace DB
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT

static constexpr size_t VECTOR_SIZE_AVX2 = sizeof(__m256i);
static constexpr size_t FULL_VECTOR_SIZE_AVX2 = 2 * VECTOR_SIZE_AVX2;
static_assert(FULL_VECTOR_SIZE_AVX2 == 64);

/// NT stand for non-temporal.
union alignas(FULL_VECTOR_SIZE_AVX2) NTAlignBufferAVX2
{
    char data[FULL_VECTOR_SIZE_AVX2]{};
    __m256i v[2];
};

/// Each time the buffer is full, the data will be flushed.
/// The maximum size is 63 so last byte can be used for saving size.
class ColumnNTAlignBufferAVX2
{
public:
    static_assert(UINT8_MAX >= FULL_VECTOR_SIZE_AVX2);
    ColumnNTAlignBufferAVX2() { buffer.data[FULL_VECTOR_SIZE_AVX2 - 1] = 0; }

    NTAlignBufferAVX2 & getBuffer() { return buffer; }

    inline UInt8 getSize() { return static_cast<UInt8>(buffer.data[FULL_VECTOR_SIZE_AVX2 - 1]); }
    inline void setSize(UInt8 size)
    {
        RUNTIME_ASSERT(size < FULL_VECTOR_SIZE_AVX2);
        buffer.data[FULL_VECTOR_SIZE_AVX2 - 1] = size;
    }

private:
    NTAlignBufferAVX2 buffer;
};

ALWAYS_INLINE inline void nonTemporalStore64B(void * dst, NTAlignBufferAVX2 & buffer)
{
    assert(reinterpret_cast<std::uintptr_t>(dst) % VECTOR_SIZE_AVX2 == 0);
    _mm256_stream_si256(reinterpret_cast<__m256i *>(dst), buffer.v[0]);
    _mm256_stream_si256(reinterpret_cast<__m256i *>(static_cast<char *>(dst) + VECTOR_SIZE_AVX2), buffer.v[1]);
}

#else

static constexpr size_t VECTOR_SIZE_AVX2 = 32;
static constexpr size_t FULL_VECTOR_SIZE_AVX2 = 2 * VECTOR_SIZE_AVX2;
class ColumnNTAlignBufferAVX2
{
};

#endif
} // namespace DB
