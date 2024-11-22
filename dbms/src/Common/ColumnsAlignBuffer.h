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

union alignas(FULL_VECTOR_SIZE_AVX2) AlignBufferAVX2
{
    char data[FULL_VECTOR_SIZE_AVX2]{};
    __m256i v[2];
};

class ColumnsAlignBufferAVX2
{
public:
    ColumnsAlignBufferAVX2() = default;

    void resize(size_t n)
    {
        buffers.resize(n, FULL_VECTOR_SIZE_AVX2);
        sizes.resize_fill_zero(n, FULL_VECTOR_SIZE_AVX2);
    }

    void resetIndex(bool need_flush_)
    {
        current_index = 0;
        need_flush = need_flush_;
    }

    size_t nextIndex()
    {
        if unlikely (current_index >= buffers.size())
            resize(current_index + 1);
        return current_index++;
    }

    AlignBufferAVX2 & getAlignBuffer(size_t index)
    {
        assert(index < buffers.size());
        return buffers[index];
    }

    UInt8 & getSize(size_t index)
    {
        assert(index < sizes.size());
        return sizes[index];
    }

    bool needFlush() const { return need_flush; }

private:
    size_t current_index = 0;
    bool need_flush = false;
    PaddedPODArray<AlignBufferAVX2> buffers;
    static_assert(UINT8_MAX >= FULL_VECTOR_SIZE_AVX2);
    PaddedPODArray<UInt8> sizes;
};

#else
class ColumnsAlignBufferAVX2
{
public:
    void resize(size_t /*n*/) {}
    void resetIndex(bool /*need_flush*/) {}
};
#endif
} // namespace DB
