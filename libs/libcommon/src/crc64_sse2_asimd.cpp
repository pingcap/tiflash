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

#include <common/crc64_fast.h>
#include <common/crc64_table.h>

#ifdef TIFLASH_CRC64_HAS_SIMD_SUPPORT

#include <common/crc64.h>
#if __SSE2__
#include <common/crc64_arch/crc64_x86.h>
#else
#include <common/crc64_arch/crc64_aarch64.h>
#endif

namespace crc64::_detail
{
template <size_t N>
using Slice = std::array<SIMD, N>;

uint64_t update_simd(uint64_t state, const void * src, size_t length)
{
    if (length == 0)
        return state;

    const auto * ptr = reinterpret_cast<const SIMD *>(__builtin_assume_aligned(src, 128));

    auto load_slice = [](const SIMD * address) -> Slice<8> {
        return {
            SIMD::aligned(address),
            SIMD::aligned(address + 1),
            SIMD::aligned(address + 2),
            SIMD::aligned(address + 3),
            SIMD::aligned(address + 4),
            SIMD::aligned(address + 5),
            SIMD::aligned(address + 6),
            SIMD::aligned(address + 7),
        };
    };

    auto x = load_slice(ptr);
    ptr += 8;
    x[0] ^= SIMD{0, state};

    auto coeff = SIMD{K_1023, K_1087};

    for (size_t i = 128; i < length; i += 128, ptr += 8)
    {
        auto chunk = load_slice(ptr);
        for (size_t j = 0; j < 8; ++j)
        {
            x[j] = chunk[j] ^ x[j].fold16(coeff);
        }
    }

    Slice<7> coeffs = {
        SIMD{K_895, K_959}, // fold by distance of 112 bytes
        SIMD{K_767, K_831}, // fold by distance of 96 bytes
        SIMD{K_639, K_703}, // fold by distance of 80 bytes
        SIMD{K_511, K_575}, // fold by distance of 64 bytes
        SIMD{K_383, K_447}, // fold by distance of 48 bytes
        SIMD{K_255, K_319}, // fold by distance of 32 bytes
        SIMD{K_127, K_191} // fold by distance of 16 bytes
    };

    auto acc = x[7];
    for (size_t i = 0; i < 7; ++i)
    {
        acc ^= x[i].fold16(coeffs[i]);
    }

    return acc.fold8(K_127).barrett(POLY, MU);
}
} // namespace crc64::_detail
#endif // TIFLASH_CRC64_HAS_SIMD_SUPPORT