// Copyright 2022 PingCAP, Ltd.
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

#include <stdint.h>
#include <cstddef>
#include <type_traits>
using std::size_t;

#include "bitops.h"

#if defined(_MSC_VER)
#include <intrin.h>
#endif


static inline uint32_t PopCountImpl(uint8_t n) {
    extern uint8_t const* PopCountLUT8;
    return PopCountLUT8[n];
}

static inline uint32_t PopCountImpl(uint16_t n) {
#if defined(_MSC_VER)
    return __popcnt16(n);
#else
    extern uint8_t const* PopCountLUT16;
    return PopCountLUT16[n];
#endif
}

static inline uint32_t PopCountImpl(uint32_t n) {
#if defined(_MSC_VER)
    return __popcnt(n);
#elif defined(__GNUC__) // it is true for Clang also
    return __builtin_popcount(n);
#else
    return PopCountImpl((uint16_t)LO_16(n)) + PopCountImpl((uint16_t)HI_16(n));
#endif
}

static inline uint32_t PopCountImpl(uint64_t n) {
#if defined(_MSC_VER) && !defined(_i386_)
    return __popcnt64(n);
#elif defined(__GNUC__) // it is true for Clang also
    return __builtin_popcountll(n);
#else
    return PopCountImpl((uint32_t)LO_32(n)) + PopCountImpl((uint32_t)HI_32(n));
#endif
}

template <class T>
static inline uint32_t PopCount(T n) {
    using TCvt = std::make_unsigned_t<std::decay_t<T>>;

    return PopCountImpl(static_cast<TCvt>(n));
}
