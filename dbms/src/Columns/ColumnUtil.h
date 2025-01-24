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

#include <common/types.h>

namespace DB
{

UInt64 ToBits64(const UInt8 * bytes64);

constexpr size_t FILTER_SIMD_BYTES = 64;

/// If mask is a number of this kind: [0]*[1]+ function returns the length of the cluster of 1s.
/// Otherwise it returns the special value: 0xFF.
/// Note: mask must be non-zero.
UInt8 prefixToCopy(UInt64 mask);

UInt8 suffixToCopy(UInt64 mask);

} // namespace DB
