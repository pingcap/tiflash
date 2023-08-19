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

#pragma once
#include <cstddef>
#include <cstdint>
namespace crc64
{
enum class Mode
{
    Table,
    Auto,
    SIMD_128,
    SIMD_256, // always define this, but no effect if VPCLMULQDQ support is unavailable
    SIMD_512, // always define this, but no effect if VPCLMULQDQ support is unavailable
};
class Digest
{
public:
    explicit Digest(Mode mode = Mode::Auto);

    void update(const void * src, size_t length) { state = update_fn(state, src, length); }

    [[nodiscard]] uint64_t checksum() const { return ~state; }

private:
    uint64_t (*update_fn)(uint64_t, const void *, size_t) = nullptr;

    uint64_t state = ~0ull;
};
} // namespace crc64
