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

#include <cstdint>
#include <cstddef>

/*
 * Author: Konstantin Oblakov
 *
 * Maps random ui64 x (in fact hash of some string) to n baskets/shards.
 * Output value is id of a basket. 0 <= ConsistentHashing(x, n) < n.
 * Probability of all baskets must be equal. Also, it should be consistent
 * in terms, that with different n_1 < n_2 probability of
 * ConsistentHashing(x, n_1) != ConsistentHashing(x, n_2) must be equal to
 * (n_2 - n_1) / n_2 - the least possible with previous conditions.
 * It requires O(1) memory and cpu to calculate. So, it is faster than classic
 * consistent hashing algos with points on circle.
 */
std::size_t ConsistentHashing(std::uint64_t x, std::size_t n); // Works good for n < 65536
std::size_t ConsistentHashing(std::uint64_t lo, std::uint64_t hi, std::size_t n); // Works good for n < 4294967296
