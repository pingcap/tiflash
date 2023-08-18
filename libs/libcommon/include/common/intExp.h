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

#include <cstdint>
#include <limits>


/// On overlow, the function returns unspecified value.

inline uint64_t intExp2(int x)
{
    return 1ULL << x;
}

inline uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    static const uint64_t table[20]
        = {1ULL,
           10ULL,
           100ULL,
           1000ULL,
           10000ULL,
           100000ULL,
           1000000ULL,
           10000000ULL,
           100000000ULL,
           1000000000ULL,
           10000000000ULL,
           100000000000ULL,
           1000000000000ULL,
           10000000000000ULL,
           100000000000000ULL,
           1000000000000000ULL,
           10000000000000000ULL,
           100000000000000000ULL,
           1000000000000000000ULL,
           10000000000000000000ULL};

    return table[x];
}
