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

#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>

#define FAIL(msg)         \
    {                     \
        std::cout << msg; \
        exit(1);          \
    }


CityHash_v1_0_2::uint128 referenceHash(const char * data, size_t len)
{
    const size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE;
    CityHash_v1_0_2::uint128 state(0, 0);
    size_t pos;

    for (pos = 0; pos + block_size <= len; pos += block_size)
    {
        state = CityHash_v1_0_2::CityHash128WithSeed(data + pos, block_size, state);
    }

    if (pos < len)
        state = CityHash_v1_0_2::CityHash128WithSeed(data + pos, len - pos, state);

    return state;
}
