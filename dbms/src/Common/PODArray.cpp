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

#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
}

/// Used for left padding of PODArray when empty
const char EmptyPODArray[EmptyPODArraySize]{};

namespace PODArrayDetails
{

ALWAYS_INLINE size_t byte_size(size_t num_elements, size_t element_size)
{
    size_t amount;
    if (__builtin_mul_overflow(num_elements, element_size, &amount))
        throw Exception(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "Amount of memory requested to allocate is more than allowed");
    return amount;
}

ALWAYS_INLINE size_t
minimum_memory_for_elements(size_t num_elements, size_t element_size, size_t pad_left, size_t pad_right)
{
    size_t amount;
    if (__builtin_add_overflow(byte_size(num_elements, element_size), pad_left + pad_right, &amount))
        throw Exception(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "Amount of memory requested to allocate is more than allowed");
    return amount;
}

} // namespace PODArrayDetails
} // namespace DB
