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

#include "StringUtils.h"

#include <cctype>

namespace detail
{
bool startsWith(const char * s, size_t size, const char * prefix, size_t prefix_size)
{
    return size >= prefix_size && 0 == memcmp(s, prefix, prefix_size);
}

bool endsWith(const char * s, size_t size, const char * suffix, size_t suffix_size)
{
    return size >= suffix_size && 0 == memcmp(s + size - suffix_size, suffix, suffix_size);
}

bool startsWithCI(const char * s, size_t size, const char * prefix, size_t prefix_size)
{
    if (size < prefix_size)
        return false;
    // case insensitive compare
    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (std::tolower(s[i]) != std::tolower(prefix[i]))
            return false;
    }
    return true;
}

bool endsWithCI(const char * s, size_t size, const char * suffix, size_t suffix_size)
{
    if (size < suffix_size)
        return false;
    // case insensitive compare
    for (size_t i = 0; i < suffix_size; ++i)
    {
        if (std::tolower(s[i]) != std::tolower(suffix[i]))
            return false;
    }
    return true;
}

} // namespace detail
