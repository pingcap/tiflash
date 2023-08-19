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

#include <Columns/ColumnString.h>

namespace DB
{
namespace StringUtil
{
/// Same as ColumnString's private offsetAt and sizeAt.
inline size_t offsetAt(const ColumnString::Offsets & offsets, size_t i)
{
    return i == 0 ? 0 : offsets[i - 1];
}

inline size_t sizeAt(const ColumnString::Offsets & offsets, size_t i)
{
    return i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1]);
}
} // namespace StringUtil
} // namespace DB
