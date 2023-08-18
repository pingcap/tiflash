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

#include <common/types.h>

namespace DB
{
struct CapacityLimits
{
    Int64 max_size;
    Int64 max_bytes;
    CapacityLimits(Int64 max_size_, Int64 max_bytes_ = std::numeric_limits<Int64>::max())
        : max_size(std::max(1, max_size_))
        , max_bytes(max_bytes_ <= 0 ? std::numeric_limits<Int64>::max() : max_bytes_)
    {}
};
} // namespace DB
