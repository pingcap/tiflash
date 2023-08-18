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

/// What to do if the limit is exceeded.
enum class OverflowMode
{
    THROW = 0, /// Throw exception.
    BREAK = 1, /// Abort query execution, return what is.
};


struct SizeLimits
{
    /// If it is zero, corresponding limit check isn't performed.
    UInt64 max_rows = 0;
    UInt64 max_bytes = 0;
    OverflowMode overflow_mode = OverflowMode::THROW;

    SizeLimits() {}
    SizeLimits(UInt64 max_rows, UInt64 max_bytes, OverflowMode overflow_mode)
        : max_rows(max_rows)
        , max_bytes(max_bytes)
        , overflow_mode(overflow_mode)
    {}

    /// Check limits. If exceeded, return false or throw an exception, depending on overflow_mode.
    bool check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const;
};

} // namespace DB
