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

#include <Core/Types.h>
#include <common/DateLUT.h>
#include <common/DateLUTImpl.h>

namespace DB
{
const static Int32 MAX_WORD_BUF_LEN = 9;
const static Int32 DIGITS_PER_WORD = 9;
const static Int32 POWERS10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

class TiDBDecimal
{
public:
    TiDBDecimal(UInt32 scale, const std::vector<Int32> & digits, bool neg);

    Int8 digits_int;
    Int8 digits_frac;
    Int8 result_frac;
    bool negative;
    Int32 word_buf[MAX_WORD_BUF_LEN] = {0};
};
} // namespace DB
