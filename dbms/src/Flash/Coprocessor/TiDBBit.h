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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <Common/TiFlashException.h>
#include <Core/Types.h>
#include <IO/Endian.h>
#include <common/StringRef.h>

namespace DB
{
class TiDBBit
{
public:
    TiDBBit(UInt64 value, Int8 byte_size)
    {
        if (byte_size != -1 && (byte_size < 1 || byte_size > 8))
            throw TiFlashException("Invalid byte size for bit encode", Errors::Coprocessor::Internal);

        raw_val = toBigEndian(value);
        const char * start = (const char *)&raw_val;
        int start_offset = 0;
        if (byte_size == -1)
        {
            for (; start_offset < 8; start_offset++)
            {
                if (start[start_offset] != 0)
                    break;
            }
            if (start_offset == 8)
            {
                start_offset = 7;
            }
        }
        else
        {
            start_offset = 8 - byte_size;
        }
        val = StringRef(start + start_offset, 8 - start_offset);
    }

    StringRef val;
    UInt64 raw_val;
};
} // namespace DB
