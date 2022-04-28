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

#include <DataStreams/SizeLimits.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <string>


namespace DB
{

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const
{
    if (max_rows && rows > max_rows)
    {
        if (overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max rows: " + formatReadableQuantity(max_rows)
                + ", current rows: " + formatReadableQuantity(rows), exception_code);
        else
            return false;
    }

    if (max_bytes && bytes > max_bytes)
    {
        if (overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max bytes: " + formatReadableSizeWithBinarySuffix(max_bytes)
                + ", current bytes: " + formatReadableSizeWithBinarySuffix(bytes), exception_code);
        else
            return false;
    }

    return true;
}

}
