// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnUtil.h>
#include <Columns/IColumn.h>
#include <IO/Buffer/ReadBuffer.h>

namespace DB
{

template <size_t FiledSize, typename T>
inline void deserializeBinaryBulkWithFilter(T & data, ReadBuffer & istr, size_t limit, const IColumn::Filter * filter)
{
    size_t current_size = data.size();
    data.resize(current_size + limit);

    if (!filter)
    {
        size_t size = istr.readBig(reinterpret_cast<char *>(&data[current_size]), FiledSize * limit);
        data.resize(current_size + size / FiledSize);
        return;
    }

    const UInt8 * filt_pos = filter->data();
    const UInt8 * filt_end = filt_pos + limit;
    const UInt8 * filt_end_aligned = filt_pos + limit / FILTER_SIMD_BYTES * FILTER_SIMD_BYTES;

    while (filt_pos < filt_end_aligned)
    {
        UInt64 mask = ToBits64(filt_pos);
        if likely (0 != mask)
        {
            if (const UInt8 prefix_to_copy = prefixToCopy(mask); 0xFF != prefix_to_copy)
            {
                size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), FiledSize * prefix_to_copy);
                current_size += size / FiledSize;
                istr.ignore(FiledSize * (FILTER_SIMD_BYTES - prefix_to_copy));
            }
            else
            {
                if (const UInt8 suffix_to_copy = suffixToCopy(mask); 0xFF != suffix_to_copy)
                {
                    istr.ignore(FiledSize * (FILTER_SIMD_BYTES - suffix_to_copy));
                    size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), FiledSize * suffix_to_copy);
                    current_size += size / FiledSize;
                }
                else
                {
                    size_t index = 0;
                    while (mask)
                    {
                        size_t m = std::countr_zero(mask);
                        istr.ignore(FiledSize * (m - index));
                        size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), FiledSize);
                        current_size += size / FiledSize;
                        index = m + 1;
                        mask &= mask - 1;
                    }
                    istr.ignore(FiledSize * (FILTER_SIMD_BYTES - index));
                }
            }
        }
        else
        {
            istr.ignore(FiledSize * FILTER_SIMD_BYTES);
        }

        filt_pos += FILTER_SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
        {
            size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), FiledSize);
            current_size += size / FiledSize;
        }
        else
        {
            istr.ignore(FiledSize);
        }

        ++filt_pos;
    }

    data.resize(current_size);
}

} // namespace DB
