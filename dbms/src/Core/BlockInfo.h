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

#pragma once

#include <Core/Types.h>


namespace DB
{
class ReadBuffer;
class WriteBuffer;

/** More information about the block.
  */
struct BlockInfo
{
    /** bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */

#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(Int32, bucket_num, -1, 2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in);
};

} // namespace DB
