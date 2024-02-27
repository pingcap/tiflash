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

#include <Common/PODArray.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>

#include <tuple>


namespace DB
{

/** Mark is the position in the compressed file. The compressed file consists of adjacent compressed blocks.
  * Mark is a tuple - the offset in the file to the start of the compressed block, the offset in the decompressed block to the start of the data.
  */
struct MarkInCompressedFile
{
    size_t offset_in_compressed_file = 0;
    size_t offset_in_decompressed_block = 0;

    bool operator==(const MarkInCompressedFile & rhs) const
    {
        return std::tie(offset_in_compressed_file, offset_in_decompressed_block)
            == std::tie(rhs.offset_in_compressed_file, rhs.offset_in_decompressed_block);
    }
    bool operator!=(const MarkInCompressedFile & rhs) const { return !(*this == rhs); }

    String toString() const
    {
        return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
    }
};

using MarksInCompressedFile = PODArray<MarkInCompressedFile>;
using MarksInCompressedFilePtr = std::shared_ptr<MarksInCompressedFile>;
} // namespace DB
