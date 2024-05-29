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

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB::DM
{

struct MergedSubFileInfo
{
    String fname; // Sub filemame
    UInt64 number = 0; // Merged file number
    UInt64 offset = 0; // Offset in merged file
    UInt64 size = 0; // Size of sub file

    MergedSubFileInfo() = default;

    MergedSubFileInfo(const String & fname_, UInt64 number_, UInt64 offset_, UInt64 size_)
        : fname(fname_)
        , number(number_)
        , offset(offset_)
        , size(size_)
    {}

    void serializeToBuffer(WriteBuffer & buf) const
    {
        writeStringBinary(fname, buf);
        writeIntBinary(number, buf);
        writeIntBinary(offset, buf);
        writeIntBinary(size, buf);
    }

    static MergedSubFileInfo parseFromBuffer(ReadBuffer & buf)
    {
        MergedSubFileInfo info;
        readStringBinary(info.fname, buf);
        readIntBinary(info.number, buf);
        readIntBinary(info.offset, buf);
        readIntBinary(info.size, buf);
        return info;
    }
};
} // namespace DB::DM
