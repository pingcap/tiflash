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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/TiDBBit.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Flash/Coprocessor/TiDBEnum.h>
#include <Flash/Coprocessor/TiDBTime.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
class TiDBColumn
{
public:
    explicit TiDBColumn(Int8 element_len);

    void appendNull();
    void append(Int64 value);
    void append(UInt64 value);
    void append(const StringRef & value);
    void append(Float64 value);
    void append(Float32 value);
    //void appendDuration();
    void append(const TiDBTime & time);
    //void appendJson();
    void append(const TiDBDecimal & decimal);
    void append(const TiDBBit & bit);
    void append(const TiDBEnum & ti_enum);
    void encodeColumn(WriteBuffer & ss);
    void clear();

    // FIXME: expose for ColumnArray::encodeIntoDatumData, find a better way to implement it.
    void finishAppendVar(UInt32 size);
    // WriteBufferFromOwnString is not moveable.
    std::unique_ptr<WriteBufferFromOwnString> data;

private:
    void finishAppendFixed();
    bool isFixed() const { return fixed_size != VAR_SIZE; }

    void appendNullBitMap(bool value);

    UInt32 length;
    UInt32 null_cnt;
    std::vector<UInt8> null_bitmap;
    std::vector<Int64> var_offsets;

    std::string default_value;
    UInt64 current_data_size;
    Int8 fixed_size;
};

} // namespace DB
