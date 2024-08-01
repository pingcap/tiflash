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

#include <Common/Exception.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/TiDBColumn.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <typename T>
void encodeLittleEndian(const T & value, WriteBuffer & ss)
{
    auto v = toLittleEndian(value);
    ss.template writeFixed<T>(&v);
}

TiDBColumn::TiDBColumn(Int8 element_len_)
    : length(0)
    , null_cnt(0)
    , current_data_size(0)
    , fixed_size(element_len_)
{
    data = std::make_unique<WriteBufferFromOwnString>();
    if (fixed_size != VAR_SIZE)
        default_value = String(fixed_size, '\0');
    var_offsets.push_back(0);
}

void TiDBColumn::clear()
{
    length = 0;
    null_cnt = 0;
    null_bitmap.clear();
    var_offsets.clear();
    var_offsets.push_back(0);
    data = std::make_unique<WriteBufferFromOwnString>();
    current_data_size = 0;
}

void TiDBColumn::appendNullBitMap(bool value)
{
    size_t index = length >> 3;
    if (index >= null_bitmap.size())
    {
        null_bitmap.push_back(0);
    }
    if (value)
    {
        size_t pos = length & 7;
        null_bitmap[index] |= (1 << pos);
    }
    else
    {
        null_cnt++;
    }
}

void TiDBColumn::finishAppendFixed()
{
    current_data_size += fixed_size;
    appendNullBitMap(true);
    length++;
}

void TiDBColumn::finishAppendVar(UInt32 size)
{
    current_data_size += size;
    appendNullBitMap(true);
    var_offsets.push_back(current_data_size);
    length++;
}

void TiDBColumn::appendNull()
{
    // todo try to decoupling the logic of appendNullBitMap and appendData
    appendNullBitMap(false);
    if (isFixed())
    {
        writeString(default_value, *data);
    }
    else
    {
        auto offset = var_offsets[length];
        var_offsets.push_back(offset);
    }
    length++;
}

void TiDBColumn::append(Int64 value)
{
    encodeLittleEndian<Int64>(value, *data);
    finishAppendFixed();
}

void TiDBColumn::append(const TiDBEnum & ti_enum)
{
    encodeLittleEndian<UInt64>(ti_enum.value, *data);
    UInt64 size = 8;
    data->write(ti_enum.name.data, ti_enum.name.size);
    size += ti_enum.name.size;
    finishAppendVar(size);
}

void TiDBColumn::appendVectorF32(UInt32 num_elem, StringRef elem_bytes)
{
    writeIntBinary(num_elem, *data);
    size_t encoded_size = sizeof(UInt32);

    RUNTIME_CHECK(elem_bytes.size == num_elem * sizeof(Float32));
    data->write(elem_bytes.data, elem_bytes.size);
    encoded_size += elem_bytes.size;

    RUNTIME_CHECK(encoded_size > 0);
    finishAppendVar(encoded_size);
}

void TiDBColumn::append(const TiDBBit & bit)
{
    data->write(bit.val.data, bit.val.size);
    finishAppendVar(bit.val.size);
}

void TiDBColumn::append(UInt64 value)
{
    encodeLittleEndian<UInt64>(value, *data);
    finishAppendFixed();
}

void TiDBColumn::append(const TiDBTime & time)
{
    encodeLittleEndian<UInt64>(time.toChunkTime(), *data);
    finishAppendFixed();
}

void TiDBColumn::append(const TiDBDecimal & decimal)
{
    encodeLittleEndian<UInt8>(decimal.digits_int, *data);
    encodeLittleEndian<UInt8>(decimal.digits_frac, *data);
    encodeLittleEndian<UInt8>(decimal.result_frac, *data);
    encodeLittleEndian<UInt8>(static_cast<UInt8>(decimal.negative), *data);
    for (int i : decimal.word_buf)
    {
        encodeLittleEndian<Int32>(i, *data);
    }
    finishAppendFixed();
}

void TiDBColumn::append(const StringRef & value)
{
    data->write(value.data, value.size);
    finishAppendVar(value.size);
}

void TiDBColumn::append(DB::Float32 value)
{
    // use memcpy to avoid breaking strict-aliasing rules
    UInt32 u;
    std::memcpy(&u, &value, sizeof(value));
    encodeLittleEndian<UInt32>(u, *data);
    finishAppendFixed();
}

void TiDBColumn::append(DB::Float64 value)
{
    // use memcpy to avoid breaking strict-aliasing rules
    UInt64 u;
    std::memcpy(&u, &value, sizeof(value));
    encodeLittleEndian<UInt64>(u, *data);
    finishAppendFixed();
}

void TiDBColumn::encodeColumn(WriteBuffer & ss)
{
    encodeLittleEndian<UInt32>(length, ss);
    encodeLittleEndian<UInt32>(null_cnt, ss);
    if (null_cnt > 0)
    {
        for (auto c : null_bitmap)
        {
            encodeLittleEndian<UInt8>(c, ss);
        }
    }
    if (!isFixed())
    {
        for (auto c : var_offsets)
        {
            encodeLittleEndian<Int64>(c, ss);
        }
    }
    writeString(data->str(), ss);
}

} // namespace DB
