// Copyright 2025 PingCAP, Inc.
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

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/CommonUtil.h>


namespace DB::DM::InvertedIndex
{

template <typename T>
void Block<T>::serialize(WriteBuffer & write_buf) const
{
    writeIntBinary(static_cast<UInt32>(entries.size()), write_buf);
    // write all values first
    for (const auto & entry : entries)
    {
        writeIntBinary(entry.value, write_buf);
        writeIntBinary(static_cast<UInt32>(entry.row_ids.size()), write_buf);
    }
    // write all row_ids
    for (const auto & entry : entries)
    {
        const auto & row_ids = entry.row_ids;
        write_buf.write(reinterpret_cast<const char *>(row_ids.data()), row_ids.size() * sizeof(RowID));
    }
}

template <typename T>
void Block<T>::deserialize(Block<T> & block, ReadBuffer & read_buf)
{
    UInt32 size;
    readIntBinary(size, read_buf);
    block.entries.resize(size);
    for (UInt32 i = 0; i < size; ++i)
    {
        T value;
        readIntBinary(value, read_buf);
        UInt32 row_ids_size;
        readIntBinary(row_ids_size, read_buf);
        block.entries[i].value = value;
        block.entries[i].row_ids.resize(row_ids_size);
    }
    for (UInt32 i = 0; i < size; ++i)
    {
        auto & entry = block.entries[i];
        read_buf.readStrict(reinterpret_cast<char *>(entry.row_ids.data()), entry.row_ids.size() * sizeof(RowID));
    }
}

template <typename T>
void Block<T>::search(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, T key)
{
    UInt32 size;
    readIntBinary(size, read_buf);
    UInt32 seek_offset = size * (sizeof(T) + sizeof(UInt32));
    for (UInt32 i = 0; i < size; ++i)
    {
        T value;
        readIntBinary(value, read_buf);
        UInt32 row_ids_size;
        readIntBinary(row_ids_size, read_buf);
        seek_offset -= (sizeof(T) + sizeof(UInt32));
        if (value == key)
        {
            // ignore the rest values and previous row_ids
            read_buf.ignore(seek_offset);
            RowIDs row_ids(row_ids_size);
            read_buf.readStrict(reinterpret_cast<char *>(row_ids.data()), row_ids_size * sizeof(RowID));
            bitmap_filter->set(row_ids, nullptr);
            return;
        }
        seek_offset += row_ids_size * sizeof(RowID);
    }
}

template <typename T>
void Block<T>::searchRange(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, T begin, T end)
{
    UInt32 read_count = read_buf.count();
    UInt32 size;
    readIntBinary(size, read_buf);
    UInt32 acc_row_ids_size = 0;
    UInt32 start_offset = 0;
    UInt32 end_offset = 0;
    for (UInt32 i = 0; i < size; ++i)
    {
        T value;
        readIntBinary(value, read_buf);
        UInt32 row_ids_size;
        readIntBinary(row_ids_size, read_buf);
        if (value >= begin && value <= end && start_offset == 0)
            start_offset = sizeof(UInt32) + size * (sizeof(T) + sizeof(UInt32)) + acc_row_ids_size * sizeof(RowID);
        acc_row_ids_size += row_ids_size;
        if (value >= begin && value <= end)
            end_offset = sizeof(UInt32) + size * (sizeof(T) + sizeof(UInt32)) + acc_row_ids_size * sizeof(RowID);
        if (value > end)
            break;
    }

    if (start_offset == 0)
        return;

    read_count = read_buf.count() - read_count;
    read_buf.ignore(start_offset - read_count);
    RowIDs row_ids((end_offset - start_offset) / sizeof(RowID));
    read_buf.readStrict(reinterpret_cast<char *>(row_ids.data()), row_ids.size() * sizeof(RowID));
    bitmap_filter->set(row_ids, nullptr);
}

template <typename T>
void MetaEntry<T>::serialize(WriteBuffer & write_buf) const
{
    writeIntBinary(offset, write_buf);
    writeIntBinary(size, write_buf);
    writeIntBinary(min, write_buf);
    writeIntBinary(max, write_buf);
}

template <typename T>
void MetaEntry<T>::deserialize(MetaEntry<T> & entry, ReadBuffer & read_buf)
{
    readIntBinary(entry.offset, read_buf);
    readIntBinary(entry.size, read_buf);
    readIntBinary(entry.min, read_buf);
    readIntBinary(entry.max, read_buf);
}

template <typename T>
void Meta<T>::serialize(WriteBuffer & write_buf) const
{
    writeIntBinary(static_cast<UInt8>(sizeof(T)), write_buf);
    writeIntBinary(static_cast<UInt32>(entries.size()), write_buf);
    for (const auto & entry : entries)
        entry.serialize(write_buf);
}

template <typename T>
void Meta<T>::deserialize(Meta<T> & meta, ReadBuffer & read_buf)
{
    UInt8 type_size;
    readIntBinary(type_size, read_buf);
    RUNTIME_CHECK(type_size == sizeof(T));

    UInt32 size;
    readIntBinary(size, read_buf);
    meta.entries.resize(size);
    for (auto & entry : meta.entries)
        MetaEntry<T>::deserialize(entry, read_buf);
}

template struct Block<UInt8>;
template struct Block<UInt16>;
template struct Block<UInt32>;
template struct Block<UInt64>;
template struct Block<Int8>;
template struct Block<Int16>;
template struct Block<Int32>;
template struct Block<Int64>;
template struct MetaEntry<UInt8>;
template struct MetaEntry<UInt16>;
template struct MetaEntry<UInt32>;
template struct MetaEntry<UInt64>;
template struct MetaEntry<Int8>;
template struct MetaEntry<Int16>;
template struct MetaEntry<Int32>;
template struct MetaEntry<Int64>;
template struct Meta<UInt8>;
template struct Meta<UInt16>;
template struct Meta<UInt32>;
template struct Meta<UInt64>;
template struct Meta<Int8>;
template struct Meta<Int16>;
template struct Meta<Int32>;
template struct Meta<Int64>;

} // namespace DB::DM::InvertedIndex
