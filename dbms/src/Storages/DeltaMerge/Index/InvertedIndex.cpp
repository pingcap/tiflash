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

#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Index/InvertedIndex.h>

#include <ext/scope_guard.h>


namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
} // namespace DB::ErrorCodes


namespace DB::DM
{

namespace InvertedIndex
{

static auto constexpr MagicFlag = "INVE";
static UInt32 constexpr MagicFlagLength = 4; // strlen(MagicFlag)

template <typename T>
void serializeBlock(const Block<T> & meta, WriteBuffer & write_buf)
{
    writeIntBinary(static_cast<UInt32>(meta.size()), write_buf);
    // write all values first
    for (const auto & entry : meta)
    {
        writeIntBinary(entry.value, write_buf);
        writeIntBinary(static_cast<UInt32>(entry.row_ids.size()), write_buf);
    }
    // write all row_ids
    for (const auto & entry : meta)
    {
        auto row_ids = entry.row_ids;
        write_buf.write(reinterpret_cast<const char *>(row_ids.data()), row_ids.size() * sizeof(RowID));
    }
}

template <typename T>
Block<T> deserializeBlock(ReadBuffer & read_buf)
{
    UInt32 size;
    readIntBinary(size, read_buf);
    Block<T> block(size);
    for (UInt32 i = 0; i < size; ++i)
    {
        T value;
        readIntBinary(value, read_buf);
        UInt32 row_ids_size;
        readIntBinary(row_ids_size, read_buf);
        block[i].value = value;
        block[i].row_ids.resize(row_ids_size);
    }
    for (UInt32 i = 0; i < size; ++i)
    {
        auto & entry = block[i];
        read_buf.read(reinterpret_cast<char *>(entry.row_ids.data()), entry.row_ids.size() * sizeof(RowID));
    }
    return block;
}

template <typename T>
void blockSearch(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, const T key)
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
void blockSearchRange(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, const T begin, const T end)
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

// Get the size of the block in bytes. But it is not accurate, because the size of the row_ids is VarUInt.
template <typename T>
constexpr size_t getBlockSize(UInt32 entry_size, UInt32 row_ids_size)
{
    return sizeof(UInt32) + entry_size * (sizeof(T) + sizeof(UInt32)) + row_ids_size * sizeof(RowID);
}

template <typename T>
void serializeMetaEntry(const MetaEntry<T> & entry, WriteBuffer & write_buf)
{
    writeIntBinary(entry.offset, write_buf);
    writeIntBinary(entry.size, write_buf);
    writeIntBinary(entry.min, write_buf);
    writeIntBinary(entry.max, write_buf);
}

template <typename T>
void deserializeMetaEntry(MetaEntry<T> & entry, ReadBuffer & read_buf)
{
    readIntBinary(entry.offset, read_buf);
    readIntBinary(entry.size, read_buf);
    readIntBinary(entry.min, read_buf);
    readIntBinary(entry.max, read_buf);
}

template <typename T>
void serializeMeta(const Meta<T> & meta, WriteBuffer & write_buf)
{
    writeIntBinary(static_cast<UInt8>(sizeof(T)), write_buf);
    writeIntBinary(static_cast<UInt32>(meta.size()), write_buf);
    for (const auto & entry : meta)
        serializeMetaEntry(entry, write_buf);
}

template <typename T>
void deserializeMeta(Meta<T> & meta, ReadBuffer & read_buf)
{
    UInt8 type_size;
    readIntBinary(type_size, read_buf);
    RUNTIME_CHECK(type_size == sizeof(T));

    UInt32 size;
    readIntBinary(size, read_buf);
    meta.resize(size);
    for (auto & entry : meta)
        deserializeMetaEntry(entry, read_buf);
}

} // namespace InvertedIndex

LocalIndexBuilderPtr createInvertedIndexBuilder(const LocalIndexInfo & index_info)
{
    if (index_info.kind != TiDB::ColumnarIndexKind::Inverted || !index_info.def_inverted_index)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid index kind or definition");

    const auto & definition = index_info.def_inverted_index;
    if (definition->type_size == sizeof(UInt8) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<UInt8>>(index_info);
    }
    else if (definition->type_size == sizeof(Int8) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<Int8>>(index_info);
    }
    else if (definition->type_size == sizeof(UInt16) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<UInt16>>(index_info);
    }
    else if (definition->type_size == sizeof(Int16) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<Int16>>(index_info);
    }
    else if (definition->type_size == sizeof(UInt32) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<UInt32>>(index_info);
    }
    else if (definition->type_size == sizeof(Int32) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<Int32>>(index_info);
    }
    else if (definition->type_size == sizeof(UInt64) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<UInt64>>(index_info);
    }
    else if (definition->type_size == sizeof(Int64) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexBuilder<Int64>>(index_info);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type size {}", definition->type_size);
    }
}

TiDB::InvertedIndexDefinitionPtr tryGetInvertedIndexDefinition(
    const TiDB::ColumnInfo & col_info,
    const IDataType & type)
{
    const auto * nullable = checkAndGetDataType<DataTypeNullable>(&type);
    const auto * real_type = nullable ? nullable->getNestedType().get() : &type;
    bool is_integer = real_type->isValueRepresentedByInteger() && !real_type->isDecimal();
    if (!is_integer)
        return nullptr;

    bool is_unsigned
        = (col_info.tp == 7 /* MyDateTime */ || col_info.tp == 10 /* MyDate */
           || col_info.tp == 12 /* MyDateTime */);
    is_unsigned = is_unsigned || col_info.hasUnsignedFlag();
    return std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
        .is_signed = !is_unsigned,
        .type_size = static_cast<UInt8>(real_type->getSizeOfValueInMemory()),
    });
}

template <typename T>
void InvertedIndexBuilder<T>::addBlock(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    ProceedCheckFn should_proceed)
{
    // Note: column may be nullable.
    const bool is_nullable = column.isColumnNullable();
    const auto * col_vector
        = is_nullable ? checkAndGetNestedColumn<ColumnVector<T>>(&column) : checkAndGetColumn<ColumnVector<T>>(&column);
    RUNTIME_CHECK_MSG(col_vector, "ColumnVector is expected, get: {}, T: {}", column.getName(), typeid(T).name());
    const auto & col_data = col_vector->getData();

    const auto * null_map = is_nullable ? &(checkAndGetColumn<ColumnNullable>(&column)->getNullMapData()) : nullptr;
    const auto * del_mark_data = del_mark ? &(del_mark->getData()) : nullptr;

    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    Stopwatch w_proceed_check(CLOCK_MONOTONIC_COARSE);

    for (size_t i = 0; i < col_data.size(); ++i)
    {
        auto row_offset = added_rows;
        ++added_rows;

        if (unlikely(i % 100 == 0 && w_proceed_check.elapsedSeconds() > 0.5))
        {
            // The check of should_proceed could be non-trivial, so do it not too often.
            w_proceed_check.restart();
            if (!should_proceed())
                throw Exception(ErrorCodes::ABORTED, "Index build is interrupted");
        }

        // Ignore rows with del_mark, as the column values are not meaningful.
        if (del_mark_data != nullptr && (*del_mark_data)[i])
            continue;

        // Ignore NULL values, as they are not meaningful to store in index.
        if (null_map && (*null_map)[i])
            continue;

        index[col_data[i]].push_back(row_offset);
    }
}

template <typename T>
void InvertedIndexBuilder<T>::saveToFile(std::string_view path) const
{
    WriteBufferFromFile write_buf(path.data());
    saveToBuffer(write_buf);
    write_buf.next();
    write_buf.sync();
}

template <typename T>
void InvertedIndexBuilder<T>::saveToBuffer(WriteBuffer & write_buf) const
{
    InvertedIndex::Meta<T> meta;

    // 1. write data by block
    size_t offset = 0;

    InvertedIndex::Block<T> block;
    size_t row_ids_size = 0;
    auto write_block = [&] {
        InvertedIndex::serializeBlock(block, write_buf);
        size_t total_size = write_buf.count();
        meta.emplace_back(offset, total_size - offset, block.front().value, block.back().value);
        block.clear();
        offset = total_size;
        row_ids_size = 0;
    };

    for (const auto & [key, row_ids] : index)
    {
        block.emplace_back(key, row_ids);
        row_ids_size += row_ids.size();

        // write block
        if (InvertedIndex::getBlockSize<T>(block.size(), row_ids_size) >= InvertedIndex::BlockSize)
            write_block();
    }
    if (!block.empty())
        write_block();

    // 2. write meta
    offset = write_buf.count();
    InvertedIndex::serializeMeta(meta, write_buf);

    // 3. write meta size
    UInt32 meta_size = write_buf.count() - offset;
    write_buf.write(reinterpret_cast<const char *>(&meta_size), sizeof(meta_size));

    // 4. write magic flag
    write_buf.write(InvertedIndex::MagicFlag, InvertedIndex::MagicFlagLength);
}

InvertedIndexViewerPtr InvertedIndexViewer::view(const DataTypePtr & type, std::string_view path)
{
    auto type_id = type->isNullable() ? dynamic_cast<const DataTypeNullable &>(*type).getNestedType()->getTypeId()
                                      : type->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return std::make_shared<InvertedIndexFileViewer<UInt8>>(path);
    case TypeIndex::Int8:
        return std::make_shared<InvertedIndexFileViewer<Int8>>(path);
    case TypeIndex::UInt16:
        return std::make_shared<InvertedIndexFileViewer<UInt16>>(path);
    case TypeIndex::Int16:
        return std::make_shared<InvertedIndexFileViewer<Int16>>(path);
    case TypeIndex::UInt32:
        return std::make_shared<InvertedIndexFileViewer<UInt32>>(path);
    case TypeIndex::Int32:
        return std::make_shared<InvertedIndexFileViewer<Int32>>(path);
    case TypeIndex::UInt64:
        return std::make_shared<InvertedIndexFileViewer<UInt64>>(path);
    case TypeIndex::Int64:
        return std::make_shared<InvertedIndexFileViewer<Int64>>(path);
    case TypeIndex::Date:
        return std::make_shared<InvertedIndexFileViewer<UInt16>>(path);
    case TypeIndex::DateTime:
        return std::make_shared<InvertedIndexFileViewer<UInt32>>(path);
    case TypeIndex::Enum8:
        return std::make_shared<InvertedIndexFileViewer<Int8>>(path);
    case TypeIndex::Enum16:
        return std::make_shared<InvertedIndexFileViewer<Int16>>(path);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return std::make_shared<InvertedIndexFileViewer<UInt64>>(path);
    case TypeIndex::MyTime:
        return std::make_shared<InvertedIndexFileViewer<Int64>>(path);
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type_id: {}", magic_enum::enum_name(type_id));
    }
}

InvertedIndexViewerPtr InvertedIndexViewer::view(const DataTypePtr & type, ReadBuffer & buf, size_t index_size)
{
    auto type_id = type->isNullable() ? dynamic_cast<const DataTypeNullable &>(*type).getNestedType()->getTypeId()
                                      : type->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return std::make_shared<InvertedIndexMemoryViewer<UInt8>>(buf, index_size);
    case TypeIndex::Int8:
        return std::make_shared<InvertedIndexMemoryViewer<Int8>>(buf, index_size);
    case TypeIndex::UInt16:
        return std::make_shared<InvertedIndexMemoryViewer<UInt16>>(buf, index_size);
    case TypeIndex::Int16:
        return std::make_shared<InvertedIndexMemoryViewer<Int16>>(buf, index_size);
    case TypeIndex::UInt32:
        return std::make_shared<InvertedIndexMemoryViewer<UInt32>>(buf, index_size);
    case TypeIndex::Int32:
        return std::make_shared<InvertedIndexMemoryViewer<Int32>>(buf, index_size);
    case TypeIndex::UInt64:
        return std::make_shared<InvertedIndexMemoryViewer<UInt64>>(buf, index_size);
    case TypeIndex::Int64:
        return std::make_shared<InvertedIndexMemoryViewer<Int64>>(buf, index_size);
    case TypeIndex::Date:
        return std::make_shared<InvertedIndexMemoryViewer<UInt16>>(buf, index_size);
    case TypeIndex::DateTime:
        return std::make_shared<InvertedIndexMemoryViewer<UInt32>>(buf, index_size);
    case TypeIndex::Enum8:
        return std::make_shared<InvertedIndexMemoryViewer<Int8>>(buf, index_size);
    case TypeIndex::Enum16:
        return std::make_shared<InvertedIndexMemoryViewer<Int16>>(buf, index_size);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return std::make_shared<InvertedIndexMemoryViewer<UInt64>>(buf, index_size);
    case TypeIndex::MyTime:
        return std::make_shared<InvertedIndexMemoryViewer<Int64>>(buf, index_size);
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type_id: {}", magic_enum::enum_name(type_id));
    }
}

template <typename T>
void InvertedIndexMemoryViewer<T>::load(ReadBuffer & read_buf, size_t index_size)
{
    // 1. read all data
    std::vector<char> buf(index_size);
    RUNTIME_CHECK(read_buf.readBig(buf.data(), index_size) == index_size);

    // 2. check magic flag
    size_t data_size = index_size - InvertedIndex::MagicFlagLength;
    if (memcmp(buf.data() + data_size, InvertedIndex::MagicFlag, InvertedIndex::MagicFlagLength) != 0)
        throw Exception(ErrorCodes::ABORTED, "Invalid magic flag");

    // 3. read meta size
    data_size = data_size - sizeof(UInt32);
    UInt32 meta_size = *reinterpret_cast<const UInt32 *>(buf.data() + data_size);

    // 4. read meta
    ReadBufferFromMemory buffer(buf.data() + data_size - meta_size, meta_size);
    InvertedIndex::Meta<T> meta;
    data_size = data_size - meta_size;
    InvertedIndex::deserializeMeta(meta, buffer);

    // 5. read blocks & build index
    buffer = ReadBufferFromMemory(buf.data(), data_size);
    for (const auto meta_entry : meta)
    {
        auto count = buffer.count();
        auto block = InvertedIndex::deserializeBlock<T>(buffer);
        RUNTIME_CHECK(buffer.count() - count == meta_entry.size);
        for (const auto & block_entry : block)
        {
            auto [value, row_ids] = block_entry;
            index[value] = row_ids;
        }
    }
}

template <typename T>
void InvertedIndexMemoryViewer<T>::search(BitmapFilterPtr & bitmap_filter, const Key & key) const
{
    T real_key = key;
    auto it = index.find(real_key);
    if (it != index.end())
        bitmap_filter->set(it->second, nullptr);
}

template <typename T>
void InvertedIndexMemoryViewer<T>::searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end)
    const
{
    T real_begin = begin;
    T real_end = end;
    auto index_begin = index.lower_bound(real_begin);
    auto index_end = index.upper_bound(real_end);
    for (auto it = index_begin; it != index_end; ++it)
        bitmap_filter->set(it->second, nullptr);
}

template <typename T>
void InvertedIndexFileViewer<T>::loadMeta(ReadBuffer & read_buf, size_t index_size)
{
    // 1. read all data
    std::vector<char> buf(index_size);
    RUNTIME_CHECK(read_buf.readBig(buf.data(), index_size) == index_size);

    // 2. check magic flag
    size_t data_size = index_size - InvertedIndex::MagicFlagLength;
    if (memcmp(buf.data() + data_size, InvertedIndex::MagicFlag, InvertedIndex::MagicFlagLength) != 0)
        throw Exception(ErrorCodes::ABORTED, "Invalid magic flag");

    // 3. read meta size
    data_size = data_size - sizeof(UInt32);
    UInt32 meta_size = *reinterpret_cast<const UInt32 *>(buf.data() + data_size);

    // 4. read meta
    data_size = data_size - meta_size;
    ReadBufferFromMemory buffer(buf.data() + data_size, meta_size);
    InvertedIndex::deserializeMeta(meta, buffer);
}

template <typename T>
void InvertedIndexFileViewer<T>::search(BitmapFilterPtr & bitmap_filter, const Key & key) const
{
    T real_key = key;
    auto it = std::find_if(meta.begin(), meta.end(), [&](const auto & entry) {
        return entry.min <= real_key && entry.max >= real_key;
    });
    if (it == meta.end())
        return;

    ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    file_buf.seek(it->offset, SEEK_SET);
    InvertedIndex::blockSearch(bitmap_filter, file_buf, real_key);
}

template <typename T>
void InvertedIndexFileViewer<T>::searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end) const
{
    T real_begin = begin;
    T real_end = end;
    // max < begin
    auto meta_begin = std::lower_bound(meta.begin(), meta.end(), real_begin, [](const auto & entry, const auto & key) {
        return entry.max < key;
    });
    // min > end
    auto meta_end = std::upper_bound(meta_begin, meta.end(), real_end, [](const auto & key, const auto & entry) {
        return key < entry.min;
    });

    ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    for (auto it = meta_begin; it != meta_end; ++it)
    {
        file_buf.seek(it->offset, SEEK_SET);
        InvertedIndex::blockSearchRange(bitmap_filter, file_buf, real_begin, real_end);
    }
}

template class InvertedIndexBuilder<UInt8>;
template class InvertedIndexBuilder<UInt16>;
template class InvertedIndexBuilder<UInt32>;
template class InvertedIndexBuilder<UInt64>;
template class InvertedIndexBuilder<Int8>;
template class InvertedIndexBuilder<Int16>;
template class InvertedIndexBuilder<Int32>;
template class InvertedIndexBuilder<Int64>;
template class InvertedIndexMemoryViewer<UInt8>;
template class InvertedIndexMemoryViewer<UInt16>;
template class InvertedIndexMemoryViewer<UInt32>;
template class InvertedIndexMemoryViewer<UInt64>;
template class InvertedIndexMemoryViewer<Int8>;
template class InvertedIndexMemoryViewer<Int16>;
template class InvertedIndexMemoryViewer<Int32>;
template class InvertedIndexMemoryViewer<Int64>;
template class InvertedIndexFileViewer<UInt8>;
template class InvertedIndexFileViewer<UInt16>;
template class InvertedIndexFileViewer<UInt32>;
template class InvertedIndexFileViewer<UInt64>;
template class InvertedIndexFileViewer<Int8>;
template class InvertedIndexFileViewer<Int16>;
template class InvertedIndexFileViewer<Int32>;
template class InvertedIndexFileViewer<Int64>;

} // namespace DB::DM
