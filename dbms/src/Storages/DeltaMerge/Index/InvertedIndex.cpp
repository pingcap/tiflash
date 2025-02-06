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
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
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
    for (const auto & entry : meta)
    {
        auto [value, row_ids] = entry;
        writeIntBinary(value, write_buf); // value
        writeIntBinary(static_cast<UInt32>(row_ids.size()), write_buf); // row_ids size
        for (const auto & row_id : row_ids)
            writeVarUInt(row_id, write_buf); // row_ids
    }
}

template <typename T>
void deserializeBlock(Block<T> & meta, ReadBuffer & read_buf)
{
    UInt32 size;
    readIntBinary(size, read_buf);
    meta.resize(size);
    for (auto & entry : meta)
    {
        auto & [value, row_ids] = entry;
        readIntBinary(value, read_buf); // value
        UInt32 row_ids_size;
        readIntBinary(row_ids_size, read_buf); // row_ids size
        row_ids.resize(row_ids_size);
        for (auto & row_id : row_ids)
            readVarUInt(row_id, read_buf); // row_ids
    }
}

// Get the size of the block in bytes. But it is not accurate, because the size of the row_ids is variable.
size_t getBlockSize(UInt32 entry_size)
{
    return 1 + entry_size * sizeof(BlockEntry<UInt32>);
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

template <typename T>
bool InvertedIndexBuilder<T>::isSupportedType(const IDataType & type)
{
    const auto * nullable = checkAndGetDataType<DataTypeNullable>(&type);
    if (nullable)
        return nullable->getNestedType()->isValueRepresentedByInteger();

    return type.isValueRepresentedByInteger();
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
    write_buf.sync();
}

template <typename T>
void InvertedIndexBuilder<T>::saveToBuffer(WriteBuffer & write_buf) const
{
    InvertedIndex::Meta<T> meta;

    // 1. write data by block
    size_t offset = 0;
    size_t uncompressed_offset = 0;
    static const CompressionSettings settings(CompressionMethod::LZ4);
    CompressedWriteBuffer compressed(write_buf, settings);

    InvertedIndex::Block<T> block;
    auto write_block = [&] {
        InvertedIndex::serializeBlock(block, compressed);
        meta.emplace_back(
            offset,
            compressed.getUncompressedBytes() - uncompressed_offset,
            block.front().value,
            block.back().value);
        block.clear();
        compressed.next(); // compress
        offset = compressed.getCompressedBytes();
        uncompressed_offset = compressed.getUncompressedBytes();
    };

    for (const auto & [key, row_ids] : index)
    {
        block.emplace_back(key, row_ids);

        // write block
        if (InvertedIndex::getBlockSize(block.size()) >= InvertedIndex::BlockSize)
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

    write_buf.next();
}

InvertedIndexViewerPtr InvertedIndexViewer::view(TypeIndex type_id, std::string_view path)
{
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

InvertedIndexViewerPtr InvertedIndexViewer::view(TypeIndex type_id, ReadBuffer & buf, size_t index_size)
{
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
    std::string_view buffer(buf.data(), buf.size());
    InvertedIndex::Meta<T> meta;
    data_size = data_size - meta_size;
    {
        ReadBufferFromString rbuf(buffer.substr(data_size, meta_size));
        InvertedIndex::deserializeMeta(meta, rbuf);
    }

    // 5. read blocks & build index
    ReadBufferFromString rbuf(buffer.substr(0, data_size));
    CompressedReadBuffer compressed(rbuf);
    for (const auto meta_entry : meta)
    {
        std::vector<char> block_data(meta_entry.size);
        compressed.readBig(block_data.data(), meta_entry.size);
        ReadBufferFromString block_buf(block_data);
        InvertedIndex::Block<T> block;
        InvertedIndex::deserializeBlock(block, block_buf);
        for (const auto & block_entry : block)
        {
            auto [value, row_ids] = block_entry;
            index[value] = row_ids;
        }
    }
}

template <typename T>
std::vector<typename InvertedIndexMemoryViewer<T>::RowID> InvertedIndexMemoryViewer<T>::search(const Key & key) const
{
    T real_key = key;
    auto it = index.find(real_key);
    return it == index.end() ? std::vector<RowID>{} : it->second;
}

template <typename T>
std::vector<typename InvertedIndexMemoryViewer<T>::RowID> InvertedIndexMemoryViewer<T>::searchRange(
    const Key & begin,
    const Key & end) const
{
    T real_begin = begin;
    T real_end = end;
    std::vector<RowID> result;
    auto index_begin = index.lower_bound(real_begin);
    auto index_end = index.lower_bound(real_end);
    for (auto it = index_begin; it != index_end; ++it)
        result.insert(result.end(), it->second.begin(), it->second.end());
    return result;
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
    std::string_view buffer(buf.data() + data_size, meta_size);
    {
        ReadBufferFromString rbuf(buffer);
        InvertedIndex::deserializeMeta(meta, rbuf);
    }
}

template <typename T>
InvertedIndex::Block<T> InvertedIndexFileViewer<T>::readBlock(UInt32 offset, UInt32 size) const
{
    ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    file_buf.ignore(offset);
    CompressedReadBuffer compressed(file_buf);
    std::vector<char> block_data(size);
    RUNTIME_CHECK(compressed.readBig(block_data.data(), size) == size);
    ReadBufferFromString block_buf(block_data);
    InvertedIndex::Block<T> block;
    InvertedIndex::deserializeBlock(block, block_buf);
    return block;
}

template <typename T>
std::vector<typename InvertedIndexFileViewer<T>::RowID> InvertedIndexFileViewer<T>::search(const Key & key) const
{
    T real_key = key;
    auto it = std::find_if(meta.begin(), meta.end(), [&](const auto & entry) {
        return entry.min <= real_key && entry.max >= real_key;
    });
    if (it == meta.end())
        return std::vector<RowID>{};

    const auto block = readBlock(it->offset, it->size);
    auto block_it
        = std::find_if(block.begin(), block.end(), [&](const auto & entry) { return entry.value == real_key; });
    return block_it == block.end() ? std::vector<RowID>{} : block_it->row_ids;
}

template <typename T>
std::vector<typename InvertedIndexFileViewer<T>::RowID> InvertedIndexFileViewer<T>::searchRange(
    const Key & begin,
    const Key & end) const
{
    T real_begin = begin;
    T real_end = end;
    std::vector<RowID> result;
    // max < begin
    auto meta_begin = std::lower_bound(meta.begin(), meta.end(), real_begin, [](const auto & entry, const auto & key) {
        return entry.max < key;
    });
    // min >= end
    auto meta_end = std::lower_bound(meta_begin, meta.end(), real_end, [](const auto & entry, const auto & key) {
        return entry.min < key;
    });

    for (auto it = meta_begin; it != meta_end; ++it)
    {
        const auto block = readBlock(it->offset, it->size);
        auto block_begin
            = std::lower_bound(block.begin(), block.end(), real_begin, [](const auto & entry, const auto & key) {
                  return entry.value < key;
              });
        for (auto block_it = block_begin; block_it != block.end(); ++block_it)
        {
            auto [value, row_ids] = *block_it;
            if (value >= real_begin && value < real_end)
                result.insert(result.end(), row_ids.begin(), row_ids.end());
            else if (value >= real_end)
                break;
        }
    }
    return result;
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
