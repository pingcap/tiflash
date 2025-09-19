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

#include <DataTypes/DataTypeNullable.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader.h>

#include <type_traits>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ABORTED;
} // namespace DB::ErrorCodes

namespace DB::DM
{

InvertedIndexReaderPtr InvertedIndexReader::view(const DataTypePtr & type, std::string_view path)
{
    auto type_id = removeNullable(type)->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return std::make_shared<InvertedIndexFileReader<UInt8>>(path);
    case TypeIndex::Int8:
        return std::make_shared<InvertedIndexFileReader<Int8>>(path);
    case TypeIndex::UInt16:
        return std::make_shared<InvertedIndexFileReader<UInt16>>(path);
    case TypeIndex::Int16:
        return std::make_shared<InvertedIndexFileReader<Int16>>(path);
    case TypeIndex::UInt32:
        return std::make_shared<InvertedIndexFileReader<UInt32>>(path);
    case TypeIndex::Int32:
        return std::make_shared<InvertedIndexFileReader<Int32>>(path);
    case TypeIndex::UInt64:
        return std::make_shared<InvertedIndexFileReader<UInt64>>(path);
    case TypeIndex::Int64:
        return std::make_shared<InvertedIndexFileReader<Int64>>(path);
    case TypeIndex::Date:
        return std::make_shared<InvertedIndexFileReader<UInt16>>(path);
    case TypeIndex::DateTime:
        return std::make_shared<InvertedIndexFileReader<UInt32>>(path);
    case TypeIndex::Enum8:
        return std::make_shared<InvertedIndexFileReader<Int8>>(path);
    case TypeIndex::Enum16:
        return std::make_shared<InvertedIndexFileReader<Int16>>(path);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return std::make_shared<InvertedIndexFileReader<UInt64>>(path);
    case TypeIndex::MyTime:
        return std::make_shared<InvertedIndexFileReader<Int64>>(path);
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type_id: {}", magic_enum::enum_name(type_id));
    }
}

InvertedIndexReaderPtr InvertedIndexReader::view(const DataTypePtr & type, ReadBuffer & buf, size_t index_size)
{
    auto type_id = removeNullable(type)->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return std::make_shared<InvertedIndexMemoryReader<UInt8>>(buf, index_size);
    case TypeIndex::Int8:
        return std::make_shared<InvertedIndexMemoryReader<Int8>>(buf, index_size);
    case TypeIndex::UInt16:
        return std::make_shared<InvertedIndexMemoryReader<UInt16>>(buf, index_size);
    case TypeIndex::Int16:
        return std::make_shared<InvertedIndexMemoryReader<Int16>>(buf, index_size);
    case TypeIndex::UInt32:
        return std::make_shared<InvertedIndexMemoryReader<UInt32>>(buf, index_size);
    case TypeIndex::Int32:
        return std::make_shared<InvertedIndexMemoryReader<Int32>>(buf, index_size);
    case TypeIndex::UInt64:
        return std::make_shared<InvertedIndexMemoryReader<UInt64>>(buf, index_size);
    case TypeIndex::Int64:
        return std::make_shared<InvertedIndexMemoryReader<Int64>>(buf, index_size);
    case TypeIndex::Date:
        return std::make_shared<InvertedIndexMemoryReader<UInt16>>(buf, index_size);
    case TypeIndex::DateTime:
        return std::make_shared<InvertedIndexMemoryReader<UInt32>>(buf, index_size);
    case TypeIndex::Enum8:
        return std::make_shared<InvertedIndexMemoryReader<Int8>>(buf, index_size);
    case TypeIndex::Enum16:
        return std::make_shared<InvertedIndexMemoryReader<Int16>>(buf, index_size);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return std::make_shared<InvertedIndexMemoryReader<UInt64>>(buf, index_size);
    case TypeIndex::MyTime:
        return std::make_shared<InvertedIndexMemoryReader<Int64>>(buf, index_size);
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type_id: {}", magic_enum::enum_name(type_id));
    }
}

template <typename T>
void InvertedIndexMemoryReader<T>::load(ReadBuffer & read_buf, size_t index_size)
{
    // 0. check version
    UInt8 version;
    readIntBinary(version, read_buf);
    RUNTIME_CHECK(version == magic_enum::enum_integer(InvertedIndex::Version::V1));
    index_size -= sizeof(UInt8);

    // 1. read all data
    std::vector<char> buf(index_size);
    RUNTIME_CHECK(read_buf.readBig(buf.data(), index_size) == index_size);

    // 2. check magic flag
    size_t data_size = index_size - InvertedIndex::MagicFlagLength;
    if (memcmp(buf.data() + data_size, InvertedIndex::MagicFlag.data(), InvertedIndex::MagicFlagLength) != 0)
        throw Exception(ErrorCodes::ABORTED, "Invalid magic flag");

    // 3. read meta size
    data_size = data_size - sizeof(UInt32);
    UInt32 meta_size = *reinterpret_cast<const UInt32 *>(buf.data() + data_size);

    // 4. read meta
    ReadBufferFromMemory buffer(buf.data() + data_size - meta_size, meta_size);
    InvertedIndex::Meta<T> meta;
    data_size = data_size - meta_size;
    InvertedIndex::Meta<T>::deserialize(meta, buffer);

    // 5. read blocks & build index
    buffer = ReadBufferFromMemory(buf.data(), data_size);
    for (const auto meta_entry : meta.entries)
    {
        auto count = buffer.count();
        InvertedIndex::Block<T> block;
        InvertedIndex::Block<T>::deserialize(block, buffer);
        RUNTIME_CHECK(buffer.count() - count == meta_entry.size);
        for (const auto & block_entry : block.entries)
        {
            auto [value, row_ids] = block_entry;
            index[value] = row_ids;
        }
    }
}

namespace
{

template <typename T>
inline bool isKeyOutOfRange(const InvertedIndexReader::Key key)
{
    if constexpr (std::is_signed_v<T>)
    {
        using SignedKey = std::make_signed_t<InvertedIndexReader::Key>;
        SignedKey signed_key = key;
        return signed_key < std::numeric_limits<T>::min() || signed_key > std::numeric_limits<T>::max();
    }
    else
    {
        return key > std::numeric_limits<T>::max();
    }
}

template <typename T>
inline bool isKeyLessThanMin(const InvertedIndexReader::Key key)
{
    if constexpr (std::is_signed_v<T>)
    {
        using SignedKey = std::make_signed_t<InvertedIndexReader::Key>;
        SignedKey signed_key = key;
        return signed_key < std::numeric_limits<T>::min();
    }
    else
    {
        return false;
    }
}

template <typename T>
inline bool isKeyGreaterThanMax(const InvertedIndexReader::Key key)
{
    if constexpr (std::is_signed_v<T>)
    {
        using SignedKey = std::make_signed_t<InvertedIndexReader::Key>;
        SignedKey signed_key = key;
        return signed_key > std::numeric_limits<T>::max();
    }
    else
    {
        return key > std::numeric_limits<T>::max();
    }
}

} // namespace

template <typename T>
void InvertedIndexMemoryReader<T>::search(BitmapFilterPtr & bitmap_filter, const Key & key) const
{
    // handle wider data type
    if (isKeyOutOfRange<T>(key))
        return;

    T real_key = key;
    auto it = index.find(real_key);
    if (it != index.end())
        bitmap_filter->set(it->second, nullptr);
}

template <typename T>
void InvertedIndexMemoryReader<T>::searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end)
    const
{
    // handle wider data type
    if (isKeyGreaterThanMax<T>(begin) || isKeyLessThanMin<T>(end))
        return;
    T real_begin = begin;
    if (isKeyLessThanMin<T>(begin))
        real_begin = std::numeric_limits<T>::min();
    T real_end = end;
    if (isKeyGreaterThanMax<T>(end))
        real_end = std::numeric_limits<T>::max();

    auto index_begin = index.lower_bound(real_begin);
    auto index_end = index.upper_bound(real_end);
    for (auto it = index_begin; it != index_end; ++it)
        bitmap_filter->set(it->second, nullptr);
}

template <typename T>
void InvertedIndexFileReader<T>::loadMeta(ReadBuffer & read_buf, size_t index_size)
{
    // 0. check version
    UInt8 version;
    readIntBinary(version, read_buf);
    RUNTIME_CHECK(version == magic_enum::enum_integer(InvertedIndex::Version::V1));
    index_size -= sizeof(UInt8);

    // 1. read all data
    std::vector<char> buf(index_size);
    RUNTIME_CHECK(read_buf.readBig(buf.data(), index_size) == index_size);

    // 2. check magic flag
    size_t data_size = index_size - InvertedIndex::MagicFlagLength;
    if (memcmp(buf.data() + data_size, InvertedIndex::MagicFlag.data(), InvertedIndex::MagicFlagLength) != 0)
        throw Exception(ErrorCodes::ABORTED, "Invalid magic flag");

    // 3. read meta size
    data_size = data_size - sizeof(UInt32);
    UInt32 meta_size = *reinterpret_cast<const UInt32 *>(buf.data() + data_size);

    // 4. read meta
    data_size = data_size - meta_size;
    ReadBufferFromMemory buffer(buf.data() + data_size, meta_size);
    InvertedIndex::Meta<T>::deserialize(meta, buffer);
}

template <typename T>
void InvertedIndexFileReader<T>::search(BitmapFilterPtr & bitmap_filter, const Key & key) const
{
    // handle wider data type
    if (isKeyOutOfRange<T>(key))
        return;

    T real_key = key;
    auto it = std::find_if(meta.entries.begin(), meta.entries.end(), [&](const auto & entry) {
        return entry.min <= real_key && entry.max >= real_key;
    });
    if (it == meta.entries.end())
        return;

    ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto ret = file_buf.seek(it->offset, SEEK_SET);
    RUNTIME_CHECK_MSG(
        ret >= 0,
        "Failed to seek in inverted index file, ret={} path={} offset={}",
        ret,
        path,
        it->offset);
    InvertedIndex::Block<T>::search(bitmap_filter, file_buf, real_key);
}

template <typename T>
void InvertedIndexFileReader<T>::searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end) const
{
    // handle wider data type
    if (isKeyGreaterThanMax<T>(begin) || isKeyLessThanMin<T>(end))
        return;
    T real_begin = begin;
    if (isKeyLessThanMin<T>(begin))
        real_begin = std::numeric_limits<T>::min();
    T real_end = end;
    if (isKeyGreaterThanMax<T>(end))
        real_end = std::numeric_limits<T>::max();

    // max < begin
    auto meta_begin = std::lower_bound(
        meta.entries.begin(),
        meta.entries.end(),
        real_begin,
        [](const auto & entry, const auto & key) { return entry.max < key; });
    // min > end
    auto meta_end
        = std::upper_bound(meta_begin, meta.entries.end(), real_end, [](const auto & key, const auto & entry) {
              return key < entry.min;
          });

    ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    for (auto it = meta_begin; it != meta_end; ++it)
    {
        auto ret = file_buf.seek(it->offset, SEEK_SET);
        RUNTIME_CHECK_MSG(
            ret >= 0,
            "Failed to seek in inverted index file, ret={} path={} offset={}",
            ret,
            this->path,
            it->offset);
        InvertedIndex::Block<T>::searchRange(bitmap_filter, file_buf, real_begin, real_end);
    }
}

template class InvertedIndexMemoryReader<UInt8>;
template class InvertedIndexMemoryReader<UInt16>;
template class InvertedIndexMemoryReader<UInt32>;
template class InvertedIndexMemoryReader<UInt64>;
template class InvertedIndexMemoryReader<Int8>;
template class InvertedIndexMemoryReader<Int16>;
template class InvertedIndexMemoryReader<Int32>;
template class InvertedIndexMemoryReader<Int64>;
template class InvertedIndexFileReader<UInt8>;
template class InvertedIndexFileReader<UInt16>;
template class InvertedIndexFileReader<UInt32>;
template class InvertedIndexFileReader<UInt64>;
template class InvertedIndexFileReader<Int8>;
template class InvertedIndexFileReader<Int16>;
template class InvertedIndexFileReader<Int32>;
template class InvertedIndexFileReader<Int64>;

} // namespace DB::DM
