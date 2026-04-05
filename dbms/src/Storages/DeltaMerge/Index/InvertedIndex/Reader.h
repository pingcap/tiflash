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

#pragma once

#include <IO/Buffer/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/Index/ICacheableLocalIndexReader.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/CommonUtil.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader_fwd.h>

namespace DB::DM
{

/// Read a InvertedIndex file.
class InvertedIndexReader : public ICacheableLocalIndexReader
{
public:
    using Key = UInt64;

public:
    explicit InvertedIndexReader() = default;
    ~InvertedIndexReader() override = default;

    static InvertedIndexReaderPtr view(const DataTypePtr & type, std::string_view path);
    static InvertedIndexReaderPtr view(const DataTypePtr & type, ReadBuffer & buf, size_t index_size);

    // All row ids that match the key will be set to 1 in bitmap_filter.
    // Key can be wider than the index type range.
    // But std::is_signed_v<Key> should equal to std::is_signed_v<T>.
    // For example, if the index type is UInt32, key can be std::numeric_limits<UInt32>::max() + 1, but key can not be minus.
    virtual void search(BitmapFilterPtr & bitmap_filter, const Key & key) const = 0;
    // All row ids that match the range [begin, end] will be set to 1 in bitmap_filter.
    // Both begin and end can be wider than the index type range.
    virtual void searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end) const = 0;
};

/// Read a InvertedIndex file by loading it into memory.
/// Its performance is better than InvertedIndexFileReader but it consumes more memory.
template <typename T>
class InvertedIndexMemoryReader : public InvertedIndexReader
{
private:
    void load(ReadBuffer & buf, size_t index_size);

public:
    explicit InvertedIndexMemoryReader(std::string_view path)
    {
        ReadBufferFromFile buf(path.data());
        load(buf, Poco::File(path.data()).getSize());
    }

    InvertedIndexMemoryReader(ReadBuffer & buf, size_t index_size) { load(buf, index_size); }

    ~InvertedIndexMemoryReader() override = default;

    void search(BitmapFilterPtr & bitmap_filter, const Key & key) const override;
    void searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end) const override;

private:
    std::map<T, std::vector<InvertedIndex::RowID>> index; // set by load
};

/// Read a InvertedIndex file by reading it from disk.
/// Its memory usage is minimal but its performance is worse than InvertedIndexMemoryReader.
template <typename T>
class InvertedIndexFileReader : public InvertedIndexReader
{
private:
    void loadMeta(ReadBuffer & buf, size_t index_size);

public:
    explicit InvertedIndexFileReader(std::string_view path)
        : path(path)
    {
        ReadBufferFromFile buffer(path.data());
        loadMeta(buffer, Poco::File(path.data()).getSize());
    }

    ~InvertedIndexFileReader() override = default;

    void search(BitmapFilterPtr & bitmap_filter, const Key & key) const override;
    void searchRange(BitmapFilterPtr & bitmap_filter, const Key & begin, const Key & end) const override;

private:
    // Since this viewer will be used in multiple threads,
    // only store the path and load the file when needed.
    // Warning: Do not shared file_buf between threads.
    const String path;
    InvertedIndex::Meta<T> meta; // set by loadMeta
};

} // namespace DB::DM
