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

#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/Index/LocalIndexBuilder.h>
#include <Storages/DeltaMerge/Index/LocalIndexViewer.h>


namespace DB::DM
{

namespace InvertedIndex
{

// InvertedIndex file format:
// | Block 0 (compressed) | Block 1 (compressed) | ... | Block N (compressed) | Meta | Meta size | Magic flag |

// Block format:
// | number of values | value | row_ids size | row_ids | value | row_ids size | row_ids | ... | value | row_ids size | row_ids |

// Meta format:
// | size of T | number of blocks | offset | size | min | max | offset | size | min | max | ... | offset | size | min | max |

using RowID = UInt32;

// A block is a minimal unit of IO, it will be as small as possible, but >= 4KB.
static constexpr size_t BlockSize = 4 * 1024; // 4 KB

// <value, row_ids>
template <typename T>
struct BlockEntry
{
    T value;
    std::vector<RowID> row_ids;
};

template <typename T>
using Block = std::vector<BlockEntry<T>>;

// <offset, size, min, max>
template <typename T>
struct MetaEntry
{
    UInt32 offset; // offset in the file
    UInt32 size; // block size, uncompressed
    T min;
    T max;
};

template <typename T>
using Meta = std::vector<MetaEntry<T>>;
} // namespace InvertedIndex

/// Builds a InvertedIndex in memory.
template <typename T>
class InvertedIndexBuilder : public LocalIndexBuilder
{
public:
    using Key = T;
    using RowID = InvertedIndex::RowID;

public:
    static bool isSupportedType(const IDataType & type);

public:
    explicit InvertedIndexBuilder(const LocalIndexInfo & index_info)
        : LocalIndexBuilder(index_info)
    {}

    ~InvertedIndexBuilder() override = default;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;
    void saveToFile(std::string_view path) const override;
    void saveToBuffer(WriteBuffer & write_buf) const override;

public:
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.
    std::map<Key, std::vector<RowID>> index;
    double total_duration = 0;
};

LocalIndexBuilderPtr createInvertedIndexBuilder(const LocalIndexInfo & index_info);

/// Views a InvertedIndex file.
class InvertedIndexViewer : public LocalIndexViewer
{
public:
    using Key = UInt64;
    using RowID = InvertedIndex::RowID;

public:
    explicit InvertedIndexViewer() = default;
    ~InvertedIndexViewer() override = default;

    static InvertedIndexViewerPtr view(TypeIndex type_id, std::string_view path);
    static InvertedIndexViewerPtr view(TypeIndex type_id, ReadBuffer & buf, size_t index_size);

    virtual std::vector<RowID> search(const Key & key) const = 0;
    // [begin, end)
    virtual std::vector<RowID> searchRange(const Key & begin, const Key & end) const = 0;
};

/// Views a InvertedIndex file by loading it into memory.
/// Its performance is better than InvertedIndexFileViewer but it consumes more memory.
template <typename T>
class InvertedIndexMemoryViewer : public InvertedIndexViewer
{
public:
    using Key = InvertedIndexViewer::Key;
    using RowID = InvertedIndexViewer::RowID;

private:
    void load(ReadBuffer & buf, size_t index_size);

public:
    explicit InvertedIndexMemoryViewer(std::string_view path)
    {
        ReadBufferFromFile buf(path.data());
        load(buf, Poco::File(path.data()).getSize());
    }

    InvertedIndexMemoryViewer(ReadBuffer & buf, size_t index_size) { load(buf, index_size); }

    ~InvertedIndexMemoryViewer() override = default;

    std::vector<RowID> search(const Key & key) const override;
    std::vector<RowID> searchRange(const Key & begin, const Key & end) const override;

private:
    std::map<Key, std::vector<RowID>> index; // set by load
};

/// Views a InvertedIndex file by reading it from disk.
/// Its memory usage is minimal but its performance is worse than InvertedIndexMemoryViewer.
template <typename T>
class InvertedIndexFileViewer : public InvertedIndexViewer
{
public:
    using Key = InvertedIndexViewer::Key;
    using RowID = InvertedIndexViewer::RowID;

private:
    void loadMeta(ReadBuffer & buf, size_t index_size);

    InvertedIndex::Block<T> readBlock(UInt32 offset, UInt32 size) const;

public:
    explicit InvertedIndexFileViewer(std::string_view path)
        : path(path)
    {
        ReadBufferFromFile buffer(path.data());
        loadMeta(buffer, Poco::File(path.data()).getSize());
    }

    ~InvertedIndexFileViewer() override = default;

    std::vector<RowID> search(const Key & key) const override;
    std::vector<RowID> searchRange(const Key & begin, const Key & end) const override;

private:
    String path;
    InvertedIndex::Meta<T> meta; // set by loadMeta
};

} // namespace DB::DM
