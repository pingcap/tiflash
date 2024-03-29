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

#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/ReadMode.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool_fwd.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB
{
namespace DM
{
static constexpr size_t COLUMN_FILE_SERIALIZE_BUFFER_SIZE = 65536;

class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
using ColumnFiles = std::vector<ColumnFilePtr>;
class ColumnFileInMemory;
class ColumnFileTiny;
class ColumnFileDeleteRange;
class ColumnFileBig;
class ColumnFilePersisted;
class ColumnFileReader;
using ColumnFileReaderPtr = std::shared_ptr<ColumnFileReader>;

static std::atomic_uint64_t MAX_COLUMN_FILE_ID{0};

/// ColumnFile represents the files stored in a Segment, like the "SST File" of LSM-Tree.
/// ColumnFile has several concrete sub-classes that represent different kinds of data.
///
///   ColumnFile
///   |-- ColumnFileInMemory
///   |-- ColumnFilePersisted (column file that can be persisted on disk)
///         |-- ColumnFileTiny
///         |-- ColumnFileDeleteRange
///         |-- ColumnFileBig
class ColumnFile
{
protected:
    UInt64 id;

    bool saved = false;

    ColumnFile()
        : id(++MAX_COLUMN_FILE_ID)
    {}

    virtual ~ColumnFile() = default;

public:
    enum Type : UInt32
    {
        DELETE_RANGE = 1,
        TINY_FILE = 2,
        BIG_FILE = 3,
        INMEMORY_FILE = 4,
    };

    struct Cache
    {
        explicit Cache(const Block & header)
            : block(header.cloneWithColumns(header.cloneEmptyColumns()))
        {}
        explicit Cache(Block && block)
            : block(std::move(block))
        {}

        std::mutex mutex;
        Block block;
    };
    using CachePtr = std::shared_ptr<Cache>;
    using ColIdToOffset = std::unordered_map<ColId, size_t>;

public:
    /// This id is only used to to do equal check in isSame().
    UInt64 getId() const { return id; }

    virtual size_t getRows() const { return 0; }
    virtual size_t getBytes() const { return 0; };
    virtual size_t getDeletes() const { return 0; };

    virtual Type getType() const = 0;

    /// Is a ColumnFileInMemory or not.
    bool isInMemoryFile() const { return getType() == Type::INMEMORY_FILE; }
    /// Is a ColumnFileTiny or not.
    bool isTinyFile() const { return getType() == Type::TINY_FILE; }
    /// Is a ColumnFileDeleteRange or not.
    bool isDeleteRange() const { return getType() == Type::DELETE_RANGE; };
    /// Is a ColumnFileBig or not.
    bool isBigFile() const { return getType() == Type::BIG_FILE; };
    /// Is a ColumnFilePersisted or not
    bool isPersisted() const { return getType() != Type::INMEMORY_FILE; };

    /**
     * Whether this column file SEEMS TO BE flushed from another.
     *
     * As it only compares metadata and never checks real data, it is not accurate
     * when checking whether a ColumnFileTiny is flushed from a ColumnFileInMemory.
     */
    virtual bool mayBeFlushedFrom(ColumnFile *) const { return false; }

    bool isSame(ColumnFile * other) const { return id == other->id; }

    ColumnFileInMemory * tryToInMemoryFile();
    ColumnFileTiny * tryToTinyFile();
    ColumnFileDeleteRange * tryToDeleteRange();
    ColumnFileBig * tryToBigFile();

    ColumnFilePersisted * tryToColumnFilePersisted();

    virtual ColumnFileReaderPtr getReader(
        const DMContext & context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefinesPtr & col_defs) const
        = 0;

    /// Note: Only ColumnFileInMemory can be appendable. Other ColumnFiles (i.e. ColumnFilePersisted) have
    /// been persisted in the disk and their data will be immutable.
    virtual bool isAppendable() const { return false; }
    virtual void disableAppend() {}
    virtual bool append(
        const DMContext & /*dm_context*/,
        const Block & /*data*/,
        size_t /*offset*/,
        size_t /*limit*/,
        size_t /*data_bytes*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    virtual String toString() const = 0;
};


class ColumnFileReader
{
public:
    virtual ~ColumnFileReader() = default;
    ColumnFileReader() = default;
    DISALLOW_COPY(ColumnFileReader);

    /// Read data from this reader and store the result into output_cols.
    /// Note that if "range" is specified, then the caller must guarantee that the rows between [rows_offset, rows_offset + rows_limit) are sorted.
    /// Returns <actual_offset, actual_limit>
    virtual std::pair<size_t, size_t> readRows(
        MutableColumns & /*output_cols*/,
        size_t /*rows_offset*/,
        size_t /*rows_limit*/,
        const RowKeyRange * /*range*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    /// This method is only used to read raw data.
    virtual Block readNextBlock() { throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR); }

    /// This method used to skip next block.
    virtual size_t skipNextBlock() { throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR); }

    /// Create a new reader from current reader with different columns to read.
    virtual ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & col_defs) = 0;

    virtual void setReadTag(ReadTag /*read_tag*/) {}
};

std::pair<size_t, size_t> copyColumnsData(
    const Columns & from,
    const ColumnPtr & pk_col,
    MutableColumns & to,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range);


/// Debugging string
template <typename T>
String columnFilesToString(const T & column_files);
} // namespace DM
} // namespace DB
