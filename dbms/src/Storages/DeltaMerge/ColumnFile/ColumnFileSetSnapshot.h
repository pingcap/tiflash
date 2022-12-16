// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetStorageReader.h>

namespace DB
{
namespace DM
{
class ColumnFileSetSnapshot;
using ColumnFileSetSnapshotPtr = std::shared_ptr<ColumnFileSetSnapshot>;
class ColumnFileSetReader;
using ColumnFileSetReaderPtr = std::shared_ptr<ColumnFileSetReader>;

class BlockOrDelete
{
private:
    Block block;
    size_t block_offset;

    RowKeyRange delete_range;

public:
    BlockOrDelete(Block && block_, size_t offset_)
        : block(block_)
        , block_offset(offset_)
    {}
    explicit BlockOrDelete(const RowKeyRange & delete_range_)
        : delete_range(delete_range_)
    {}

    bool isBlock() { return (bool)block; }
    auto & getBlock() { return block; };
    auto getBlockOffset() { return block_offset; }
    auto & getDeleteRange() { return delete_range; }
};

using BlockOrDeletes = std::vector<BlockOrDelete>;

/**
 * An immutable list of Column Files.
 */
class ColumnFileSetSnapshot : public std::enable_shared_from_this<ColumnFileSetSnapshot>
    , private boost::noncopyable
{
    friend class MemTableSet;
    friend class ColumnFilePersistedSet;

private:
    IColumnFileSetStorageReaderPtr storage;

    ColumnFiles column_files;
    size_t rows = 0;
    size_t bytes = 0;
    size_t deletes = 0;

    bool is_common_handle = false;
    size_t rowkey_column_size = 1;

public:
    explicit ColumnFileSetSnapshot(const IColumnFileSetStorageReaderPtr & storage_)
        : storage{storage_}
    {}

    ColumnFileSetSnapshotPtr clone()
    {
        auto c = std::make_shared<ColumnFileSetSnapshot>(storage);
        c->column_files = column_files;
        c->rows = rows;
        c->bytes = bytes;
        c->deletes = deletes;
        c->is_common_handle = is_common_handle;
        c->rowkey_column_size = rowkey_column_size;

        return c;
    }

    ColumnFiles & getColumnFiles() { return column_files; }

    size_t getColumnFileCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    RowKeyRange getSquashDeleteRange() const;

    const auto & getStorage() const { return storage; }

    std::vector<RemoteProtocol::ColumnFile> serializeToRemoteProtocol() const
    {
        std::vector<RemoteProtocol::ColumnFile> ret;
        ret.reserve(column_files.size());

        for (const auto & file : column_files)
            ret.push_back(file->serializeToRemoteProtocol());

        return ret;
    }

    static ColumnFileSetSnapshotPtr deserializeFromRemoteProtocol(
        const std::vector<RemoteProtocol::ColumnFile> & proto,
        UInt64 remote_write_node_id,
        const DMContext & context,
        const RowKeyRange & segment_range);
};


} // namespace DM
} // namespace DB