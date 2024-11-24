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

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>

namespace DB::DM
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
        : block_offset(0)
        , delete_range(delete_range_)
    {}

    bool isBlock() { return static_cast<bool>(block); }
    auto & getBlock() { return block; }
    auto getBlockOffset() const { return block_offset; }
    auto & getDeleteRange() { return delete_range; }
};

using BlockOrDeletes = std::vector<BlockOrDelete>;

class ColumnFileSetSnapshot
    : public std::enable_shared_from_this<ColumnFileSetSnapshot>
    , private boost::noncopyable
{
    friend class MemTableSet;
    friend class ColumnFilePersistedSet;
    friend struct Remote::Serializer;

private:
    const ColumnFiles column_files;
    const size_t rows{0};
    const size_t bytes{0};
    const size_t deletes{0};

public:
    /// This field is public writeable intentionally. It allows us to build a snapshot first,
    /// then change how these data can be read later.
    /// In disaggregated mode, we first restore the snapshot from remote proto without a specific data provider (NopProvider),
    /// and then assign the correct data provider according to the data in the snapshot.
    /// Why we don't know the data provider at that time? Because when we have remote proto, data is not yet received.
    IColumnFileDataProviderPtr data_provider = nullptr;

    ColumnFileSetSnapshot(
        const IColumnFileDataProviderPtr & data_provider_,
        ColumnFiles && column_files_,
        size_t rows_,
        size_t bytes_,
        size_t deletes_)
        : column_files(std::move(column_files_))
        , rows(rows_)
        , bytes(bytes_)
        , deletes(deletes_)
        , data_provider{data_provider_}
    {}

    static ColumnFileSetSnapshotPtr buildFromColumnFiles(
        const IColumnFileDataProviderPtr & data_provider_,
        ColumnFiles && column_files_)
    {
        size_t rows = 0, bytes = 0, deletes = 0;
        for (const auto & column_file : column_files_)
        {
            rows += column_file->getRows();
            bytes += column_file->getBytes();
            deletes += column_file->getDeletes();
        }
        return std::make_shared<ColumnFileSetSnapshot>(data_provider_, std::move(column_files_), rows, bytes, deletes);
    }

    ColumnFileSetSnapshotPtr clone()
    {
        ColumnFiles cf_copy = column_files;
        return std::make_shared<ColumnFileSetSnapshot>(data_provider, std::move(cf_copy), rows, bytes, deletes);
    }

    const ColumnFiles & getColumnFiles() const { return column_files; }

    size_t getColumnFileCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    RowKeyRange getSquashDeleteRange(bool is_common_handle, size_t rowkey_column_size) const;

    const auto & getDataProvider() const { return data_provider; }
};

} // namespace DB::DM
