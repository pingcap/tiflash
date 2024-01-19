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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool_fwd.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB::DM
{

/**
 * An interface controls how different ColumnFiles reads the (delta) data.
 * ColumnFiles is actually storage independent: it only records a (Page)Id to read the data.
 */
class IColumnFileDataProvider
{
public:
    // ColumnFileTiny always carry a U64 PageId, so it can only read by using a U64 PageId.
    using PageId = PageIdU64;

    virtual ~IColumnFileDataProvider() = default;

    // TODO: We should improve PageStorage to provide a "LazyPage",
    //    from which we can know page metadata (like data size) and lazily
    //    access data when needed.
    //    In this way, we only need one function in this interface, like:
    //    getColumnFileTinyPage(PageId) -> LazyPage

    /// Read tiny data. When fields are provided, only read fields. Otherwise read all data.
    /// For example, When disagg WN responds the page to the disagg RN, all data will be read.
    virtual Page readTinyData(PageId, const std::optional<std::vector<size_t>> & fields = std::nullopt) const = 0;

    /// Get the total data size without reading the data.
    virtual size_t getTinyDataSize(PageId) const = 0;
};

/**
 * ColumnFiles read data from a DM::StoragePool. This is the default way used in production.
 */
class ColumnFileDataProviderLocalStoragePool : public IColumnFileDataProvider
{
private:
    /// Although we only use its log_reader member, we still want to keep the whole
    /// storage snapshot valid, because some Column Files like Column File Big relies
    /// on specific DMFile to be valid, whose lifecycle is maintained by other reader members.
    StorageSnapshotPtr storage_snap;

public:
    explicit ColumnFileDataProviderLocalStoragePool(StorageSnapshotPtr storage_snap_)
        : storage_snap(storage_snap_)
    {}

    static std::shared_ptr<ColumnFileDataProviderLocalStoragePool> create(StorageSnapshotPtr storage_snap)
    {
        return std::make_shared<ColumnFileDataProviderLocalStoragePool>(storage_snap);
    }

    Page readTinyData(PageId page_id, const std::optional<std::vector<size_t>> & fields) const override;

    size_t getTinyDataSize(PageId page_id) const override;
};

/**
 * Indicate that ColumnFiles cannot read data.
 */
class ColumnFileDataProviderNop : public IColumnFileDataProvider
{
public:
    Page readTinyData(PageId, const std::optional<std::vector<size_t>> &) const override
    {
        RUNTIME_CHECK_MSG(false, "ColumnFileDataProviderNop cannot read");
    }

    size_t getTinyDataSize(PageId) const override { RUNTIME_CHECK_MSG(false, "ColumnFileDataProviderNop cannot read"); }
};

} // namespace DB::DM
