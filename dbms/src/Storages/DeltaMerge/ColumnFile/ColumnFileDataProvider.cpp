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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/Page/PageStorage.h>

namespace DB::DM
{

Page ColumnFileDataProviderLocalStoragePool::readTinyData(
    IColumnFileDataProvider::PageId page_id,
    const std::optional<std::vector<size_t>> & fields) const
{
    if (!fields.has_value())
        return storage_snap->log_reader->read(page_id);

    auto page_map = storage_snap->log_reader->read({{page_id, *fields}});
    return page_map.at(page_id);
}

size_t ColumnFileDataProviderLocalStoragePool::getTinyDataSize(IColumnFileDataProvider::PageId page_id) const
{
    return storage_snap->log_reader->getPageEntry(page_id).size;
}

} // namespace DB::DM
