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

#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>

namespace DB::DM::Remote
{

Page ColumnFileDataProviderRNLocalPageCache::readTinyData(
    PageId page_id,
    const std::optional<std::vector<size_t>> & fields) const
{
    RUNTIME_CHECK_MSG(
        fields.has_value(),
        "ColumnFileDataProviderRNLocalPageCache currently does not support read all data from a page");

    auto oid = Remote::PageOID{
        .store_id = store_id,
        .ks_table_id = ks_table_id,
        .page_id = page_id,
    };
    return page_cache->getPage(oid, *fields);
}

size_t ColumnFileDataProviderRNLocalPageCache::getTinyDataSize(PageId) const
{
    RUNTIME_CHECK_MSG(false, "ColumnFileDataProviderRNLocalPageCache currently does not support getTinyDataSize");
}

} // namespace DB::DM::Remote
