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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider_fwd.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/KVStore/Types.h>

namespace DB::DM::Remote
{

class ColumnFileDataProviderRNLocalPageCache : public IColumnFileDataProvider
{
private:
    RNLocalPageCachePtr page_cache;
    RNLocalPageCacheGuardPtr pages_guard; // Only keep for maintaining lifetime for related keys

    StoreID store_id;
    KeyspaceTableID ks_table_id;

public:
    ColumnFileDataProviderRNLocalPageCache(
        RNLocalPageCachePtr page_cache_,
        RNLocalPageCacheGuardPtr pages_guard_,
        StoreID store_id_,
        KeyspaceTableID ks_table_id_)
        : page_cache(page_cache_)
        , pages_guard(pages_guard_)
        , store_id(store_id_)
        , ks_table_id(ks_table_id_)
    {}

    Page readTinyData(PageId page_id, const std::optional<std::vector<size_t>> & fields) const override;

    size_t getTinyDataSize(PageId page_id) const override;
};

} // namespace DB::DM::Remote
