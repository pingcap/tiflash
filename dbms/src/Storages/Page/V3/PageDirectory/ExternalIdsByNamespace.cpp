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

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdsByNamespace.h>

#include <mutex>
#include <optional>

namespace DB::PS::V3
{
void ExternalIdsByNamespace::addExternalIdUnlock(const std::shared_ptr<PageIdV3Internal> & external_id)
{
    const NamespaceId & ns_id = external_id->high;
    // create a new ExternalIds if the ns_id is not exists, else return
    // the existing one.
    auto [ns_iter, new_inserted] = ids_by_ns.try_emplace(ns_id, ExternalIds{});
    ns_iter->second.emplace_back(std::weak_ptr<PageIdV3Internal>(external_id));
}

void ExternalIdsByNamespace::addExternalId(const std::shared_ptr<PageIdV3Internal> & external_id)
{
    std::unique_lock map_guard(mu);
    addExternalIdUnlock(external_id);
}

std::optional<std::set<PageId>> ExternalIdsByNamespace::getAliveIds(NamespaceId ns_id) const
{
    // Now we assume a lock among all NamespaceIds is good enough.
    std::unique_lock map_guard(mu);

    auto ns_iter = ids_by_ns.find(ns_id);
    if (ns_iter == ids_by_ns.end())
        return std::nullopt;

    // Only scan the given `ns_id`
    std::set<PageId> valid_external_ids;
    auto & external_ids = ns_iter->second;
    for (auto iter = external_ids.begin(); iter != external_ids.end(); /*empty*/)
    {
        if (auto holder = iter->lock(); holder == nullptr)
        {
            // the external id has been removed from `PageDirectory`,
            // cleanup the invalid weak_ptr
            iter = external_ids.erase(iter);
            continue;
        }
        else
        {
            valid_external_ids.emplace(holder->low);
            ++iter;
        }
    }
    // The `external_ids` maybe an empty list now, leave it to be
    // cleaned by unregister
    return valid_external_ids;
}

void ExternalIdsByNamespace::unregisterNamespace(NamespaceId ns_id)
{
    std::unique_lock map_guard(mu);
    // free all weak_ptrs of this namespace
    ids_by_ns.erase(ns_id);
}
} // namespace DB::PS::V3
