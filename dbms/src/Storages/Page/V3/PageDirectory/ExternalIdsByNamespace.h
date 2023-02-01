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

#include <Common/nocopyable.h>
#include <Storages/Page/PageDefines.h>

#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

namespace DB::PS::V3
{

// A thread-safe class to manage external ids.
// Manage all external ids by NamespaceId.
class ExternalIdsByNamespace
{
public:
    ExternalIdsByNamespace() = default;

    // Add a external ids
    void addExternalId(const std::shared_ptr<PageIdV3Internal> & external_id);
    // non thread-safe version, only for restore
    void addExternalIdUnlock(const std::shared_ptr<PageIdV3Internal> & external_id);

    // Get all alive external ids of given `ns_id`
    // Will also cleanup the invalid external ids.
    // If the ns_id is invalid, std::nullopt will be returned.
    std::optional<std::set<PageId>> getAliveIds(NamespaceId ns_id) const;

    // After table dropped, the `getAliveIds` with specified
    // `ns_id` will not be cleaned. We need this method to
    // cleanup all external id ptrs.
    void unregisterNamespace(NamespaceId ns_id);

    // Check whether `ns_id` exist. Expose for testing.
    // Note that the result is meaningless unless `getAliveIds`
    // or `unregisterNamespace` is called to cleanup invalid
    // external ids.
    bool existNamespace(NamespaceId ns_id) const
    {
        std::lock_guard map_guard(mu);
        return ids_by_ns.count(ns_id) > 0;
    }

    DISALLOW_COPY_AND_MOVE(ExternalIdsByNamespace);

private:
    mutable std::mutex mu;
    // Only store weak_ptrs. The weak_ptrs will be invalid after the external id
    // in PageDirectory get removed.
    using ExternalIds = std::list<std::weak_ptr<PageIdV3Internal>>;
    using NamespaceMap = std::unordered_map<NamespaceId, ExternalIds>;
    mutable NamespaceMap ids_by_ns;
};
} // namespace DB::PS::V3
