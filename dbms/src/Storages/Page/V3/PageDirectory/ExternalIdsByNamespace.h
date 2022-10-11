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
template <typename Trait>
class ExternalIdsByNamespace
{
private:
    using PageId = typename Trait::PageId;
    using Prefix = typename Trait::Prefix;

public:
    ExternalIdsByNamespace() = default;

    // Add a external ids
    void addExternalId(const std::shared_ptr<PageId> & external_id);
    // non thread-safe version, only for restore
    void addExternalIdUnlock(const std::shared_ptr<PageId> & external_id);

    // Get all alive external ids of given `ns_id`
    // Will also cleanup the invalid external ids.
    std::set<DB::PageId> getAliveIds(const Prefix & ns_id) const;

    // After table dropped, the `getAliveIds` with specified
    // `ns_id` will not be cleaned. We need this method to
    // cleanup all external id ptrs.
    void unregisterNamespace(const Prefix & ns_id);

    // Check whether `ns_id` exist. Expose for testing.
    // Note that the result is meaningless unless `getAliveIds`
    // or `unregisterNamespace` is called to cleanup invalid
    // external ids.
    bool existNamespace(const Prefix & ns_id) const
    {
        std::lock_guard map_guard(mu);
        return ids_by_ns.count(ns_id) > 0;
    }

    DISALLOW_COPY_AND_MOVE(ExternalIdsByNamespace);

private:
    mutable std::mutex mu;
    // Only store weak_ptrs. The weak_ptrs will be invalid after the external id
    // in PageDirectory get removed.
    using ExternalIds = std::list<std::weak_ptr<PageId>>;
    using NamespaceMap = std::unordered_map<Prefix, ExternalIds>;
    mutable NamespaceMap ids_by_ns;
};

namespace u128
{
struct ExternalIdTrait
{
    using U64PageId = DB::PageId;
    using PageId = PageIdV3Internal;
    using Prefix = NamespaceId;

    static inline PageId getInvalidID()
    {
        return buildV3Id(0, DB::INVALID_PAGE_ID);
    }
    static inline U64PageId getU64ID(PageId page_id)
    {
        return page_id.low;
    }
    static inline Prefix getPrefix(const PageId & page_id)
    {
        return page_id.high;
    }
};
} // namespace u128
namespace universal
{
struct ExternalIdTrait
{
    using U64PageId = DB::PageId;
    using PageId = UniversalPageId;
    using Prefix = String;

    static inline PageId getInvalidID()
    {
        return "";
    }
    static inline U64PageId getU64ID(const PageId & /*page_id*/)
    {
        // FIXME: we need to ignore some page_id with prefix
        return 0;
    }
    static inline Prefix getPrefix(const PageId & /*page_id*/)
    {
        // FIXME
        return "";
    }
};
} // namespace universal

} // namespace DB::PS::V3
