#include <Common/SyncPoint/SyncPoint.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdsByNamespace.h>

#include <mutex>
#include <optional>

namespace DB::PS::V3
{
void ExternalIdsByNamespace::addExternalIdUnlock(const std::shared_ptr<PageIdV3Internal> & external_id)
{
    const NamespaceId & ns_id = external_id->high;
    auto [ns_iter, new_inserted] = ids_by_ns.emplace(ns_id, ExternalIds{});
    ns_iter->second.external_ids.emplace_back(std::weak_ptr<PageIdV3Internal>(external_id));
}

void ExternalIdsByNamespace::addExternalId(const std::shared_ptr<PageIdV3Internal> & external_id)
{
    auto [ns_iter, ns_guard] = [this, &external_id]() {
        std::unique_lock map_guard(mu);
        const NamespaceId & ns_id = external_id->high;
        // create a new ExternalIds if the ns_id is not exists, else return
        // the existing one.
        auto [ns_iter, new_inserted] = ids_by_ns.try_emplace(ns_id, ExternalIds{});
        return std::pair<StableMap::iterator, std::unique_lock<std::mutex>>(ns_iter, ns_iter->second.mu);
    }();

    SYNC_FOR("before_ExternalIdsByNamespace::insert_to_ns");

    ns_iter->second.external_ids.emplace_back(std::weak_ptr<PageIdV3Internal>(external_id));
}

std::set<PageId> ExternalIdsByNamespace::getAliveIds(NamespaceId ns_id) const
{
    std::set<PageId> valid_external_ids;
    {
        auto [exist, ns_iter, ns_guard] = [this, ns_id]() {
            std::unique_lock map_guard(mu);
            auto ns_iter = ids_by_ns.find(ns_id);
            if (ns_iter == ids_by_ns.end())
                return std::make_tuple(false, ns_iter, std::unique_lock<std::mutex>());
            return std::tuple<bool, StableMap::iterator, std::unique_lock<std::mutex>>(true, ns_iter, ns_iter->second.mu);
        }();
        if (!exist)
            return valid_external_ids;

        auto & external_ids = ns_iter->second.external_ids;
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
    } // release ns_guard

    // FIXME: There is a chance that other threads call `addExternalId` before this thread get the map_guard and
    // execute erase.
    if (valid_external_ids.empty())
    {
        std::unique_lock map_guard(mu);
        valid_external_ids.erase(ns_id);
    }
    return valid_external_ids;
}
} // namespace DB::PS::V3
