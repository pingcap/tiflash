#pragma once

#include <Common/nocopyable.h>
#include <Storages/Page/PageDefines.h>

namespace DB::PS::V3
{

struct ExternalIds
{
    std::mutex mu;
    std::list<std::weak_ptr<PageIdV3Internal>> external_ids;

    ExternalIds() = default;
    ExternalIds(ExternalIds &&rhs): external_ids(std::move(rhs.external_ids)) {}
};

class ExternalIdsByNamespace
{
public:
    ExternalIdsByNamespace() = default;

    void addExternalId(const std::shared_ptr<PageIdV3Internal> & external_id);
    void addExternalIdUnlock(const std::shared_ptr<PageIdV3Internal> & external_id);

    std::set<PageId> getAliveIds(NamespaceId ns_id) const;


    DISALLOW_COPY_AND_MOVE(ExternalIdsByNamespace);

private:
    mutable std::mutex mu;
    using StableMap = std::map<NamespaceId, ExternalIds>;
    mutable StableMap ids_by_ns;
};
} // namespace DB::PS::V3
