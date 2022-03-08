#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>

namespace DB
{
PageStoragePtr PageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const PageStorage::Config & config,
    const FileProviderPtr & file_provider,
    bool use_v3)
{
    if (use_v3)
        return std::make_shared<PS::V3::PageStorageImpl>(name, delegator, config, file_provider);
    else
        return std::make_shared<PS::V2::PageStorage>(name, delegator, config, file_provider);
}

} // namespace DB
