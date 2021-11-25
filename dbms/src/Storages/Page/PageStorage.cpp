#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>

namespace DB
{
PageStoragePtr PageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const PageStorage::Config & config,
    const FileProviderPtr & file_provider)
{
    return std::make_shared<PS::V2::PageStorage>(name, delegator, config, file_provider);
}

} // namespace DB
