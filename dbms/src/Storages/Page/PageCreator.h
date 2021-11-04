#pragma once

#include <Storages/Page/V2/PageStorage.h>

namespace DB
{
class PageCreator
{
public:
    inline static PageStoragePtr createPageStorage(String name,
                                                   PSDiskDelegatorPtr delegator, //
                                                   const PageStorage::Config & config,
                                                   const FileProviderPtr & file_provider)
    {
        return std::make_shared<PS::V2::PageStorage>(name, delegator, config, file_provider);
    }
};
} // namespace DB