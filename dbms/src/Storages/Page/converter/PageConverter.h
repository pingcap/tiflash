#pragma once

#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>

namespace DB
{
using PageStorageV2 = PS::V2::PageStorage;
using PageStorageV3 = PS::V3::PageStorageImpl;
using PageFile = PS::V2::PageFile;

class PageConverter
{
public:
    PageConverter(FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_);

    void readFromV2();

private:
    PSDiskDelegatorPtr delegator;
    FileProviderPtr file_provider;
    LogWithPrefixPtr log;
};

} // namespace DB