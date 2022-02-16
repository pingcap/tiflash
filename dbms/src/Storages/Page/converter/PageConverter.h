#pragma once

#include <PageConverterOptions.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>

namespace DB
{
using PageStorageV2 = PS::V2::PageStorage;
using PageStorageV3 = PS::V3::PageStorageImpl;
using PageFile = PS::V2::PageFile;
using PageEntriesEditV2 = PS::V2::PageEntriesEdit;
class PageConverter
{
public:
    PageConverter(FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_, PageConverterOptions & options_);

    void convertV2toV3();

private:
    void packV2data();

    void cleanV2data();

    std::pair<PageEntriesEditV2, std::vector<PageEntriesEditV2>> readFromV2();

    void writeIntoV3(const PageEntriesEditV2 & edit);

private:
    PSDiskDelegatorPtr delegator;
    FileProviderPtr file_provider;
    PageConverterOptions options;

    LogWithPrefixPtr log;
};

} // namespace DB