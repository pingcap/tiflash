#pragma once

#include <PageConverterOptions.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>

namespace DB
{
using PageStorageV2 = PS::V2::PageStorage;
using PageFile = PS::V2::PageFile;
using PageEntriesEditV2 = PS::V2::PageEntriesEdit;
using EditRecordsV2 = PS::V2::PageEntriesEdit::EditRecords;

using PageStorageV3 = PS::V3::PageStorageImpl;

class PageConverter
{
public:
    PageConverter(FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_, PageConverterOptions & options_);

    void convertV2toV3();

private:
    void packV2data();

    void cleanV2data();

    std::pair<PageEntriesEditV2, std::vector<PageEntriesEditV2>> readV2meta();

    WriteBatch record2WriteBatch(const EditRecordsV2 & record);

    char * readV2data(const PageEntry & entry);

    void writeIntoV3(const PageEntriesEditV2 & edit, const std::vector<PageEntriesEditV2> & edits);

private:
    PSDiskDelegatorPtr delegator;
    FileProviderPtr file_provider;
    PageConverterOptions options;

    LogWithPrefixPtr log;
};

} // namespace DB