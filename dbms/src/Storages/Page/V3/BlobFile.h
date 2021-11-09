#pragma once

#include <Encryption/FileProvider.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSetWithDelta.h>

using namespace DB::PS::V2;
namespace DB::PS::V3
{

#define PAGE_FILE_DATA "page_file_data"
#define PAGE_FILE_META "page_file_meta"

class BlobFile : Allocator<false>
{
    using VersionedPageEntries = PageEntriesVersionSetWithDelta;
    using SnapshotPtr = VersionedPageEntries::SnapshotPtr;
public:
    BlobFile(String path_, FileProviderPtr file_provider_);

    String getPath();

    void read(const std::vector<PageId> & page_ids, const PageHandler & handler, SnapshotPtr snapshot = {});

    void read(const PageId & page_id, const PageHandler & handler, SnapshotPtr snapshot = {});

/* todo
    struct FieldReadInfo
        {
            PageId page_id;
            PageEntry entry;
            std::vector<size_t> fields;

            FieldReadInfo(PageId id_, PageEntry entry_, std::vector<size_t> fields_)
                : page_id(id_)
                , entry(entry_)
                , fields(fields_)
            {}
        };
    using FieldReadInfos = std::vector<FieldReadInfo>;
    PageMap read(FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter = nullptr);
*/
    void write(WriteBatch && write_batch);

    SnapshotPtr getSnapshot()
    {
        return versioned_page_entries.getSnapshot();
    }

    void restore();

private :
    void read(PageIdAndEntries & to_read, const PageHandler & handler);

private:
    std::atomic<WriteBatch::SequenceID> write_batch_seq = 0;

    // meta/data writer reader.
    WritableFilePtr meta_writer;
    WritableFilePtr data_writer;

    UInt64 meta_position = 0;

    RandomAccessFilePtr meta_reader;
    RandomAccessFilePtr data_reader;
    
    FileProviderPtr file_provider;

    SpaceMapPtr space_map;
    String path;

    ::DB::MVCC::VersionSetConfig version_set_config;
    Poco::Logger * log;

    VersionedPageEntries versioned_page_entries;
};

}