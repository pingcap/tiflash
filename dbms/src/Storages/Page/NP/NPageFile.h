#pragma once

#include "pagemap/PageMap.h"
#include <Encryption/FileProvider.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSetWithDelta.h>

namespace DB
{

#define PAGE_FILE_DATA "page_file_data"
#define PAGE_FILE_META "page_file_meta"


class NPageFileFlat
{
public:

    enum FlatTypes 
    {
        TRUNCATE_FILE_END = 1,
        SEQUENCE_AT_END = 2,
    };

    class NPageFileFlatRules : Allocator<false>
    {
    public:
        bool measure();
        void flat();
        void getType();
    private:
        int type;
    };

    NPageFileFlat(NPageMapPtr page_map_);

private:
    std::vector<NPageFileFlatRules> rules;
    NPageMapPtr page_map;
};

class NPageFile : Allocator<false>
{
    using VersionedPageEntries = PageEntriesVersionSetWithDelta;
    using SnapshotPtr = VersionedPageEntries::SnapshotPtr;
public:
    NPageFile(String path_, FileProviderPtr file_provider_);

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

    NPageMapPtr page_map;
    String path;

    ::DB::MVCC::VersionSetConfig version_set_config;
    Poco::Logger * log;

    VersionedPageEntries versioned_page_entries;
};

}