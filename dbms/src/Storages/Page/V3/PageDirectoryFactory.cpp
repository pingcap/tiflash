#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WALStore.h>

namespace DB::PS::V3
{

PageDirectoryPtr PageDirectoryFactory::create(FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator)
{
    auto [wal, reader] = WALStore::create(file_provider, delegator);
    PageDirectoryPtr dir = std::make_unique<PageDirectory>(std::move(wal));
    loadFromDisk(dir, std::move(reader));
    // TODO: After restored ends, set the last offset of log file for `wal`
    if (blob_stats)
        blob_stats->restore();
    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;
    return dir;
}

void PageDirectoryFactory::loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader)
{
    PageDirectory::MVCCMapType & mvcc_table_directory = dir->mvcc_table_directory;
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        if (!ok)
        {
            // TODO: Handle error, some error could be ignored.
            // If the file happened to some error,
            // should truncate it to throw away incomplete data.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }

        // apply the edit read
        for (const auto & r : edit.getRecords())
        {
            if (max_applied_ver < r.version)
                max_applied_ver = r.version;
            max_applied_page_id = std::max(r.page_id, max_applied_page_id);

            auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
            if (created)
            {
                iter->second = std::make_shared<VersionedPageEntries>();
            }

            const auto & version_list = iter->second;
            const auto & restored_version = r.version;
            try
            {
                switch (r.type)
                {
                case EditRecordType::VAR_EXTERNAL:
                case EditRecordType::VAR_REF:
                    version_list->fromRestored(r);
                    break;
                case EditRecordType::VAR_ENTRY:
                    version_list->fromRestored(r);
                    if (blob_stats)
                        blob_stats->restoreByEntry(r.entry);
                    break;
                case EditRecordType::PUT_EXTERNAL:
                    version_list->createNewExternal(restored_version);
                    break;
                case EditRecordType::PUT:
                    version_list->createNewEntry(restored_version, r.entry);
                    if (blob_stats)
                        blob_stats->restoreByEntry(r.entry);
                    break;
                case EditRecordType::DEL:
                case EditRecordType::VAR_DELETE: // nothing different from `DEL`
                    version_list->createDelete(restored_version);
                    break;
                case EditRecordType::REF:
                    PageDirectory::applyRefEditRecord(mvcc_table_directory, version_list, r, restored_version);
                    break;
                case EditRecordType::UPSERT:
                    version_list->createNewEntry(restored_version, r.entry);
                    if (blob_stats)
                        blob_stats->restoreByEntry(r.entry);
                    break;
                }
            }
            catch (DB::Exception & e)
            {
                e.addMessage(fmt::format(" [type={}] [page_id={}] [ver={}]", r.type, r.page_id, restored_version));
                throw e;
            }
        }
    }
}
} // namespace DB::PS::V3
