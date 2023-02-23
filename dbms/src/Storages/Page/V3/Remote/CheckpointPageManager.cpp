#include <Storages/Page/V3/Remote/CheckpointPageManager.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

#include <algorithm>

namespace DB::PS::V3
{
using PS::V3::CheckpointManifestFileReader;
using PS::V3::universal::PageDirectoryTrait;

static std::atomic<int64_t> local_ps_num = 0;

UniversalPageStoragePtr CheckpointPageManager::createTempPageStorage(Context & context, const String & checkpoint_manifest_path, const String & data_dir)
{
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    auto num = local_ps_num.fetch_add(1, std::memory_order_relaxed);
    auto local_ps = UniversalPageStorage::create( //
        "local",
        context.getPathPool().getPSDiskDelegatorGlobalMulti(fmt::format("local_{}", num)),
        config,
        data_dir,
        file_provider);
    local_ps->restore();

    auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{
        .file_path = checkpoint_manifest_path});
    auto t_edit = reader->read();
    auto & records = t_edit.getMutRecords();
    UniversalWriteBatch wb;
    // insert delete records at last
    PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    for (auto & record : records)
    {
        if (record.type == EditRecordType::VAR_ENTRY)
        {
            wb.putRemotePage(record.page_id, record.entry.tag, record.entry.remote_info->data_location, std::move(record.entry.field_offsets));
        }
        else if (record.type == EditRecordType::VAR_REF)
        {
            ref_records.emplace_back(record);
        }
        else if (record.type == EditRecordType::VAR_DELETE)
        {
            delete_records.emplace_back(record);
        }
        else if (record.type == EditRecordType::VAR_EXTERNAL)
        {
            wb.putExternal(record.page_id, record.entry.tag);
        }
        else
        {
            std::cout << "Unknown record type" << std::endl;
            RUNTIME_CHECK_MSG(false, fmt::format("Unknown record type {}", typeToString(record.type)));
        }
    }
    for (const auto & record : ref_records)
    {
        RUNTIME_CHECK(record.type == EditRecordType::VAR_REF);
        wb.putRefPage(record.page_id, record.ori_page_id);
    }
    for (const auto & record : delete_records)
    {
        RUNTIME_CHECK(record.type == EditRecordType::VAR_DELETE);
        wb.delPage(record.page_id);
    }
    local_ps->write(std::move(wb));
    LOG_DEBUG(&Poco::Logger::get("CheckpointPageManager"), "write to local ps done");
    return local_ps;
}
} // namespace DB::PS::V3
