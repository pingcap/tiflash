#include <algorithm>

#include <Storages/Page/V3/Remote/CheckpointPageManager.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

namespace DB::PS::V3
{
using PS::V3::CheckpointManifestFileReader;
using PS::V3::PageDirectory;
using PS::V3::RemoteDataLocation;
using PS::V3::Remote::WriterInfo;
using PS::V3::universal::BlobStoreTrait;
using PS::V3::universal::PageDirectoryTrait;

static std::atomic<int64_t> local_ps_num = 0;

UniversalPageStoragePtr CheckpointPageManager::createTempPageStorage(Context & context, const String & checkpoint_manifest_path, const String & data_dir)
{
    RUNTIME_CHECK(endsWith(data_dir, "/"));
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    auto num = local_ps_num.fetch_add(1, std::memory_order_relaxed);
    auto local_ps = UniversalPageStorage::create( //
        "local",
        context.getPathPool().getPSDiskDelegatorGlobalMulti(fmt::format("local_{}", num)),
        config,
        file_provider);
    local_ps->restore();

    auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{
        .file_path = checkpoint_manifest_path
    });
    auto t_edit = reader->read();
    const auto & records = t_edit.getRecords();
    UniversalWriteBatch wb;
    for (const auto & record: records)
    {
        if (record.type == EditRecordType::VAR_ENTRY)
        {
            const auto & location = record.entry.remote_info->data_location;
            auto buf = std::make_shared<ReadBufferFromFile>(data_dir + *location.data_file_id);
            buf->seek(location.offset_in_file);
            wb.putPage(record.page_id, record.entry.tag, buf, location.size_in_file, Page::fieldOffsetsToSizes(record.entry.field_offsets, location.size_in_file));
        }
        else if (record.type == EditRecordType::VAR_REF)
        {
            wb.putRefPage(record.page_id, record.ori_page_id);
        }
        else if (record.type == EditRecordType::VAR_DELETE)
        {
            wb.delPage(record.page_id);
        }
        else if (record.type == EditRecordType::VAR_EXTERNAL)
        {
            wb.putExternal(record.page_id, record.entry.tag);
        }
        else
        {
            RUNTIME_CHECK_MSG(false, fmt::format("Unknown record type {}", typeToString(record.type)));
        }
    }
    local_ps->write(std::move(wb));
    return local_ps;
}
}
