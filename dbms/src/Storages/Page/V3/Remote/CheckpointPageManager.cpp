#include <Storages/Page/V3/Remote/CheckpointPageManager.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

#include <algorithm>
#include "Storages/Page/RemoteDataLocation.h"

namespace DB::PS::V3
{
using PS::V3::CheckpointManifestFileReader;
using PS::V3::universal::PageDirectoryTrait;

static std::atomic<int64_t> local_ps_num = 0;

UniversalPageStoragePtr CheckpointPageManager::createTempPageStorage(Context & context, const String & checkpoint_manifest_path, const String & data_dir)
{
    UNUSED(data_dir);
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
        .file_path = checkpoint_manifest_path});
    auto t_edit = reader->read();
    const auto & records = t_edit.getRecords();
    UniversalWriteBatch wb;
    // insert delete records at last
    PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    for (const auto & record : records)
    {
        if (record.type == EditRecordType::VAR_ENTRY)
        {
            const auto & location = record.entry.remote_info->data_location;
            MemoryWriteBuffer buf;
            writeStringBinary(*location.data_file_id, buf);
            writeIntBinary(location.offset_in_file, buf);
            writeIntBinary(location.size_in_file, buf);
            auto field_sizes = Page::fieldOffsetsToSizes(record.entry.field_offsets, location.size_in_file);
            writeIntBinary(field_sizes.size(), buf);
            for (size_t i = 0; i < field_sizes.size(); i++)
            {
                writeIntBinary(field_sizes[i], buf);
            }
            wb.putPage(record.page_id, record.entry.tag, buf.tryGetReadBuffer(), buf.count());
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
            // TODO: set the remote location
            auto remote_loc = RemoteDataLocation {
            };
            wb.putRemoteExternal(record.page_id, record.entry.tag, remote_loc);
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
    return local_ps;
}

std::tuple<ReadBufferPtr, size_t, PageFieldSizes> CheckpointPageManager::getReadBuffer(const Page & page, const String & data_dir)
{
    RUNTIME_CHECK(page.isValid());
    RUNTIME_CHECK(endsWith(data_dir, "/"));
    String data_path;
    UInt64 offset;
    UInt64 size;
    PageFieldSizes field_sizes;
    {
        ReadBufferFromMemory page_buf(page.data.begin(), page.data.size());
        readStringBinary(data_path, page_buf);
        readIntBinary(offset, page_buf);
        readIntBinary(size, page_buf);
        UInt64 field_count;
        readIntBinary(field_count, page_buf);
        for (size_t i = 0; i < field_count; i++)
        {
            UInt64 f_size;
            readIntBinary(f_size, page_buf);
            field_sizes.push_back(f_size);
        }
        RUNTIME_CHECK(page_buf.eof());
    }
    auto buf = std::make_shared<ReadBufferFromFile>(data_dir + data_path);
    buf->seek(offset);
    return std::make_tuple(buf, size, field_sizes);
}
} // namespace DB::PS::V3
