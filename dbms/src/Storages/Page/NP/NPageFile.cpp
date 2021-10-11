#include "NPageFile.h"

#include "PageSpec.h"
#include "pagemap/BitMap.h"
#include <Poco/File.h>
#include <stdlib.h>
#include <Storages/Page/PageUtil.h>

namespace DB
{

NPageFile::NPageFile(String path_, FileProviderPtr file_provider_) 
    : file_provider{file_provider_}
    , path(path_)
    , log(&Poco::Logger::get("NewPageStorage"))
    , versioned_page_entries("NewPageStorage", version_set_config, log)
{
    if (Poco::File page_file_data(path + PAGE_FILE_DATA); !page_file_data.exists())
    {
        page_file_data.createFile();
    }

    // TBD: add a V3 magic number 
    if (Poco::File page_file_meta(path + PAGE_FILE_META); !page_file_meta.exists())
    {
        page_file_meta.createFile();
    } else {
        meta_position = page_file_meta.getSize();
    }

    // todo try/catch here
    page_map = NPageMap::newPageMap(path, BMAP64_RBTREE, file_provider_);

    data_writer = file_provider->newWritableFile(
        path + PAGE_FILE_DATA,
        EncryptionPath(path + PAGE_FILE_DATA, ""),
        false,
        false);

    meta_writer = file_provider->newWritableFile(
        path + PAGE_FILE_META,
        EncryptionPath(path + PAGE_FILE_META, ""),
        false,
        false);

    data_reader = file_provider->newRandomAccessFile(
        path + PAGE_FILE_DATA,
        EncryptionPath(path + PAGE_FILE_DATA, "")
    );

    meta_reader = file_provider->newRandomAccessFile(
        path + PAGE_FILE_META,
        EncryptionPath(path + PAGE_FILE_META, "")
    );

    DB::MVCC::PageEntityRmHandler handler = [this](const DB::PageEntry & entry) {
        if (entry.size != 0)
            this->page_map->unmarkDataRange(entry.offset, entry.size);
    };

    versioned_page_entries.regPageEntityRmHandler(handler);
};

void NPageFile::restore()
{
    PageEntriesEdit edit;
    edit = page_map->restore();
    versioned_page_entries.apply(edit);
}

void NPageFile::write(WriteBatch && write_batch)
{
    PageEntriesEdit edit;
    size_t meta_write_bytes = sizeof(PFMetaHeader);
    auto & writes = write_batch.getWrites();
    size_t write_batch_size = writes.size();

    if (write_batch_size == 0 )
    {
        return;
    }
    write_batch.setSequence(++write_batch_seq);

    // Read and get offset.
    UInt64 data_sizes[write_batch_size];
    UInt64 offsets_in_file[write_batch_size];
    UInt64 total_data_sizes = 0;
    size_t index = 0;
    for (const auto & write : writes)
    {
        total_data_sizes += write.size;
        data_sizes[index++] = write.size;
        if (write.type == WriteBatch::WriteType::PUT || write.type == WriteBatch::WriteType::UPSERT){
            meta_write_bytes += write.offsets.size() * sizeof(PFMetaFieldOffset);
        }

        meta_write_bytes += sizeof(PFMeta);
    }

    // This will multi-io , so give up different offset, just let it combine to big io.
    /* page_map->getDataRange(data_sizes, write_batch_size, offsets_in_file, true); */

    auto offset_begin = page_map->getDataRange(total_data_sizes, true);
    if (offset_begin != UINT64_MAX){
        page_map->splitDataInRange(data_sizes, write_batch_size, offsets_in_file, offset_begin, total_data_sizes);
    }

    char * meta_buffer = (char *)alloc(meta_write_bytes);
    char * data_buffer = (char *)alloc(total_data_sizes);
    SCOPE_EXIT({
        free(meta_buffer,meta_write_bytes);
        free(data_buffer,total_data_sizes);
    });

    char * meta_pos = meta_buffer;
    auto header = PageUtil::cast<PFMetaHeader>(meta_pos); 
    header->bits.meta_byte_size = meta_write_bytes;
    header->bits.meta_seq_id = write_batch.getSequence();
    // TBD CRC
    header->bits.page_checksum = 0;

    PageEntry entry;
    index = 0;
    for (auto & write : writes)
    {
        auto meta = PageUtil::cast<PFMeta>(meta_pos); 
        meta->pu.type = (UInt8)write.type;
        switch (write.type)
        {
            case WriteBatch::WriteType::PUT:
            case WriteBatch::WriteType::UPSERT:{
                meta->pu.page_id = write.page_id;
                meta->pu.page_offset = offsets_in_file[index];
                meta->pu.page_size = write.size;
                meta->pu.field_offset_len = write.offsets.size();
        
                entry.offset = meta->pu.page_offset;
                entry.size = write.size;
                entry.field_offsets.swap(write.offsets);

                for (size_t i = 0; i < entry.field_offsets.size(); ++i)
                {
                    auto field_offset = PageUtil::cast<PFMetaFieldOffset>(meta_pos);
                    field_offset->bits.field_offset = (UInt64)entry.field_offsets[i].first;
                }

                if (write.type == WriteBatch::WriteType::PUT)
                    edit.put(write.page_id, entry);
                else if (write.type == WriteBatch::WriteType::UPSERT)
                    edit.upsertPage(write.page_id, entry);
                break;
            }
            case WriteBatch::WriteType::DEL:{
                meta->del.page_id = write.page_id;
                edit.del(write.page_id);
                break;
            }
            case WriteBatch::WriteType::REF: {
                meta->ref.page_id = write.page_id;
                meta->ref.og_page_id = write.ori_page_id;
                edit.ref(write.page_id, write.ori_page_id);
                break;
            }
        }
        index++;
    }

    for (size_t j = 0 ; j < write_batch_size ; j++){
        // std::cout << "buffer_need_write.size() : " << buffer_need_write.size() << std::endl;
        std::cout << "offsets_in_file[j] : " << offsets_in_file[j] << std::endl;
    }

    // FIXME: should mark pagemap after written
    if (offset_begin != UINT64_MAX)
    {
        UInt64 range_move = 0;
        for (size_t i = 0; i < write_batch_size; i++)
        {
            if (data_sizes[i] != 0)
            {
                // auto & buffer_need_write = writes[i].read_buffer->buffer();
                
                // Still need copy , because need compact data into single IO.
                // it depends on we used un-align buffer.
                // memcpy(data_buffer + range_move,buffer_need_write.begin(),buffer_need_write.size());
                writes[i].read_buffer->readStrict(data_buffer + range_move, writes[i].size);
                range_move += data_sizes[i];
            }
        }

        // TBD: maybe we can use a fixed memory store.
        // Then we won't need alloc data memory and it can make buffer bigger.
        PageUtil::writeFile(data_writer, offset_begin, data_buffer, total_data_sizes, nullptr, false);
        PageUtil::syncFile(data_writer);
    }

    PageUtil::writeFile(meta_writer, meta_position, meta_buffer, meta_write_bytes, nullptr, false);
    PageUtil::syncFile(meta_writer);
    meta_position += meta_write_bytes;

    versioned_page_entries.apply(edit);
}

String NPageFile::getPath()
{
    return path;
}


void NPageFile::read(PageIdAndEntries & to_read, const PageHandler & handler)
{
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndEntry & a, const PageIdAndEntry & b) { 
            return a.second.offset < b.second.offset;
    });

    size_t buf_size = 0;
    for (const auto & p : to_read)
        buf_size = std::max(buf_size, p.second.size);

    char * data_buf = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    auto it = to_read.begin();
    while (it != to_read.end())
    {
        auto && [page_id, entry] = *it;

        PageUtil::readFile(data_reader, entry.offset, data_buf, entry.size, nullptr);

        // TODO CRC
        Page page;
        page.page_id = page_id;
        page.data = ByteBuffer(data_buf, data_buf + entry.size);
        page.mem_holder = mem_holder;

        ++it;
        handler(page_id, page);
    }
}


void NPageFile::read(const std::vector<PageId> & page_ids, const PageHandler & handler, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    PageIdAndEntries to_read;
    for (auto page_id : page_ids)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (!page_entry)
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        to_read.emplace_back(page_id, *page_entry);
    }

    read(to_read, handler);

}

void NPageFile::read(const PageId & page_id, const PageHandler & handler, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    const auto page_entry = snapshot->version()->find(page_id);
    if (!page_entry)
        throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
    PageIdAndEntries to_read = {{page_id, *page_entry}};
    read(to_read, handler);
}

}
