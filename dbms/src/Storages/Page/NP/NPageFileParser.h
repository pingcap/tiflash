#pragma once

#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/PageUtil.h>
#include "PageSpec.h"

namespace DB
{

class NPageFileParser 
{
public:
    NPageFileParser(PageEntriesEdit * edit_, char * buffer_, UInt64 size) :buffer(buffer_), buffer_size(size), curr_edit(edit_) {};

    virtual ~NPageFileParser(){};

    virtual void parse()
    {
        pos = buffer;
        while(pos < buffer + buffer_size)
        {
            parserHeader();
        }
    }

    virtual void applyPU(PFMeta * meta, PageEntry & entry)
    {
        if (meta->pu.type == 1)
        {
            curr_edit->put(meta->pu.page_id,entry);
        } 
        else 
        {
            curr_edit->upsertPage(meta->pu.page_id,entry);
        }
    }

    virtual void applyDel(PFMeta * meta)
    {
        curr_edit->del(meta->del.page_id);
    }

    virtual void applyRef(PFMeta * meta)
    {
        curr_edit->ref(meta->ref.page_id,meta->ref.og_page_id);
    }

    virtual void dealPU(PFMeta * meta)
    {
        PageEntry entry;
        entry.offset = meta->pu.page_offset;
        entry.size = meta->pu.page_size;
        for (size_t i = 0 ; i < meta->pu.field_offset_len ; i++)
        {
            auto field_offset = PageUtil::cast<PFMetaFieldOffset>(pos);
            entry.field_offsets.emplace_back(field_offset->bits.field_offset, 0);
        }
        applyPU(meta, entry);
    }

    virtual void dealDel(PFMeta * meta)
    {
        applyDel(meta);
    }

    virtual void dealRef(PFMeta * meta)
    {
        applyRef(meta);
    }

    virtual void parseWriteBatch()
    {
        auto meta = PageUtil::cast<PFMeta>(pos);
        switch((WriteBatch::WriteType)meta->pu.type) 
        {
            case WriteBatch::WriteType::PUT:
            case WriteBatch::WriteType::UPSERT: {
                dealPU(meta);
                break;
            }
            case WriteBatch::WriteType::DEL: {
                dealDel(meta);
                break;
            }
            case WriteBatch::WriteType::REF: {
                dealRef(meta);
                break;
            }
            default:
                // TBD:
                return;
        }
    }
    
    virtual void parserHeader()
    {
        char * curr_pos = pos;

        auto meta_header = PageUtil::cast<PFMetaHeader>(pos);
        size_t meta_size = meta_header->bits.meta_byte_size;
        if (meta_size > buffer_size)
        {
            // TBD: deal this situation
            return;
        }
        UInt64 meta_crc = meta_header->bits.page_checksum;
        // TBD: do crc check
        (void)meta_crc;
        curr_write_batch_sequence = std::max(curr_write_batch_sequence,meta_header->bits.meta_seq_id);

        while (pos < curr_pos + meta_size)
        {
            parseWriteBatch();
        }
    }


protected:
    char * buffer;
    char * pos;
    UInt64 buffer_size;

    WriteBatch::SequenceID curr_write_batch_sequence = 0;
    PageEntriesEdit * curr_edit;
};


class NPageFileBuilder
{
public:
    NPageFileBuilder(WriteBatch & write_batch_) : write_batch(write_batch_)
    {
        
    };


private:
    char * buffer;
    char * pos;

    UInt64 buffer_size;
    WriteBatch & write_batch;
};

}