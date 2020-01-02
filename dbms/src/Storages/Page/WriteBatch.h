#pragma once

#include <vector>

#include <IO/ReadBuffer.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{

class WriteBatch
{
public:
    enum class WriteType : UInt8
    {
        DEL = 0,
        // Create / Update a page, will implicitly create a RefPage{id} -> Page{id}.
        PUT = 1,
        // Create a RefPage{ref_id} -> Page{id}
        REF = 2,
        // Move an exist normal page to new PageFile. Now only used by GC.
        // Compare to `PUT`, this type won't create the RefPage{id} -> Page{id} by default.
        MOVE_NORMAL_PAGE = 3,
        // Ingest an exists normal page, the concrete meta will be apply soon by MOVE_NORMAL_PAGE.
        INGEST           = 4,
    };

private:
    struct Write
    {
        WriteType type;
        PageId    page_id;
        UInt64    tag;
        // Page's data and size
        ReadBufferPtr read_buffer;
        PageSize      size;
        // RefPage's origin page
        PageId ori_page_id;
    };
    using Writes = std::vector<Write>;

public:
    void putPage(PageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size)
    {
        Write w = {WriteType::PUT, page_id, tag, read_buffer, size, 0};
        writes.emplace_back(w);
    }

    void ingestPage(PageId page_id, UInt64 tag)
    {
        Write w = {WriteType::INGEST, page_id, tag, nullptr, 0, 0};
        writes.emplace_back(w);
    }

    void putExternal(PageId page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w = {WriteType::PUT, page_id, tag, nullptr, 0, 0};
        writes.emplace_back(w);
    }

    void gcMovePage(PageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, UInt32 size)
    {
        Write w = {WriteType::MOVE_NORMAL_PAGE, page_id, tag, read_buffer, size, 0};
        writes.emplace_back(w);
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageId ref_id, PageId page_id)
    {
        Write w = {WriteType::REF, ref_id, 0, {}, 0, page_id};
        writes.emplace_back(w);
    }

    void delPage(PageId page_id)
    {
        Write w = {WriteType::DEL, page_id, 0, {}, 0, 0};
        writes.emplace_back(w);
    }

    bool empty() const { return writes.empty(); }

    const Writes & getWrites() const { return writes; }

    size_t putWriteCount() const
    {
        size_t count = 0;
        for (auto & w : writes)
            count += (w.type == WriteType::PUT);
        return count;
    }

    void swap(WriteBatch & o) { writes.swap(o.writes); }

    void clear()
    {
        Writes tmp;
        writes.swap(tmp);
    }

private:
    Writes writes;
};

} // namespace DB
