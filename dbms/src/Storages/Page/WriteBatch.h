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
        PUT = 1,
        REF = 2,
    };

private:
    struct Write
    {
        WriteType type;
        PageId    page_id;
        UInt64    tag;
        // Page's data and size
        ReadBufferPtr read_buffer;
        UInt32        size;
        // RefPage's origin page
        PageId ori_page_id;
    };
    using Writes = std::vector<Write>;

public:
    void putPage(PageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, UInt32 size)
    {
        Write w = {WriteType::PUT, page_id, tag, read_buffer, size, 0};
        writes.emplace_back(w);
    }

    void putRefPage(PageId page_id, PageId ori_page_id)
    {
        Write w = {WriteType::REF, page_id, 0, {}, 0, ori_page_id};
        writes.emplace_back(w);
    }

    void delPage(PageId page_id)
    {
        Write w = {WriteType::DEL, page_id, 0, {}, 0, 0};
        writes.emplace_back(w);
    }
    const Writes & getWrites() const { return writes; }

private:
    Writes writes;
};

} // namespace DB