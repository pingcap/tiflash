#pragma once

#include <forward_list>

#include <IO/ReadBuffer.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{

class WriteBatch
{
public:
    struct Write
    {
        bool          is_put;
        PageId        page_id;
        UInt64        version;
        ReadBufferPtr read_buffer;
        UInt32        size;
    };
    using Writes = std::vector<Write>;

public:
    void putPage(PageId page_id, UInt64 version, ReadBufferPtr read_buffer, UInt32 size)
    {
        Write w = {true, page_id, version, read_buffer, size};
        writes.push_back(w);
    }
    void delPage(PageId page_id)
    {
        Write w = {false, page_id, 0, {}, 0};
        writes.push_back(w);
    }
    const Writes & getWrites() const { return writes; }

private:
    Writes writes;
};

} // namespace DB