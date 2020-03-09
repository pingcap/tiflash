#pragma once

#include <IO/ReadBuffer.h>
#include <Storages/Page/PageDefines.h>

#include <vector>

namespace DB
{

class WriteBatch : private boost::noncopyable
{
public:
    enum class WriteType : UInt8
    {
        DEL = 0,
        // Create / Update a page, will implicitly create a RefPage{id} -> Page{id}.
        PUT = 1,
        // Create a RefPage{ref_id} -> Page{id}
        REF = 2,
        // Create or update a Page. Now only used by GC.
        // Compare to `PUT`, this type won't create the RefPage{id} -> Page{id} by default.
        UPSERT = 3,
    };

    using SequenceID = UInt64;

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
        // Fields' offset inside Page's data
        PageFieldOffsetChecksums offsets;

        /// The meta and data may not be the same PageFile, (read_buffer == nullptr)
        /// use `target_file_id`, `page_offset`, `page_checksum` to indicate where
        /// data is actually store in.
        /// Should only use by `UPSERT` now.

        UInt64             page_offset;
        UInt64             page_checksum;
        PageFileIdAndLevel target_file_id;
    };
    using Writes = std::vector<Write>;

public:
    void putPage(PageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size, const PageFieldSizes & data_sizes = {})
    {
        // Conver from data_sizes to the offset of each field
        PageFieldOffsetChecksums offsets;
        PageFieldOffset          off = 0;
        for (auto data_sz : data_sizes)
        {
            offsets.emplace_back(off, 0);
            off += data_sz;
        }
        if (unlikely(!data_sizes.empty() && off != size))
            throw Exception("Try to put Page" + DB::toString(page_id) + " with " + DB::toString(data_sizes.size())
                                + " fields, but page size and filelds total size not match, page_size: " + DB::toString(size)
                                + ", all fields size: " + DB::toString(off),
                            ErrorCodes::LOGICAL_ERROR);

        Write w{WriteType::PUT, page_id, tag, read_buffer, size, 0, std::move(offsets), 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void putExternal(PageId page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w{WriteType::PUT, page_id, tag, nullptr, 0, 0, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void upsertPage(PageId                           page_id,
                    UInt64                           tag,
                    const PageFileIdAndLevel &       file_id,
                    const ReadBufferPtr &            read_buffer,
                    UInt32                           size,
                    const PageFieldOffsetChecksums & offsets)
    {
        Write w{WriteType::UPSERT, page_id, tag, read_buffer, size, 0, offsets, 0, 0, file_id};
        writes.emplace_back(std::move(w));
    }

    void upsertPage(PageId                           page_id,
                    UInt64                           tag,
                    const PageFileIdAndLevel &       file_id,
                    UInt64                           page_offset,
                    UInt32                           size,
                    UInt64                           page_checksum,
                    const PageFieldOffsetChecksums & offsets)
    {
        Write w{WriteType::UPSERT, page_id, tag, nullptr, size, 0, offsets, page_offset, page_checksum, file_id};
        writes.emplace_back(std::move(w));
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageId ref_id, PageId page_id)
    {
        Write w{WriteType::REF, ref_id, 0, nullptr, 0, page_id, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void delPage(PageId page_id)
    {
        Write w{WriteType::DEL, page_id, 0, nullptr, 0, 0, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
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

    void swap(WriteBatch & o)
    {
        writes.swap(o.writes);
        o.sequence = sequence;
    }

    void clear()
    {
        Writes tmp;
        writes.swap(tmp);
        sequence = 0;
    }

    SequenceID getSequence() const { return sequence; }


    // `setSequence` should only called by internal method of PageStorage.
    void setSequence(SequenceID sequence_) { sequence = sequence_; }

    WriteBatch() = default;
    WriteBatch(WriteBatch && rhs) : writes(std::move(rhs.writes)), sequence(rhs.sequence) {}

private:
    Writes     writes;
    SequenceID sequence = 0;
};

} // namespace DB
