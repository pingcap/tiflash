#pragma once

#include <optional>

#include <IO/CompressedStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/Page/Page.h>

namespace DB
{
namespace DM
{
static constexpr size_t CHUNK_SERIALIZE_BUFFER_SIZE = 65536;


// TODO: version des/ser
struct ColumnMeta
{
    ColId       col_id;
    PageId      page_id;
    UInt32      rows;
    UInt64      bytes;
    DataTypePtr type;
};
using ColumnMetas = std::vector<ColumnMeta>;

class Chunk
{
public:
    using ColumnMetaMap = std::unordered_map<ColId, ColumnMeta>;

    Chunk(Handle handle_first_, Handle handle_last_) : handle_start(handle_first_), handle_end(handle_last_), is_delete_range(false) {}
    explicit Chunk(const HandleRange & delete_range) : handle_start(delete_range.start), handle_end(delete_range.end), is_delete_range(true)
    {
    }

    bool        isDeleteRange() const { return is_delete_range; }
    HandleRange getDeleteRange() const
    {
        if (!is_delete_range)
            throw Exception("Not a delete range");
        return {handle_start, handle_end};
    }

    std::pair<Handle, Handle> getHandleFirstLast() const
    {
        if (is_delete_range)
            throw Exception("It is a delete range");
        return {handle_start, handle_end};
    }

    size_t getRows() const { return rows; }

    UInt64 getBytes() const
    {
        UInt64 bytes = 0;
        for (const auto & p : columns)
            bytes += p.second.bytes;
        return bytes;
    }

    const ColumnMeta & getColumn(ColId col_id) const
    {
        auto it = columns.find(col_id);
        if (unlikely(it == columns.end()))
            throw Exception("Column with id" + DB::toString(col_id) + " not found");
        return it->second;
    }

    const ColumnMetaMap & getMetas() const { return columns; }

    void insert(const ColumnMeta & c)
    {
        if (isDeleteRange())
            throw Exception("Insert column into delete range chunk is not allowed");
        columns[c.col_id] = c;
        if (rows && rows != c.rows)
            throw Exception("Rows not match");
        else
            rows = c.rows;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeIntBinary(handle_start, buf);
        writeIntBinary(handle_end, buf);
        writePODBinary(is_delete_range, buf);
        writeIntBinary((UInt64)columns.size(), buf);
        for (const auto & [col_id, d] : columns)
        {
            writeIntBinary(col_id, buf);
            writeIntBinary(d.page_id, buf);
            writeIntBinary(d.rows, buf);
            writeIntBinary(d.bytes, buf);
            writeStringBinary(d.type->getName(), buf);
        }
    }

    static Chunk deserialize(ReadBuffer & buf)
    {
        Handle start, end;
        readIntBinary(start, buf);
        readIntBinary(end, buf);

        Chunk chunk(start, end);

        readPODBinary(chunk.is_delete_range, buf);
        UInt64 col_size;
        readIntBinary(col_size, buf);
        chunk.columns.reserve(col_size);
        for (UInt64 ci = 0; ci < col_size; ++ci)
        {
            ColumnMeta d;
            String     type;
            readIntBinary(d.col_id, buf);
            readIntBinary(d.page_id, buf);
            readIntBinary(d.rows, buf);
            readIntBinary(d.bytes, buf);
            readStringBinary(type, buf);

            d.type = DataTypeFactory::instance().get(type);

            chunk.columns.emplace(d.col_id, d);

            if (chunk.rows && chunk.rows != d.rows)
                throw Exception("Rows not match");
            else
                chunk.rows = d.rows;
        }
        return chunk;
    }

private:
    Handle        handle_start;
    Handle        handle_end;
    bool          is_delete_range;
    ColumnMetaMap columns;
    size_t        rows = 0;
};

using Chunks = std::vector<Chunk>;

inline void serializeChunks(WriteBuffer & buf, Chunks::const_iterator begin, Chunks ::const_iterator end, std::optional<Chunk> extra_chunk)
{
    UInt64 size = extra_chunk.has_value() ? (UInt64)(end - begin) + 1 : (UInt64)(end - begin);
    writeIntBinary(size, buf);
    for (; begin != end; ++begin)
        (*begin).serialize(buf);
    if (extra_chunk)
        extra_chunk->serialize(buf);
}

inline Chunks deserializeChunks(ReadBuffer & buf)
{
    Chunks chunks;
    UInt64 size;
    readIntBinary(size, buf);
    for (UInt64 i = 0; i < size; ++i)
        chunks.push_back(Chunk::deserialize(buf));
    return chunks;
}

} // namespace DM
} // namespace DB