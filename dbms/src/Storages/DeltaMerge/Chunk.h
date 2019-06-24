#pragma once

#include <optional>

#include <IO/CompressedStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/Page/PageStorage.h>


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

    HandlePair getHandleFirstLast() const
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

    void         serialize(WriteBuffer & buf) const;
    static Chunk deserialize(ReadBuffer & buf);

private:
    Handle        handle_start;
    Handle        handle_end;
    bool          is_delete_range;
    ColumnMetaMap columns;
    size_t        rows = 0;
};

using Chunks    = std::vector<Chunk>;
using GenPageId = std::function<PageId()>;

void   serializeChunks(WriteBuffer & buf, Chunks::const_iterator begin, Chunks ::const_iterator end, std::optional<Chunk> extra_chunk);
Chunks deserializeChunks(ReadBuffer & buf);


using BufferAndSize = std::pair<ReadBufferPtr, size_t>;
BufferAndSize serializeColumn(const IColumn & column, const DataTypePtr & type, size_t offset, size_t num, bool compress);
Chunk         prepareChunkDataWrite(const DMContext & dm_context, const GenPageId & gen_data_page_id, WriteBatch & wb, const Block & block);

void deserializeColumn(IColumn & column, const ColumnMeta & meta, const Page & page, size_t rows_limit);

void readChunkData(MutableColumns &      columns,
                   const Chunk &         chunk,
                   const ColumnDefines & column_defines,
                   PageStorage &         storage,
                   size_t                rows_offset,
                   size_t                rows_limit);


Block readChunk(const Chunk & chunk, const ColumnDefines & read_column_defines, PageStorage & data_storage);


} // namespace DM
} // namespace DB