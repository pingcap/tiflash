#pragma once

#include <optional>

#include <IO/CompressedStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/RSIndex.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/Page/PageStorage.h>


namespace DB
{
namespace DM
{
static constexpr size_t CHUNK_SERIALIZE_BUFFER_SIZE = 65536;


struct ColumnMeta
{
    ColId          col_id;
    PageId         page_id;
    UInt32         rows;
    UInt64         bytes;
    DataTypePtr    type;
    MinMaxIndexPtr minmax;
};
using ColumnMetas = std::vector<ColumnMeta>;

class Chunk
{
public:
    using ColumnMetaMap = std::unordered_map<ColId, ColumnMeta>;

    Chunk() : Chunk(0, 0) {}
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

    bool hasColumn(ColId col_id) const { return columns.count(col_id) > 0; }

    const ColumnMeta & getColumn(ColId col_id) const
    {
        auto it = columns.find(col_id);
        if (unlikely(it == columns.end()))
            throw Exception("Column with id" + DB::toString(col_id) + " not found");
        return it->second;
    }

    ColumnMeta const * tryGetColumn(ColId col_id) const
    {
        auto it = columns.find(col_id);
        if (unlikely(it == columns.end()))
            return nullptr;
        return &it->second;
    }

    const ColumnMetaMap & getMetas() const { return columns; }

    void insert(const ColumnMeta & c)
    {
        if (isDeleteRange())
            throw Exception("Insert column into delete range chunk is not allowed");
        columns[c.col_id] = c;
        if (rows != 0 && rows != c.rows)
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

// TODO: use list instead of vector, so that DiskValueSpace won't need to do copy during Segment#getReadSnapshot.
using Chunks    = std::vector<Chunk>;
using GenPageId = std::function<PageId()>;

void   serializeChunks(WriteBuffer &           buf,
                       Chunks::const_iterator  begin,
                       Chunks ::const_iterator end,
                       const Chunk *           extra1 = nullptr,
                       const Chunk *           extra2 = nullptr);
Chunks deserializeChunks(ReadBuffer & buf);

Chunk prepareChunkDataWrite(const DMContext & dm_context, const GenPageId & gen_data_page_id, WriteBatch & wb, const Block & block);

/**
 * Read `chunk`'s columns from `storage` and append the `chunk`'s data range
 * [`rows_offset`, `rows_offset`+`rows_limit`) to `columns`.
 *
 * Note that after ddl, the data type between `chunk.columns` and `column_defines` maybe different,
 * we do a cast according to `column_defines` before append to `columns`.
 *
 * @param columns           The columns to append data.
 * @param column_defines    The DataType, column-id of `columns`.
 * @param chunk             Info about chunk to read. e.g. PageId in `storage`, DataType for reading.
 * @param page_reader       Where the serialized data stored in.
 * @param rows_offset
 * @param rows_limit
 */
void readChunkData(MutableColumns &      columns,
                   const ColumnDefines & column_defines,
                   const Chunk &         chunk,
                   const PageReader &    page_reader,
                   size_t                rows_offset,
                   size_t                rows_limit);


Block readChunk(const Chunk & chunk, const ColumnDefines & read_column_defines, const PageReader & page_reader);

/**
 * Cast `disk_col` from `disk_type` according to `read_define`, and append data
 * [`rows_offset`, `rows_offset`+`rows_limit`) to `memory_col`
 *
 * @param disk_type
 * @param disk_col
 * @param read_define
 * @param memory_col
 * @param rows_offset
 * @param rows_limit
 */
void castColumnAccordingToColumnDefine(const DataTypePtr &  disk_type,
                                       const ColumnPtr &    disk_col,
                                       const ColumnDefine & read_define,
                                       MutableColumnPtr     memory_col,
                                       size_t               rows_offset,
                                       size_t               rows_limit);


} // namespace DM
} // namespace DB
