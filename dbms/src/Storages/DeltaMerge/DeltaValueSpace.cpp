#include <Storages/DeltaMerge/DeltaValueSpace.h>

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DM
{

constexpr size_t DELTA_SPACE_SERIALIZE_BUFFER_SIZE = 255;

const ChunkMeta::Version ChunkMeta::CURRENT_VERSION = 1;

void ChunkMeta::serialize(WriteBuffer & buf) const
{
    writeVarUInt(ChunkMeta::CURRENT_VERSION, buf); // Add binary version

    writeIntBinary(id, buf);
    writeIntBinary(file_id, buf);
    writeIntBinary(index, buf);
    writeIntBinary(rows, buf);

    writeIntBinary(static_cast<UInt8>(is_delete_range), buf);
    writeIntBinary(delete_range.start, buf);
    writeIntBinary(delete_range.end, buf);
}

ChunkMeta ChunkMeta::deserialize(ReadBuffer & buf)
{
    // Check binary version
    ChunkMeta::Version chunk_version;
    readVarUInt(chunk_version, buf);
    if (chunk_version != ChunkMeta::CURRENT_VERSION)
        throw Exception("Chunkmeta binary version not match: " + DB::toString(chunk_version), ErrorCodes::LOGICAL_ERROR);

    ChunkMeta chunk;
    readIntBinary(chunk.id, buf);
    readIntBinary(chunk.file_id, buf);
    readIntBinary(chunk.index, buf);
    readIntBinary(chunk.rows, buf);

    UInt8 is_delete_range = 0;
    readIntBinary(is_delete_range, buf);
    chunk.is_delete_range = is_delete_range != 0;
    Handle start, end;
    readIntBinary(start, buf);
    readIntBinary(end, buf);
    chunk.delete_range = HandleRange(start, end);

    return chunk;
}

static ChunkMeta createRefChunk(const ChunkMeta & chunk, const GenPageId & gen_chunk_id, WriteBatches & wbs)
{
    if (chunk.is_delete_range)
        return ChunkMeta(chunk.delete_range);

    ChunkMeta ref_chunk(chunk);
    ref_chunk.id = gen_chunk_id();
    wbs.log.putRefPage(ref_chunk.id, chunk.id);
    wbs.removed_log.delPage(chunk.id);

    return ref_chunk;
}

void serializeChunkMetas(WriteBuffer &                 buf,
                         ChunkMetaList::const_iterator begin,
                         ChunkMetaList::const_iterator end,
                         ChunkMeta *                   uncommitted_chunk)
{
    size_t size = 0;
    for (auto iter = begin; iter != end; ++iter)
        ++size;
    if (uncommitted_chunk != nullptr)
        ++size;

    writeIntBinary(size, buf);
    for (; begin != end; ++begin)
        begin->serialize(buf);
    if (uncommitted_chunk)
        uncommitted_chunk->serialize(buf);
}
ChunkMetaList deserializeChunkMetas(ReadBuffer & buf)
{
    ChunkMetaList chunks;
    UInt64        size = 0;
    readIntBinary(size, buf);
    for (UInt64 i = 0; i < size; ++i)
        chunks.emplace_back(ChunkMeta::deserialize(buf));
    return chunks;
}

//==================================================================
// DeltaSpace
//==================================================================

DeltaSpacePtr DeltaSpace::restore(PageId id, const String & parent_path, const DMContext & context)
{
    DeltaSpacePtr        instance = std::make_shared<DeltaSpace>(id, parent_path);
    Page                 page     = context.storage_pool.meta().read(instance->id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());

    instance->chunks = deserializeChunkMetas(buf);
    // Open DMFiles for later read
    std::unordered_set<DMFileID> file_ids;
    for (const auto & chunk : instance->chunks)
        file_ids.insert(chunk.file_id);
    for (const auto & file_id : file_ids)
    {
        DMFilePtr file = DMFile::restore(file_id, 0, parent_path);
        instance->files.emplace(file_id, std::move(file));
    }

    return instance;
}

void DeltaSpace::saveMeta(WriteBatch & meta_wb)
{
    MemoryWriteBuffer buf(0, DELTA_SPACE_SERIALIZE_BUFFER_SIZE);
    serializeChunkMetas(buf, chunks.begin(), chunks.end());

    const auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    meta_wb.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

DeltaSpace::AppendTaskPtr DeltaSpace::appendToDisk(const BlockOrDelete & update, WriteBatches & wbs, const DMContext & context)
{
    /// Note that this function is NOT thread safe! Now we only allow one thread to write.

    AppendTaskPtr task = std::make_shared<AppendTask>();

    if (unlikely(update.isDelete()))
    {
        const auto & delete_range = update.delete_range;
        // TODO: do we need to open another file for write?
        ChunkMeta meta;
        meta.is_delete_range = true;
        meta.delete_range    = delete_range;
        task->append_chunk   = meta;
        return task;
    }

    // If no file for write or current file is full, open a new file for write.
    if (file_writting == nullptr)
    {
        // Create a new DMFile for write
        PageId    file_id = context.storage_pool.newLogPageId();
        DMFilePtr file    = DMFile::create(file_id, parent_path);
        wbs.log.putExternal(file_id, 0);
        file_writting = file;
        files.emplace(file_id, file);
        writer = std::make_unique<DMFileWriter>( //
            file_writting,
            context.store_columns,
            context.db_context.getSettingsRef().min_compress_block_size,
            context.db_context.getSettingsRef().max_compress_block_size,
            CompressionSettings(CompressionMethod::NONE), // FIXME: do we need compress for delta?
            true);
    }

    const auto & append_block = update.block;
    // Allocate chunk_id and apply in memory
    const PageId chunk_id = context.storage_pool.newLogPageId();
    ChunkMeta    meta;
    meta.id            = chunk_id;
    meta.file_id       = file_writting->fileId();
    meta.index         = file_writting->getChunks();
    meta.rows          = append_block.rows();
    task->append_chunk = meta;

    // Append to disk
    writer->write(append_block, append_block.rows());

    // Chunk{meta.id} is in DMFile{meta.file_id}
    wbs.log.putRefPage(meta.id, meta.file_id);
    if (meta.index == 0)
        wbs.log.delPage(meta.file_id);

    // Delta's chunks info after apply.
    MemoryWriteBuffer buf(0, DELTA_SPACE_SERIALIZE_BUFFER_SIZE);
    serializeChunkMetas(buf, chunks.begin(), chunks.end(), &(task->append_chunk));
    const auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);

    return task;
}

void DeltaSpace::applyAppend(const DeltaSpace::AppendTaskPtr & task)
{
    chunks.emplace_back(std::move(task->append_chunk));
}

DeltaSpacePtr DeltaSpace::nextGeneration(const SnapshotPtr & snap, WriteBatches & wbs)
{
    DeltaSpacePtr new_delta = std::make_shared<DeltaSpace>(id, parent_path);
    if (chunks.begin() != snap->beg)
        throw Exception("Try to get next generation of delta with invalid snapshot.");

    // Remove the chunks in snap
    auto snap_iter = chunks.begin();
    for (size_t i = 0; i < snap->chunks_size; ++i, ++snap_iter)
    {
        // Remove the chunk from log. If all chunks of DMFile have been removed,
        // the DMFile will be gc by PageStorage later.
        if (!snap_iter->is_delete_range)
        {
            std::cerr << "Remove chunk[" << snap_iter->id << "]" << std::endl;
            wbs.removed_log.delPage(snap_iter->id);
        }
    }
    assert(new_delta->chunks.empty());
    new_delta->chunks.insert(new_delta->chunks.end(), snap_iter, chunks.end());

    for (const auto & chunk : new_delta->chunks)
    {
        if (new_delta->files.count(chunk.file_id) == 0)
        {
            auto iter = files.find(chunk.file_id);
            if (unlikely(iter == files.end()))
                throw Exception("");
            else
                new_delta->files.emplace(chunk.file_id, iter->second);
        }
    }
    new_delta->file_writting = file_writting;
    new_delta->writer.swap(writer);

    return new_delta;
}

DeltaSpacePtr DeltaSpace::newRef(
    const SnapshotPtr & snap, PageId new_delta_id, const String & parent_path, const GenPageId & gen_chunk_id, WriteBatches & wbs)
{
    DeltaSpacePtr ref_delta = std::make_shared<DeltaSpace>(new_delta_id, parent_path);

    ChunkMetaList new_chunks;
    auto          iter = snap->beg;
    for (size_t i = 0; i < snap->chunks_size; ++i, ++iter)
        new_chunks.push_back(createRefChunk(*iter, gen_chunk_id, wbs));
    ref_delta->chunks.swap(new_chunks);
    // TODO: files

    return ref_delta;
}

//==================================================================
// DeltaSpace::Snapshot
//==================================================================

DeltaSpace::DeltaValuesPtr DeltaSpace::Snapshot::getValues( //
    const ColumnDefines & read_columns,
    const DMContext &     context) const
{
    auto values = std::make_shared<DeltaSpace::Values>();

    auto callback = [&values](const Block & block, size_t, size_t) { values->addBlock(block, block.rows()); };

    DMFileID    current_file   = 0;
    IndexSetPtr chunks_to_read = std::make_shared<IndexSet>();
    auto        iter           = beg;
    for (size_t i = 0; /* empty */; ++i, ++iter)
    {
        if (i == chunks_size && !chunks_to_read->empty())
        {
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            break;
        }

        const ChunkMeta & chunk = *iter;
        if (iter == beg)
            current_file = chunk.file_id;

        if (unlikely(chunk.is_delete_range))
        {
            // DeleteRange, read from current file.
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);

            chunks_to_read->clear();
            chunks_to_read->insert(chunk.index);
        }
        else
        {
            if (chunk.file_id == current_file)
            {
                chunks_to_read->insert(chunk.index);
                continue;
            }

            // Chunk in another file (or the last chunk), read from current file and move to collect chunks in next file.
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            current_file = chunk.file_id;
            chunks_to_read->clear();
            chunks_to_read->insert(chunk.index);
        }
    }

    return values;
}

BlockOrDeletes DeltaSpace::Snapshot::getMergeBlocks( //
    const ColumnDefine & handle,
    size_t               rows_begin,
    size_t               deletes_begin,
    const DMContext &    context) const
{
    BlockOrDeletes res;

    // We only need pk and version
    const ColumnDefines read_columns = {handle, getVersionColumnDefine()};

    auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(rows_begin, deletes_begin);
    // TODO: we simply read to the end of current snapshot, maybe we can simplify it.
    auto [end_chunk_index, rows_end_in_end_chunk] = findChunk(numRows(), numDeletes());

    size_t block_rows_start = rows_begin;
    size_t block_rows_end   = rows_begin;

    auto iter = beg;
    for (size_t chunk_index = 0; chunk_index <= end_chunk_index; ++chunk_index, ++iter)
    {
        // Locate the first chunk
        if (chunk_index < start_chunk_index)
            continue;

        if (chunk_index == chunks_size)
        {
            res.emplace_back(read(read_columns, block_rows_start, block_rows_end - block_rows_start, context));
            break;
        }

        const ChunkMeta & chunk = *iter;

        size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
        size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.rows;

        block_rows_end += rows_end_in_chunk - rows_start_in_chunk;

        if (chunk.is_delete_range || chunk_index == end_chunk_index)
        {
            if (chunk.is_delete_range)
                res.emplace_back(chunk.delete_range);
            else if (block_rows_end != block_rows_start)
                res.emplace_back(read(read_columns, block_rows_start, block_rows_end - block_rows_start, context));

            block_rows_start = block_rows_end;
        }
    }

    return res;
}

Block DeltaSpace::Snapshot::read( //
    const ColumnDefines & read_columns,
    size_t                rows_offset,
    size_t                rows_limit,
    const DMContext &     context,
    std::optional<size_t> reserve_rows_) const
{
    if (unlikely(read_columns.empty()))
        return {};
    // Adjust the `rows_limit`
    rows_limit = std::min(rows_limit, numRows() - rows_offset);

    MutableColumns columns;
    const size_t   reserve_rows = reserve_rows_ ? *reserve_rows_ : rows_limit;
    for (const auto & define : read_columns)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(reserve_rows);
    }

    auto callback = [&](const Block & block, size_t block_rows_offset, size_t block_rows_limit) -> void {
        for (size_t i = 0; i < read_columns.size(); ++i)
        {
            const auto & define = read_columns[i];
            // TODO: handle new columns after ddl
            const auto & column_from_block = getByColumnId(block, define.id);
            columns[i]->insertRangeFrom(*column_from_block.column, block_rows_offset, block_rows_limit);
        }
    };

    const auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(rows_offset);
    const auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(rows_offset + rows_limit);

    DMFileID    current_file_id = 0;
    IndexSetPtr chunks_to_read  = std::make_shared<IndexSet>();

    std::optional<size_t> rows_offset_in_read = std::nullopt;
    size_t                rows_limit_in_read  = 0;

    auto iter = beg;
    for (size_t chunk_index = 0; chunk_index <= end_chunk_index; ++chunk_index, ++iter)
    {
        if (chunk_index < start_chunk_index)
            continue;
        if (chunk_index == chunks_size)
        {
            readFromFile(current_file_id, read_columns, chunks_to_read, context, callback, rows_offset_in_read, rows_limit_in_read);
            break;
        }

        const ChunkMeta & chunk = *iter;
        if (chunk_index == start_chunk_index)
            current_file_id = chunk.file_id;

        if (chunk.is_delete_range)
            continue;

        if (chunk.file_id == current_file_id)
        {
            if (!rows_offset_in_read.has_value())
            {
                // A new read action in DMFile{current_file_id}
                rows_offset_in_read.emplace((chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0));
                rows_limit_in_read = 0;
            }
            rows_limit_in_read += (chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.rows);
            chunks_to_read->insert(chunk.index);
        }

        if (chunk.file_id != current_file_id || chunk_index == end_chunk_index)
        {
            // Finish read action
            readFromFile(current_file_id, read_columns, chunks_to_read, context, callback, rows_offset_in_read, rows_limit_in_read);
            // Move to read next DMFile
            current_file_id = chunk.file_id;
            chunks_to_read->clear();
            chunks_to_read->insert(chunk.index);
            rows_offset_in_read = std::nullopt;
        }
    }

    Block header = toEmptyBlock(read_columns);
    return header.cloneWithColumns(std::move(columns));
}

void DeltaSpace::Snapshot::readFromFile(DMFileID              file_id,
                                        const ColumnDefines & read_columns,
                                        const IndexSetPtr &   indices,
                                        const DMContext &     context,
                                        const ReadCallback &  callback,
                                        std::optional<size_t> rows_offset_,
                                        std::optional<size_t> rows_limit_) const
{
    auto iter = files.find(file_id);
    if (iter == files.end())
        throw Exception("Read chunks in non-open DMFile[" + DB::toString(file_id) + "]", ErrorCodes::LOGICAL_ERROR);
    const auto & file = iter->second;

    DMFileBlockInputStream stream(context.db_context,
                                  MAX_UINT64, // TODO: enable version filter
                                  /* enable_clean_read */ false,
                                  context.hash_salt,
                                  file,
                                  read_columns,
                                  HandleRange::newAll(),
                                  EMPTY_FILTER,
                                  indices);

    // The rows offset for this read.
    const size_t rows_offset = rows_offset_ ? *rows_offset_ : 0;

    if (true)
    {
        std::stringstream ss;
        for (auto iter = indices->cbegin(); iter != indices->cend(); ++iter)
        {
            if (iter != indices->begin())
                ss << ",";
            ss << *iter;
        }
        std::cerr << "Reading from DMFile[" << file_id << "] with indices[" //
                  << ss.str() << "] offset[" << rows_offset << "]"          //
                  << std::endl;
    }

    Block  block;
    bool   is_first      = true;
    size_t num_rows_read = 0;
    stream.readPrefix();
    while (true)
    {
        block = stream.read();
        // No more blocks
        if (!block)
            break;

        // Non-empty block
        if (block.rows())
        {
            const size_t rows_offset_in_this_block = is_first ? rows_offset : 0;
            const size_t rows_limit_in_this_block  = rows_limit_ ? (*rows_limit_ - num_rows_read) : block.rows();
            std::cerr << "Callback with a block with " << block.rows()     //
                      << " rows, off[" << rows_offset_in_this_block << "]" //
                      << " limit[" << rows_limit_in_this_block << "]" << std::endl;
            callback(block, rows_offset_in_this_block, rows_limit_in_this_block);
            is_first = false;
            num_rows_read += block.rows();
        }
    }
    stream.readSuffix();
}

std::pair<size_t, size_t> DeltaSpace::Snapshot::findChunk(size_t rows_offset, size_t deletes_offset) const
{
    size_t rows_count    = 0;
    size_t deletes_count = 0;
    size_t chunk_index   = 0;
    for (auto iter = beg; chunk_index < chunks_size; ++iter, ++chunk_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {chunk_index, 0};

        const auto & chunk = *iter;
        if (chunk.rows > 0)
        {
            rows_count += chunk.rows;
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");
                return {chunk_index, chunk.rows - (rows_count - rows_offset)};
            }
        }
        if (chunk.is_delete_range)
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");
                return {chunk_index, 0};
            }
            ++deletes_count;
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset(" + DB::toString(rows_offset) + "), deletes_count(" + DB::toString(deletes_count) + ")");

    return {chunk_index, 0};
}

std::pair<size_t, size_t> DeltaSpace::Snapshot::findChunk(size_t rows_offset) const
{
    size_t rows_count  = 0;
    size_t chunk_index = 0;
    for (auto iter = beg; chunk_index < chunks_size; ++iter, ++chunk_index)
    {
        if (rows_count == rows_offset)
            return {chunk_index, 0};
        auto cur_chunk_rows = iter->rows;
        rows_count += cur_chunk_rows;
        if (rows_count > rows_offset)
            return {chunk_index, cur_chunk_rows - (rows_count - rows_offset)};
    }
    if (rows_count != rows_offset)
        throw Exception("rows_offset(" + DB::toString(rows_offset) + ") is out of total_rows(" + DB::toString(rows_count) + ")");

    return {chunk_index, 0};
}

size_t DeltaSpace::Snapshot::numDeletes() const
{
    size_t num_deletes = 0;
    auto   iter        = beg;
    for (size_t i = 0; i < chunks_size; ++i, ++iter)
        num_deletes += iter->is_delete_range;
    return num_deletes;
}
size_t DeltaSpace::Snapshot::numChunks() const
{
    return chunks_size;
}
size_t DeltaSpace::Snapshot::numRows() const
{
    // Note that DeleteRange.rows is always 0.
    size_t rows = 0;
    auto   iter = beg;
    for (size_t i = 0; i < chunks_size; ++i, ++iter)
        rows += iter->rows;
    return rows;
}
size_t DeltaSpace::Snapshot::numBytes() const
{
    // FIXME: numBytes
    return 0;
}

//==================================================================
// DeltaSpace::Values
//==================================================================

size_t DeltaSpace::Values::write(MutableColumns & output_columns, size_t offset, size_t limit) const
{
    auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(offset);
    auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(offset + limit);

    size_t actually_read = 0;
    size_t chunk_index   = start_chunk_index;
    for (; chunk_index <= end_chunk_index && chunk_index < sizes.size(); ++chunk_index)
    {
        size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
        size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : sizes[chunk_index];
        size_t rows_in_chunk_limit = rows_end_in_chunk - rows_start_in_chunk;

        auto & block = blocks[chunk_index];
        // Empty block means we don't need to read.
        if (rows_end_in_chunk > rows_start_in_chunk && block)
        {
            for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
            {
                if (rows_in_chunk_limit == 1)
                    output_columns[col_index]->insertFrom(*(block.getByPosition(col_index).column), rows_start_in_chunk);
                else
                    output_columns[col_index]->insertRangeFrom( //
                        *(block.getByPosition(col_index).column),
                        rows_start_in_chunk,
                        rows_in_chunk_limit);
            }

            actually_read += rows_in_chunk_limit;
        }
    }
    return actually_read;
}


} // namespace DM
} // namespace DB
