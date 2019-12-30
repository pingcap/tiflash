#include <Storages/DeltaMerge/DeltaValueSpace.h>

#include <ext/scope_guard.h>

#include <DataStreams/BlocksListBlockInputStream.h>
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

namespace
{
ChunkMeta createRefChunk(const ChunkMeta & chunk, const GenPageId & gen_chunk_id, WriteBatches & wbs)
{
    if (chunk.is_delete_range)
        return ChunkMeta(chunk.delete_range);

    ChunkMeta ref_chunk(chunk);
    ref_chunk.id = gen_chunk_id();
    // Add ref for chunk.id to DMFile.id and remove old chunk
    wbs.log.putRefPage(ref_chunk.id, ref_chunk.file_id);
    wbs.removed_log.delPage(chunk.id);

    return ref_chunk;
}

void serializeChunkMetas(WriteBuffer &              buf,
                         ChunkMetas::const_iterator begin,
                         ChunkMetas::const_iterator end,
                         ChunkMeta *                uncommitted_chunk = nullptr)
{
    size_t size = 0;
    for (auto iter = begin; iter != end; ++iter)
        ++size;
    if (uncommitted_chunk != nullptr)
        ++size;

    writeIntBinary(size, buf);
    for (auto iter = begin; iter != end; ++iter)
        iter->serialize(buf);
    if (uncommitted_chunk)
        uncommitted_chunk->serialize(buf);
}

ChunkMetas deserializeChunkMetas(ReadBuffer & buf)
{
    ChunkMetas chunks;
    UInt64     size = 0;
    readIntBinary(size, buf);
    for (UInt64 i = 0; i < size; ++i)
        chunks.emplace_back(ChunkMeta::deserialize(buf));
    return chunks;
}
} // namespace

//==================================================================
// DeltaSpace
//==================================================================
DeltaSpace::~DeltaSpace()
{
    if (file_writting)
    {
        writer->finalize();
        writer.reset();
        file_writting->enableGC();
        file_writting.reset();
    }
}

void DeltaSpace::drop()
{
    // Simply free the writer so that we don't apply finalize in destructor.
    writer.reset();
    file_writting.reset();
}

DeltaSpacePtr DeltaSpace::restore(PageId id, const DMContext & context)
{
    const String         parent_path = context.deltaPath();
    DeltaSpacePtr        instance    = std::make_shared<DeltaSpace>(id, parent_path);
    const Page           page        = context.storage_pool.meta().read(instance->id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());

    instance->chunks = deserializeChunkMetas(buf);
    // Open DMFiles for later read
    std::unordered_set<DMFileID> file_ids;
    for (const auto & chunk : instance->chunks)
    {
        if (!chunk.is_delete_range)
            file_ids.insert(chunk.file_id);
    }
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
    serializeChunkMetas(buf, chunks.cbegin(), chunks.cend());

    const auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    meta_wb.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

DeltaSpace::AppendTaskPtr DeltaSpace::createAppendTask(const DMContext & context, const BlockOrDelete & update, WriteBatches & wbs)
{
    /// Note that this function is NOT thread safe! Now caller must ensure only one thread to write.

    AppendTaskPtr task = std::make_shared<AppendTask>();

    if (unlikely(update.isDelete()))
    {
        const auto & delete_range = update.delete_range;
        ChunkMeta    meta(delete_range);
        task->append_chunk = meta;
        return task;
    }

    // If no file for write or current file is full, open a new file for write.
    if (file_writting == nullptr)
    {
        // Create a new DMFile for write
        const DMFileID file_id = context.storage_pool.newLogPageId();
        DMFilePtr      file    = DMFile::create(file_id, parent_path, true);
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
        LOG_TRACE(log, "Delta[" + DB::toString(id) + "] create DMFile[" + DB::toString(file_id) + "] to write.");
    }

    const auto &  append_block = update.block;
    const ChunkID chunk_id     = context.storage_pool.newLogPageId();
    ChunkMeta     meta(chunk_id, file_writting->fileId(), file_writting->getChunks(), append_block.rows());
    task->append_chunk = meta;

    // Append to disk
    writer->write(append_block, append_block.rows());
    return task;
}

void DeltaSpace::applyAppendToWriteBatches(const DeltaSpace::AppendTaskPtr & task, WriteBatches & wbs) const
{
    const ChunkMeta & meta = task->append_chunk;
    if (!meta.is_delete_range)
    {
        // Chunk{meta.id} is in DMFile{meta.file_id}
        wbs.log.putRefPage(meta.id, meta.file_id);
        if (meta.index == 0)
            wbs.log.delPage(meta.file_id);
    }
    // Delta's chunks info after apply.
    MemoryWriteBuffer buf(0, DELTA_SPACE_SERIALIZE_BUFFER_SIZE);
    serializeChunkMetas(buf, chunks.begin(), chunks.end(), &(task->append_chunk));
    const auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

void DeltaSpace::applyAppendInMemory(const DeltaSpace::AppendTaskPtr & task)
{
    // In case the DMFilePtr is removed by `nextGeneration`.
    // See test Concurrency_CreateNextGeneration_Append for details.
    const auto & chunk = task->append_chunk;
    if (!chunk.is_delete_range && files.find(chunk.file_id) == files.end())
    {
        const auto file_id = chunk.file_id;
        DMFilePtr  file    = DMFile::restore(file_id, 0, parent_path);
        files.emplace(file_id, std::move(file));
    }
    chunks.emplace_back(std::move(task->append_chunk));
}

DeltaSpacePtr DeltaSpace::nextGeneration(const SnapshotPtr & snap, WriteBatches & wbs)
{
    DeltaSpacePtr new_delta = std::make_shared<DeltaSpace>(id, parent_path);
    if (!chunks.empty() && !snap->chunks.empty() && !chunks[0].equals(snap->chunks[0]))
        throw Exception("Try to get next generation of delta with invalid snapshot.");

    // Remove the chunks in snap
    {
        auto iter = chunks.begin();
        for (size_t i = 0; i < snap->chunks.size(); ++i, ++iter)
        {
            // Remove the chunk from log. If all chunks of DMFile have been removed,
            // the DMFile will be gc by PageStorage later.
            if (!iter->is_delete_range)
            {
                wbs.removed_log.delPage(iter->id);
            }
        }
        assert(new_delta->chunks.empty());
        new_delta->chunks.insert(new_delta->chunks.end(), iter, chunks.end());
    }

    // Copy the DMFilePtr we need.
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
    // Keep file_writting empty so that `new_delta` will open a new DMFile to write next time.

    if (log->trace())
    {
        std::stringstream ss;
        for (const auto & chunk : new_delta->chunks)
        {
            if (chunk.is_delete_range)
                ss << "DeleteRange" << chunk.delete_range.toString();
            else
                ss << "DMFile_" << chunk.file_id << "_" << chunk.index << "_" << chunk.id << ",";
        }

        LOG_TRACE(log,
                  "Generate new delta [" + DB::toString(new_delta->id) + "] with " + DB::toString(new_delta->numChunks()) + " chunks("
                      + DB::toString(new_delta->numDeletes()) + " deletes), chunks[" + ss.str() + "]. Removed " //
                      + DB::toString(snap->numChunks()) + " chunks(" + DB::toString(snap->numDeletes()) + " deletes)");
    }

    return new_delta;
}

DeltaSpacePtr DeltaSpace::newRef(
    const SnapshotPtr & snap, PageId new_delta_id, const String & parent_path, const GenPageId & gen_chunk_id, WriteBatches & wbs)
{
    DeltaSpacePtr ref_delta = std::make_shared<DeltaSpace>(new_delta_id, parent_path);

    ChunkMetas new_chunks;
    new_chunks.reserve(snap->chunks.size());
    for (size_t i = 0; i < snap->chunks.size(); ++i)
        new_chunks.push_back(createRefChunk(snap->chunks[i], gen_chunk_id, wbs));
    ref_delta->chunks.swap(new_chunks);
    // Copy opened DMfiles
    for (const auto & [file_id, file] : snap->files)
        ref_delta->files.emplace(file_id, file);
    // Keep file_writting empty so that `ref_delta` will open a new DMFile to write next time.

    if (ref_delta->log->trace())
    {
        std::stringstream ss;
        for (const auto & chunk : ref_delta->chunks)
        {
            if (chunk.is_delete_range)
                ss << "DeleteRange" << chunk.delete_range.toString();
            else
                ss << "DMFile_" << chunk.file_id << "_" << chunk.index << "_" << chunk.id << ",";
        }

        LOG_TRACE(ref_delta->log,
                  "Generate ref delta [" + DB::toString(ref_delta->id) + "] with " + DB::toString(ref_delta->numChunks()) + " chunks("
                      + DB::toString(ref_delta->numDeletes()) + " deletes), chunks[" + ss.str() + "].");
    }

    return ref_delta;
}

void DeltaSpace::check(const PageReader & meta_page_reader, const String & when)
{
    MemoryWriteBuffer buf(0, DELTA_SPACE_SERIALIZE_BUFFER_SIZE);
    serializeChunkMetas(buf, chunks.cbegin(), chunks.cend());
    const auto data_size   = buf.count(); // Must be called before tryGetReadBuffer.
    char *     data_buffer = (char *)::malloc(data_size);
    SCOPE_EXIT({ ::free(data_buffer); });
    auto read_buf = buf.tryGetReadBuffer();
    read_buf->readStrict(data_buffer, data_size);
    auto page_checksum = CityHash_v1_0_2::CityHash64(data_buffer, data_size);
    if (meta_page_reader.getPageChecksum(id) != page_checksum)
    {
        auto                 page = meta_page_reader.read(id);
        ReadBufferFromMemory rb(page.data.begin(), page.data.size());
        auto                 disk_chunks = deserializeChunkMetas(rb);
        throw Exception(when + ", DeltaSpace[" + DB::toString(id) + "] memory and disk content not match, memory: "
                        + DB::toString(chunks.size()) + ", disk: " + DB::toString(disk_chunks.size()));
    }

    for (const auto & chunk : chunks)
    {
        if (chunk.is_delete_range)
            continue;
        if (auto iter = files.find(chunk.file_id); iter == files.end())
        {
            throw Exception(when + ", DeltaSpace[" + DB::toString(id) + "] Chunk[" + DB::toString(chunk.id) + "] DMFile["
                            + DB::toString(chunk.file_id) + "] not open. Chunk index:" + DB::toString(chunk.index));
        }
    }
}

//==================================================================
// DeltaSpace::Snapshot
//==================================================================
DeltaSpace::SnapshotPtr DeltaSpace::getSnapshot() const
{
    if constexpr (DM_RUN_CHECK)
    {
        for (const auto & chunk : chunks)
        {
            if (chunk.is_delete_range)
                continue;
            if (files.find(chunk.file_id) == files.end())
            {
                throw Exception("Try to create borken snapshot for delta[" + DB::toString(id) + "]: Chunk[" + DB::toString(chunk.id)
                                    + "] should be in DMFile" + DB::toString(chunk.file_id) + ", index " + DB::toString(chunk.index)
                                    + ", but no DMFile ptr.",
                                ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    return std::make_shared<Snapshot>(id, chunks, files);
}

DeltaSpace::DeltaValuesPtr DeltaSpace::Snapshot::getValues( //
    const ColumnDefines & read_columns,
    const DMContext &     context) const
try
{
    auto values = std::make_shared<DeltaSpace::Values>();

    auto callback = [&values](const Block & block, size_t, size_t) { values->addBlock(block, block.rows()); };

    bool        is_first_chunk = true;
    DMFileID    current_file   = 0;
    IndexSetPtr chunks_to_read = std::make_shared<IndexSet>();
    for (size_t i = 0; /* empty */; ++i)
    {
        if (i == chunks.size())
        {
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            break;
        }

        const ChunkMeta & chunk = chunks[i];
        if (unlikely(chunk.is_delete_range))
        {
            // DeleteRange, read from current file.
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            chunks_to_read->clear();
        }
        else
        {
            if (is_first_chunk)
            {
                current_file   = chunk.file_id;
                is_first_chunk = false;
            }

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
catch (Exception & e)
{
    e.addMessage("(while getValues from delta[" + DB::toString(id) + "])");
    throw;
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

    for (size_t chunk_index = start_chunk_index; chunk_index <= end_chunk_index; ++chunk_index)
    {
        if (chunk_index == chunks.size())
        {
            try
            {
                res.emplace_back(read(read_columns, block_rows_start, block_rows_end - block_rows_start, context));
            }
            catch (Exception & e)
            {
                e.addMessage("(while getValues from delta[" + DB::toString(id) + "])");
                throw;
            }

            break;
        }

        const ChunkMeta & chunk = chunks[chunk_index];

        size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
        size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.rows;

        block_rows_end += rows_end_in_chunk - rows_start_in_chunk;

        if (chunk.is_delete_range || chunk_index == end_chunk_index)
        {
            if (block_rows_end != block_rows_start)
            {
                try
                {
                    res.emplace_back(read(read_columns, block_rows_start, block_rows_end - block_rows_start, context));
                }
                catch (Exception & e)
                {
                    e.addMessage("(while getValues from delta[" + DB::toString(id) + "])");
                    throw;
                }
            }

            if (chunk.is_delete_range)
                res.emplace_back(chunk.delete_range);

            block_rows_start = block_rows_end;
        }
    }

    return res;
}

BlockInputStreamPtr DeltaSpace::Snapshot::getInputStream(const ColumnDefines & read_columns, const DMContext & context) const
{
    // This function is just for readRaw, simply read all data from delta and return a BlocksList
    BlocksList blocks;

    auto callback = [&blocks](const Block & block, size_t, size_t) { blocks.emplace_back(block); };

    bool        is_first_chunk = true;
    DMFileID    current_file   = 0;
    IndexSetPtr chunks_to_read = std::make_shared<IndexSet>();
    for (size_t i = 0; /* empty */; ++i)
    {
        if (i == chunks.size())
        {
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            break;
        }

        const ChunkMeta & chunk = chunks[i];
        if (unlikely(chunk.is_delete_range))
        {
            // DeleteRange, read from current file.
            readFromFile(current_file, read_columns, chunks_to_read, context, callback);
            chunks_to_read->clear();
        }
        else
        {
            if (is_first_chunk)
            {
                current_file   = chunk.file_id;
                is_first_chunk = false;
            }

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
    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
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

    bool        is_first_chunk  = true;
    DMFileID    current_file_id = 0;
    IndexSetPtr chunks_to_read  = std::make_shared<IndexSet>();

    std::optional<size_t> block_rows_offset = std::nullopt;
    size_t                block_rows_limit  = 0;

    for (size_t chunk_index = start_chunk_index; chunk_index <= end_chunk_index; ++chunk_index)
    {
        if (chunk_index == chunks.size())
        {
            readFromFile(current_file_id, read_columns, chunks_to_read, context, callback, block_rows_offset, block_rows_limit);
            break;
        }

        const ChunkMeta & chunk = chunks[chunk_index];
        if (!chunk.is_delete_range && is_first_chunk)
        {
            current_file_id = chunk.file_id;
            is_first_chunk  = false;
        }

        if (!chunk.is_delete_range && chunk.file_id == current_file_id)
        {
            if (!block_rows_offset.has_value())
            {
                // A new read action in DMFile{current_file_id}
                block_rows_offset.emplace((chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0));
                block_rows_limit = 0;
            }
            block_rows_limit += (chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.rows);
            chunks_to_read->insert(chunk.index);
        }

        if (chunk_index == end_chunk_index || (!chunk.is_delete_range && chunk.file_id != current_file_id))
        {
            // Finish read action
            readFromFile(current_file_id, read_columns, chunks_to_read, context, callback, block_rows_offset, block_rows_limit);
            // Move to read next DMFile
            current_file_id = chunk.file_id;
            chunks_to_read->clear();
            chunks_to_read->insert(chunk.index);
            // A new read action in next DMFile, reset offset and limit
            block_rows_offset.emplace(0);
            block_rows_limit = (chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.rows);
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
    assert(indices != nullptr);
    if (indices->empty())
        return;

    // The rows offset for this read.
    const size_t rows_offset = rows_offset_ ? *rows_offset_ : 0;

    String str_indices;
    if (true)
    {
        std::stringstream ss;
        ss << "[";
        for (auto iter = indices->cbegin(); iter != indices->cend(); ++iter)
        {
            if (iter != indices->begin())
                ss << ",";
            ss << *iter;
        }
        ss << "]";
        str_indices = ss.str();
    }

    auto iter = files.find(file_id);
    if (iter == files.end())
        throw Exception("Read " + DB::toString(indices->size()) + " chunks in non-open DMFile[" + DB::toString(file_id)
                            + "], indices: " + str_indices,
                        ErrorCodes::LOGICAL_ERROR);
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
            const size_t rows_limit_in_this_block  = rows_limit_ ?     //
                std::min((*rows_limit_ - num_rows_read), block.rows()) //
                                                                : block.rows();
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
    for (; chunk_index < chunks.size(); ++chunk_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {chunk_index, 0};

        const auto & chunk = chunks[chunk_index];
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
    for (; chunk_index < chunks.size(); ++chunk_index)
    {
        if (rows_count == rows_offset)
            return {chunk_index, 0};
        auto cur_chunk_rows = chunks[chunk_index].rows;
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
    for (size_t i = 0; i < chunks.size(); ++i)
        num_deletes += chunks[i].is_delete_range;
    return num_deletes;
}
size_t DeltaSpace::Snapshot::numChunks() const
{
    return chunks.size();
}
size_t DeltaSpace::Snapshot::numRows() const
{
    // Note that DeleteRange.rows is always 0.
    size_t rows = 0;
    for (size_t i = 0; i < chunks.size(); ++i)
        rows += chunks[i].rows;
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
