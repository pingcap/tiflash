// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyReader.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>

#include <memory>


namespace DB::DM
{

ColumnFileReaderPtr ColumnFileTiny::getReader(
    const DMContext &,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnDefinesPtr & col_defs,
    ReadTag) const
{
    return std::make_shared<ColumnFileTinyReader>(*this, data_provider, col_defs);
}

void ColumnFileTiny::serializeMetadata(WriteBuffer & buf, bool save_schema) const
{
    serializeSchema(buf, save_schema ? schema->getSchema() : Block{});

    writeIntBinary(data_page_id, buf);
    writeIntBinary(rows, buf);
    writeIntBinary(bytes, buf);
}

void ColumnFileTiny::serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool save_schema) const
{
    dtpb::ColumnFileTiny * tiny_pb = cf_pb->mutable_tiny_file();
    if (save_schema)
        serializeSchema(tiny_pb, schema->getSchema());

    tiny_pb->set_id(data_page_id);
    tiny_pb->set_rows(rows);
    tiny_pb->set_bytes(bytes);

    if (!index_infos)
        return;

    for (const auto & index_info : *index_infos)
    {
        auto * index_pb = tiny_pb->add_indexes();
        index_pb->set_index_page_id(index_info.index_page_id);
        if (index_info.vector_index.has_value())
            index_pb->mutable_vector_index()->CopyFrom(*index_info.vector_index);
    }
}

ColumnFilePersistedPtr ColumnFileTiny::deserializeMetadata(
    const DMContext & context,
    ReadBuffer & buf,
    ColumnFileSchemaPtr & last_schema)
{
    auto schema_block = deserializeSchema(buf);
    auto schema = getSchema(context, schema_block, last_schema);

    PageIdU64 data_page_id;
    size_t rows, bytes;

    readIntBinary(data_page_id, buf);
    readIntBinary(rows, buf);
    readIntBinary(bytes, buf);

    return std::make_shared<ColumnFileTiny>(schema, rows, bytes, data_page_id, context);
}

std::shared_ptr<ColumnFileSchema> ColumnFileTiny::getSchema(
    const DMContext & context,
    BlockPtr schema_block,
    ColumnFileSchemaPtr & last_schema)
{
    std::shared_ptr<ColumnFileSchema> schema;

    if (!schema_block)
        schema = last_schema;
    else
    {
        schema = getSharedBlockSchemas(context)->getOrCreate(*schema_block);
        last_schema = schema;
    }

    if (unlikely(!schema))
        throw Exception("Cannot deserialize DeltaPackBlock's schema", ErrorCodes::LOGICAL_ERROR);
    return schema;
}

ColumnFilePersistedPtr ColumnFileTiny::deserializeMetadata(
    const DMContext & context,
    const dtpb::ColumnFileTiny & cf_pb,
    ColumnFileSchemaPtr & last_schema)
{
    auto schema_block = deserializeSchema(cf_pb.columns());
    auto schema = getSchema(context, schema_block, last_schema);

    PageIdU64 data_page_id = cf_pb.id();
    size_t rows = cf_pb.rows();
    size_t bytes = cf_pb.bytes();
    auto index_infos = std::make_shared<IndexInfos>();
    index_infos->reserve(cf_pb.indexes().size());
    for (const auto & index_pb : cf_pb.indexes())
    {
        if (index_pb.has_vector_index())
            index_infos->emplace_back(index_pb.index_page_id(), index_pb.vector_index());
        else
            index_infos->emplace_back(index_pb.index_page_id(), std::nullopt);
    }

    return std::make_shared<ColumnFileTiny>(schema, rows, bytes, data_page_id, context, index_infos);
}

ColumnFilePersistedPtr ColumnFileTiny::restoreFromCheckpoint(
    const LoggerPtr & parent_log,
    const DMContext & context,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs,
    BlockPtr schema,
    PageIdU64 data_page_id,
    size_t rows,
    size_t bytes,
    IndexInfosPtr index_infos)
{
    auto put_remote_page = [&](PageIdU64 page_id) {
        auto new_cf_id = context.storage_pool->newLogPageId();
        /// Generate a new RemotePage with an entry with data location on S3
        auto remote_page_id = UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(context.keyspace_id, StorageType::Log, context.physical_table_id),
            page_id);
        // The `data_file_id` in temp_ps is lock key, we need convert it to data key before write to local ps
        auto remote_data_location = temp_ps->getCheckpointLocation(remote_page_id);
        RUNTIME_CHECK(remote_data_location.has_value());
        auto remote_data_file_lock_key_view = S3::S3FilenameView::fromKey(*remote_data_location->data_file_id);
        RUNTIME_CHECK(remote_data_file_lock_key_view.isLockFile());
        auto remote_data_file_key = remote_data_file_lock_key_view.asDataFile().toFullKey();
        PS::V3::CheckpointLocation new_remote_data_location{
            .data_file_id = std::make_shared<String>(remote_data_file_key),
            .offset_in_file = remote_data_location->offset_in_file,
            .size_in_file = remote_data_location->size_in_file,
        };
        // TODO: merge the `getEntry` and `getCheckpointLocation`
        auto entry = temp_ps->getEntry(remote_page_id);
        LOG_DEBUG(
            parent_log,
            "Write remote page to local, page_id={} remote_location={} remote_page_id={}",
            new_cf_id,
            new_remote_data_location.toDebugString(),
            remote_page_id);
        wbs.log.putRemotePage(new_cf_id, 0, entry.size, new_remote_data_location, std::move(entry.field_offsets));
        return new_cf_id;
    };

    // Write column data page to local ps
    auto new_cf_id = put_remote_page(data_page_id);
    auto column_file_schema = std::make_shared<ColumnFileSchema>(*schema);
    if (!index_infos)
        return std::make_shared<ColumnFileTiny>(column_file_schema, rows, bytes, new_cf_id, context);

    // Write index data page to local ps
    auto new_index_infos = std::make_shared<IndexInfos>();
    for (const auto & index : *index_infos)
    {
        auto new_index_page_id = put_remote_page(index.index_page_id);
        if (index.vector_index)
            new_index_infos->emplace_back(new_index_page_id, index.vector_index);
        else
            new_index_infos->emplace_back(new_index_page_id, std::nullopt);
    }
    return std::make_shared<ColumnFileTiny>(column_file_schema, rows, bytes, new_cf_id, context, new_index_infos);
}

std::tuple<ColumnFilePersistedPtr, BlockPtr> ColumnFileTiny::createFromCheckpoint(
    const LoggerPtr & parent_log,
    const DMContext & context,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    const BlockPtr & last_schema,
    WriteBatches & wbs)
{
    auto schema = deserializeSchema(buf);
    if (!schema)
        schema = last_schema;
    RUNTIME_CHECK(schema != nullptr);

    PageIdU64 data_page_id;
    size_t rows, bytes;

    readIntBinary(data_page_id, buf);
    readIntBinary(rows, buf);
    readIntBinary(bytes, buf);

    return {
        restoreFromCheckpoint(parent_log, context, temp_ps, wbs, schema, data_page_id, rows, bytes, nullptr),
        schema,
    };
}

std::tuple<ColumnFilePersistedPtr, BlockPtr> ColumnFileTiny::createFromCheckpoint(
    const LoggerPtr & parent_log,
    const DMContext & context,
    const dtpb::ColumnFileTiny & cf_pb,
    UniversalPageStoragePtr temp_ps,
    const BlockPtr & last_schema,
    WriteBatches & wbs)
{
    auto schema = deserializeSchema(cf_pb.columns());
    if (!schema)
        schema = last_schema;
    RUNTIME_CHECK(schema != nullptr);

    PageIdU64 data_page_id = cf_pb.id();
    size_t rows = cf_pb.rows();
    size_t bytes = cf_pb.bytes();
    auto index_infos = std::make_shared<IndexInfos>();
    index_infos->reserve(cf_pb.indexes().size());
    for (const auto & index_pb : cf_pb.indexes())
    {
        if (index_pb.has_vector_index())
            index_infos->emplace_back(index_pb.index_page_id(), index_pb.vector_index());
        else
            index_infos->emplace_back(index_pb.index_page_id(), std::nullopt);
    }

    return {
        restoreFromCheckpoint(parent_log, context, temp_ps, wbs, schema, data_page_id, rows, bytes, index_infos),
        schema,
    };
}

Block ColumnFileTiny::readBlockForMinorCompaction(const PageReader & page_reader) const
{
    const auto & schema_ref = schema->getSchema();
    auto page = page_reader.read(data_page_id);
    auto columns = schema_ref.cloneEmptyColumns();

    if (unlikely(columns.size() != page.fieldSize()))
        throw Exception("Column size and field size not the same");

    for (size_t index = 0; index < schema_ref.columns(); ++index)
    {
        auto data_buf = page.getFieldData(index);
        const auto & type = schema_ref.getByPosition(index).type;
        auto & column = columns[index];
        deserializeColumn(*column, type, data_buf, rows);
    }

    return schema_ref.cloneWithColumns(std::move(columns));
}

ColumnFileTinyPtr ColumnFileTiny::writeColumnFile(
    const DMContext & context,
    const Block & block,
    size_t offset,
    size_t limit,
    WriteBatches & wbs)
{
    auto page_id = writeColumnFileData(context, block, offset, limit, wbs);

    auto schema = getSharedBlockSchemas(context)->getOrCreate(block);

    auto bytes = block.bytes(offset, limit);
    return std::make_shared<ColumnFileTiny>(schema, limit, bytes, page_id, context);
}

PageIdU64 ColumnFileTiny::writeColumnFileData(
    const DMContext & dm_context,
    const Block & block,
    size_t offset,
    size_t limit,
    WriteBatches & wbs)
{
    auto page_id = dm_context.storage_pool->newLogPageId();

    MemoryWriteBuffer write_buf;
    PageFieldSizes col_data_sizes;
    for (const auto & col : block)
    {
        auto last_buf_size = write_buf.count();
        serializeColumn(
            write_buf,
            *col.column,
            col.type,
            offset,
            limit,
            dm_context.global_context.getSettingsRef().dt_compression_method,
            dm_context.global_context.getSettingsRef().dt_compression_level);
        size_t serialized_size = write_buf.count() - last_buf_size;
        RUNTIME_CHECK_MSG(
            serialized_size != 0,
            "try to persist a block with empty column, colname={} colid={} block={}",
            col.name,
            col.column_id,
            block.dumpJsonStructure());
        col_data_sizes.push_back(serialized_size);
    }

    auto data_size = write_buf.count();
    auto buf = write_buf.tryGetReadBuffer();
    if (const auto & file_provider = dm_context.global_context.getFileProvider();
        unlikely(file_provider->isEncryptionEnabled(dm_context.keyspace_id)))
    {
        if (const auto ep = EncryptionPath("", "", dm_context.keyspace_id);
            unlikely(!file_provider->isFileEncrypted(ep)))
        {
            file_provider->createEncryptionInfo(ep);
        }

        char page_data[data_size];
        buf->readStrict(page_data, data_size);
        // encrypt the page data in place
        file_provider->encryptPage(dm_context.keyspace_id, page_data, data_size, page_id);
        // ReadBufferFromOwnString will copy the data, and own the data.
        buf = std::make_shared<ReadBufferFromOwnString>(std::string_view(page_data, data_size));
    }
    wbs.log.putPage(page_id, 0, buf, data_size, col_data_sizes);

    return page_id;
}

void ColumnFileTiny::removeData(WriteBatches & wbs) const
{
    wbs.removed_log.delPage(data_page_id);
    if (index_infos)
    {
        for (const auto & index_info : *index_infos)
            wbs.removed_log.delPage(index_info.index_page_id);
    }
}

ColumnFileTiny::ColumnFileTiny(
    const ColumnFileSchemaPtr & schema_,
    UInt64 rows_,
    UInt64 bytes_,
    PageIdU64 data_page_id_,
    const DMContext & dm_context,
    const IndexInfosPtr & index_infos_)
    : schema(schema_)
    , rows(rows_)
    , bytes(bytes_)
    , data_page_id(data_page_id_)
    , index_infos(index_infos_)
    , keyspace_id(dm_context.keyspace_id)
    , file_provider(dm_context.global_context.getFileProvider())
{}

} // namespace DB::DM
