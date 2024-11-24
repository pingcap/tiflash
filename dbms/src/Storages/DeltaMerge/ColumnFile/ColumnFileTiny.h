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

#pragma once

#include <IO/FileProvider/FileProvider_fwd.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>
#include <Storages/DeltaMerge/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/dtpb/vector_index.pb.h>
#include <Storages/Page/PageStorage_fwd.h>

namespace DB::DM
{

class ColumnFileTiny;
using ColumnFileTinyPtr = std::shared_ptr<ColumnFileTiny>;

/// A column file which data is stored in PageStorage.
/// It may be created in two ways:
///   1. created directly when writing to storage if the data is large enough
///   2. created when flushed `ColumnFileInMemory` to disk
class ColumnFileTiny : public ColumnFilePersisted
{
public:
    friend class ColumnFileTinyReader;
    friend class ColumnFileTinyVectorIndexWriter;
    friend class ColumnFileTinyVectorIndexReader;
    friend struct Remote::Serializer;

    struct IndexInfo
    {
        IndexInfo(PageIdU64 page_id, std::optional<dtpb::VectorIndexFileProps> vec_index)
            : index_page_id(page_id)
            , vector_index(vec_index)
        {}

        PageIdU64 index_page_id{};
        std::optional<dtpb::VectorIndexFileProps> vector_index = std::nullopt;
    };
    using IndexInfos = std::vector<IndexInfo>;
    using IndexInfosPtr = std::shared_ptr<IndexInfos>;

private:
    ColumnFileSchemaPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    /// The id of data page which stores the data of this pack.
    PageIdU64 data_page_id;

    /// HACK: Currently this field is only available when ColumnFileTiny is restored from remote proto.
    /// It is not available when ColumnFileTiny is constructed or restored locally.
    /// Maybe we should just drop this field, and store the data_page_size in somewhere else.
    UInt64 data_page_size = 0;

    /// The index information of this file.
    IndexInfosPtr index_infos;

    /// The id of the keyspace which this ColumnFileTiny belongs to.
    KeyspaceID keyspace_id;
    /// The global file_provider
    const FileProviderPtr file_provider;

    /// The members below are not serialized.

    /// The cache data in memory.
    /// Currently this field is unused.
    CachePtr cache;

private:
    /// Read a block of columns in `column_defines` from cache / disk,
    /// if `pack->schema` is not match with `column_defines`, take good care of ddl cast
    Columns readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;
    Columns readFromDisk(
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefines & column_defines,
        size_t col_start,
        size_t col_end) const;

    void fillColumns(
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefines & col_defs,
        size_t col_count,
        Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const { return schema->getDataType(column_id); }

public:
    ColumnFileTiny(
        const ColumnFileSchemaPtr & schema_,
        UInt64 rows_,
        UInt64 bytes_,
        PageIdU64 data_page_id_,
        const DMContext & dm_context,
        const IndexInfosPtr & index_infos_ = nullptr,
        const CachePtr & cache_ = nullptr);

    Type getType() const override { return Type::TINY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; }

    IndexInfosPtr getIndexInfos() const { return index_infos; }
    bool hasIndex(Int64 index_id) const
    {
        if (!index_infos)
            return false;
        return std::any_of(index_infos->cbegin(), index_infos->cend(), [index_id](const auto & info) {
            if (!info.vector_index)
                return false;
            return info.vector_index->index_id() == index_id;
        });
    }

    auto getCache() const { return cache; }
    void clearCache() { cache = {}; }

    ColumnFileSchemaPtr getSchema() const { return schema; }

    ColumnFileTinyPtr cloneWith(PageIdU64 new_data_page_id)
    {
        auto new_tiny_file = std::make_shared<ColumnFileTiny>(*this);
        new_tiny_file->data_page_id = new_data_page_id;
        return new_tiny_file;
    }

    ColumnFileTinyPtr cloneWith(PageIdU64 new_data_page_id, const IndexInfosPtr & index_infos_) const
    {
        auto new_tiny_file = std::make_shared<ColumnFileTiny>(*this);
        new_tiny_file->data_page_id = new_data_page_id;
        new_tiny_file->index_infos = index_infos_;
        return new_tiny_file;
    }

    ColumnFileReaderPtr getReader(
        const DMContext &,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefinesPtr & col_defs,
        ReadTag) const override;

    void removeData(WriteBatches & wbs) const override;

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    void serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool save_schema) const override;

    PageIdU64 getDataPageId() const { return data_page_id; }

    /// WARNING: DO NOT USE THIS MEMBER FUNCTION UNLESS YOU KNOW WHAT YOU ARE DOING.
    /// This function will be refined and dropped soon.
    UInt64 getDataPageSize() const { return data_page_size; }

    Block readBlockForMinorCompaction(const PageReader & page_reader) const;

    static std::shared_ptr<ColumnFileSchema> getSchema(
        const DMContext & context,
        BlockPtr schema_block,
        ColumnFileSchemaPtr & last_schema);

    static ColumnFileTinyPtr writeColumnFile(
        const DMContext & context,
        const Block & block,
        size_t offset,
        size_t limit,
        WriteBatches & wbs,
        const CachePtr & cache = nullptr);

    static PageIdU64 writeColumnFileData(
        const DMContext & context,
        const Block & block,
        size_t offset,
        size_t limit,
        WriteBatches & wbs);

    static ColumnFilePersistedPtr deserializeMetadata(
        const DMContext & context,
        ReadBuffer & buf,
        ColumnFileSchemaPtr & last_schema);

    static ColumnFilePersistedPtr deserializeMetadata(
        const DMContext & context,
        const dtpb::ColumnFileTiny & cf_pb,
        ColumnFileSchemaPtr & last_schema);

    static ColumnFilePersistedPtr restoreFromCheckpoint(
        const LoggerPtr & parent_log,
        const DMContext & context,
        UniversalPageStoragePtr temp_ps,
        WriteBatches & wbs,
        BlockPtr schema,
        PageIdU64 data_page_id,
        size_t rows,
        size_t bytes,
        IndexInfosPtr index_infos);
    static std::tuple<ColumnFilePersistedPtr, BlockPtr> createFromCheckpoint(
        const LoggerPtr & parent_log,
        const DMContext & context,
        ReadBuffer & buf,
        UniversalPageStoragePtr temp_ps,
        const BlockPtr & last_schema,
        WriteBatches & wbs);
    static std::tuple<ColumnFilePersistedPtr, BlockPtr> createFromCheckpoint(
        const LoggerPtr & parent_log,
        const DMContext & context,
        const dtpb::ColumnFileTiny & cf_pb,
        UniversalPageStoragePtr temp_ps,
        const BlockPtr & last_schema,
        WriteBatches & wbs);

    bool mayBeFlushedFrom(ColumnFile * from_file) const override
    {
        // The current ColumnFileTiny may come from a ColumnFileInMemory (which contains data in memory)
        // or ColumnFileTiny (which contains data in PageStorage).

        if (const auto * other_tiny = from_file->tryToTinyFile(); other_tiny)
            return data_page_id == other_tiny->data_page_id;
        else if (const auto * other_in_memory = from_file->tryToInMemoryFile(); other_in_memory)
            // For ColumnFileInMemory, we just do a rough check, instead of checking byte by byte, which
            // is too expensive.
            return bytes == from_file->getBytes() && rows == from_file->getRows();
        else
            return false;
    }

    String toString() const override
    {
        String s = "{tiny_file,rows:" + DB::toString(rows) //
            + ",bytes:" + DB::toString(bytes) //
            + ",data_page_id:" + DB::toString(data_page_id) //
            + ",schema:" + (schema ? schema->toString() : "none") //
            + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + "}";
        return s;
    }
};

} // namespace DB::DM
