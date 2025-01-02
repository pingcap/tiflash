// Copyright 2024 PingCAP, Inc.
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

#include <IO/Compression/CompressedWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyVectorIndexWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>


namespace DB::ErrorCodes
{
extern const int ABORTED;
} // namespace DB::ErrorCodes

namespace DB::DM
{

ColumnFileTinyVectorIndexWriter::LocalIndexBuildInfo ColumnFileTinyVectorIndexWriter::getLocalIndexBuildInfo(
    const LocalIndexInfosSnapshot & index_infos,
    const ColumnFilePersistedSetPtr & file_set)
{
    assert(index_infos != nullptr);

    LocalIndexBuildInfo build;
    build.indexes_to_build = std::make_shared<LocalIndexInfos>();
    build.file_ids.reserve(file_set->getColumnFileCount());
    for (const auto & file : file_set->getFiles())
    {
        auto * tiny_file = file->tryToTinyFile();
        if (!tiny_file)
            continue;

        bool any_new_index_build = false;
        for (const auto & index : *index_infos)
        {
            auto schema = tiny_file->getSchema();
            assert(schema != nullptr);
            // The ColumnFileTiny may be built before col_id is added. Skip build indexes for it
            if (!schema->getColIdToOffset().contains(index.column_id))
                continue;

            // Index already built, skip
            if (tiny_file->hasIndex(index.index_id))
                continue;

            any_new_index_build = true;
            // FIXME: the memory usage is not accurate, but it's fine for now.
            build.estimated_memory_bytes += tiny_file->getBytes();

            // Avoid duplicate index build
            if (std::find(build.index_ids.begin(), build.index_ids.end(), index.index_id) == build.index_ids.end())
            {
                build.indexes_to_build->emplace_back(index);
                build.index_ids.emplace_back(index.index_id);
            }
        }

        if (any_new_index_build)
        {
            build.file_ids.emplace_back(LocalIndexerScheduler::ColumnFileTinyID(tiny_file->getDataPageId()));
        }
    }

    build.file_ids.shrink_to_fit();
    return build;
}

ColumnFileTinyPtr ColumnFileTinyVectorIndexWriter::buildIndexForFile(
    const ColumnDefines & column_defines,
    const ColumnDefine & del_cd,
    const ColumnFileTiny * file,
    ProceedCheckFn should_proceed) const
{
    // read_columns are: DEL_MARK, COL_A, COL_B, ...
    // index_builders are: COL_A, COL_B, ...

    ColumnDefinesPtr read_columns = std::make_shared<ColumnDefines>();
    read_columns->reserve(options.index_infos->size() + 1);
    read_columns->push_back(del_cd);

    std::unordered_map<ColId, std::vector<VectorIndexBuilderPtr>> index_builders;

    std::unordered_map<ColId, std::vector<LocalIndexInfo>> col_indexes;
    for (const auto & index_info : *options.index_infos)
    {
        if (index_info.type != IndexType::Vector)
            continue;
        col_indexes[index_info.column_id].emplace_back(index_info);
    }

    for (const auto & [col_id, index_infos] : col_indexes)
    {
        // Make sure the column_id is in the schema.
        const auto cd_iter = std::find_if( //
            column_defines.cbegin(),
            column_defines.cend(),
            [col_id = col_id](const auto & cd) { return cd.id == col_id; });
        RUNTIME_CHECK_MSG(
            cd_iter != column_defines.cend(),
            "Cannot find column_id={} in file_id={}",
            col_id,
            file->getDataPageId());

        for (const auto & idx_info : index_infos)
        {
            // Just skip if the index is already built
            if (file->hasIndex(idx_info.index_id))
                continue;

            index_builders[col_id].emplace_back(
                VectorIndexBuilder::create(idx_info.index_id, idx_info.index_definition));
        }
        read_columns->push_back(*cd_iter);
    }

    // If no index to build, return nullptr
    if (read_columns->size() == 1 || index_builders.empty())
        return nullptr;

    // Read all blocks and build index
    // TODO: read one column at a time to reduce peak memory usage.
    const size_t num_cols = read_columns->size();
    ColumnFileTinyReader reader(*file, options.data_provider, read_columns);
    while (true)
    {
        if (!should_proceed())
            throw Exception(ErrorCodes::ABORTED, "Index build is interrupted");

        auto block = reader.readNextBlock();
        if (!block)
            break;

        RUNTIME_CHECK(block.columns() == read_columns->size());
        RUNTIME_CHECK(block.getByPosition(0).column_id == MutSup::delmark_col_id);

        auto del_mark_col = block.safeGetByPosition(0).column;
        RUNTIME_CHECK(del_mark_col != nullptr);
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
        RUNTIME_CHECK(del_mark != nullptr);

        for (size_t col_idx = 1; col_idx < num_cols; ++col_idx)
        {
            const auto & col_with_type_and_name = block.safeGetByPosition(col_idx);
            RUNTIME_CHECK(col_with_type_and_name.column_id == read_columns->at(col_idx).id);
            const auto & col = col_with_type_and_name.column;
            for (const auto & index_builder : index_builders[read_columns->at(col_idx).id])
            {
                index_builder->addBlock(*col, del_mark, should_proceed);
            }
        }
    }

    // Save index to PageStorage
    auto index_infos = std::make_shared<ColumnFileTiny::IndexInfos>();
    for (size_t col_idx = 1; col_idx < num_cols; ++col_idx)
    {
        const auto & cd = read_columns->at(col_idx);
        for (const auto & index_builder : index_builders[cd.id])
        {
            auto index_page_id = options.storage_pool->newLogPageId();
            MemoryWriteBuffer write_buf;
            CompressedWriteBuffer compressed(write_buf);
            index_builder->saveToBuffer(compressed);
            compressed.next();
            auto data_size = write_buf.count();
            auto buf = write_buf.tryGetReadBuffer();
            // ColumnFileDataProviderRNLocalPageCache currently does not support read data with fields
            options.wbs.log.putPage(index_page_id, 0, buf, data_size, {data_size});

            dtpb::VectorIndexFileProps vector_index;
            vector_index.set_index_id(index_builder->index_id);
            vector_index.set_index_bytes(data_size);
            vector_index.set_index_kind(tipb::VectorIndexKind_Name(index_builder->definition->kind));
            vector_index.set_distance_metric(
                tipb::VectorDistanceMetric_Name(index_builder->definition->distance_metric));
            vector_index.set_dimensions(index_builder->definition->dimension);
            index_infos->emplace_back(index_page_id, vector_index);
        }
    }

    if (file->index_infos)
        file->index_infos->insert(file->index_infos->end(), index_infos->begin(), index_infos->end());

    options.wbs.writeLogAndData();
    // Note: The id of the file cannot be changed, otherwise minor compaction will fail.
    // So we just clone the file with new index info.
    return file->cloneWith(file->getDataPageId(), index_infos);
}

ColumnFileTinys ColumnFileTinyVectorIndexWriter::build(ProceedCheckFn should_proceed) const
{
    ColumnFileTinys new_files;
    new_files.reserve(options.files.size());
    ColumnDefines column_defines;
    ColumnDefine del_cd;
    for (const auto & file : options.files)
    {
        // Only build index for ColumnFileTiny
        const auto * tiny_file = file->tryToTinyFile();
        if (!tiny_file)
            continue;
        if (column_defines.empty())
        {
            auto schema = tiny_file->getSchema();
            column_defines = getColumnDefinesFromBlock(schema->getSchema());
            const auto del_cd_iter
                = std::find_if(column_defines.cbegin(), column_defines.cend(), [](const ColumnDefine & cd) {
                      return cd.id == MutSup::delmark_col_id;
                  });
            RUNTIME_CHECK_MSG(
                del_cd_iter != column_defines.cend(),
                "Cannot find del_mark column, file_id={}",
                tiny_file->getDataPageId());
            del_cd = *del_cd_iter;
        }
        if (auto new_file = buildIndexForFile(column_defines, del_cd, tiny_file, should_proceed); new_file)
            new_files.push_back(new_file);
    }
    new_files.shrink_to_fit();
    return new_files;
}

} // namespace DB::DM
