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

#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileIndexWriter.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/PathPool.h>

namespace DB::DM
{

DMFileIndexWriter::LocalIndexBuildInfo DMFileIndexWriter::getLocalIndexBuildInfo(
    const IndexInfosPtr & index_infos,
    const DMFiles & dm_files)
{
    assert(index_infos != nullptr);
    static constexpr double VECTOR_INDEX_SIZE_FACTOR = 1.2;

    LocalIndexBuildInfo build;
    build.indexes_to_build = std::make_shared<IndexInfos>();
    build.file_ids.reserve(dm_files.size());
    for (const auto & dmfile : dm_files)
    {
        bool any_new_index_build = false;
        for (const auto & index : *index_infos)
        {
            auto col_id = index.column_id;
            // The dmfile may be built before col_id is added. Skip build indexes for it
            if (!dmfile->isColumnExist(col_id))
                continue;

            if (dmfile->getColumnStat(col_id).index_bytes > 0)
                continue;

            any_new_index_build = true;

            auto col_stat = dmfile->getColumnStat(col_id);
            build.indexes_to_build->emplace_back(index);
            build.estimated_memory_bytes += col_stat.serialized_bytes * VECTOR_INDEX_SIZE_FACTOR;
        }

        if (any_new_index_build)
        {
            build.file_ids.emplace_back(dmfile->fileId());
        }
    }

    build.file_ids.shrink_to_fit();
    return build;
}

void DMFileIndexWriter::buildIndexForFile(const DMFilePtr & dm_file_mutable) const
{
    const auto column_defines = dm_file_mutable->getColumnDefines();
    const auto del_cd_iter = std::find_if(column_defines.cbegin(), column_defines.cend(), [](const ColumnDefine & cd) {
        return cd.id == TAG_COLUMN_ID;
    });
    RUNTIME_CHECK_MSG(
        del_cd_iter != column_defines.cend(),
        "Cannot find del_mark column, file={}",
        dm_file_mutable->path());

    // read_columns are: DEL_MARK, COL_A, COL_B, ...
    // index_builders are: COL_A, COL_B, ...

    ColumnDefines read_columns{*del_cd_iter};
    read_columns.reserve(options.index_infos->size() + 1);

    std::vector<VectorIndexBuilderPtr> index_builders;
    index_builders.reserve(options.index_infos->size());

    // The caller should avoid building index for the same column multiple times.
    for (const auto & index_info : *options.index_infos)
    {
        const auto cd_iter = std::find_if(column_defines.cbegin(), column_defines.cend(), [&](const auto & cd) {
            return cd.id == index_info.column_id;
        });
        RUNTIME_CHECK_MSG(
            cd_iter != column_defines.cend(),
            "Cannot find column_id={} in file={}",
            index_info.column_id,
            dm_file_mutable->path());

        // Index already built. We don't allow. The caller should filter away,
        RUNTIME_CHECK(dm_file_mutable->getColumnStat(index_info.column_id).index_bytes == 0, index_info.column_id);

        read_columns.push_back(*cd_iter);
        index_builders.push_back(VectorIndexBuilder::create(index_info.index_definition));
    }

    if (index_builders.empty())
    {
        // No index to build.
        return;
    }

    DMFileV3IncrementWriter::Options iw_options{
        .dm_file = dm_file_mutable,
        .file_provider = options.file_provider,
        .write_limiter = options.write_limiter,
        .path_pool = options.path_pool,
        .disagg_ctx = options.disagg_ctx,
    };
    auto iw = DMFileV3IncrementWriter::create(iw_options);

    // TODO: Maybe using DMFileReader directly is better because it doesn't need db_context.
    DMFileBlockInputStreamBuilder read_stream_builder(options.db_context);
    auto scan_context = std::make_shared<ScanContext>();

    // Note: We use range::newAll to build index for all data in dmfile, because the index is file-level.
    auto read_stream = read_stream_builder.build(
        dm_file_mutable,
        read_columns,
        {RowKeyRange::newAll(options.is_common_handle, options.rowkey_column_size)},
        scan_context);

    // Read all blocks and build index
    while (true)
    {
        auto block = read_stream->read();
        if (!block)
            break;

        RUNTIME_CHECK(block.columns() == read_columns.size());
        RUNTIME_CHECK(block.getByPosition(0).column_id == TAG_COLUMN_ID);

        auto del_mark_col = block.safeGetByPosition(0).column;
        RUNTIME_CHECK(del_mark_col != nullptr);
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());
        RUNTIME_CHECK(del_mark != nullptr);

        for (size_t col_idx = 0, col_idx_max = index_builders.size(); col_idx < col_idx_max; ++col_idx)
        {
            const auto & index_builder = index_builders[col_idx];
            const auto & col_with_type_and_name = block.safeGetByPosition(col_idx + 1);
            RUNTIME_CHECK(col_with_type_and_name.column_id == read_columns[col_idx + 1].id);
            const auto & col = col_with_type_and_name.column;
            index_builder->addBlock(*col, del_mark);
        }
    }

    // Write down the index
    for (size_t col_idx = 0, col_idx_max = index_builders.size(); col_idx < col_idx_max; col_idx++)
    {
        const auto & index_builder = index_builders[col_idx];
        const auto & cd = read_columns[col_idx + 1];

        // Save index and update column stats
        auto callback = [&](const IDataType::SubstreamPath & substream_path) -> void {
            if (IDataType::isNullMap(substream_path) || IDataType::isArraySizes(substream_path))
                return;

            const auto stream_name = DMFile::getFileNameBase(cd.id, substream_path);
            const auto index_file_name = colIndexFileName(stream_name);
            const auto index_path = iw->localPath() + "/" + index_file_name;
            index_builder->save(index_path);

            auto & col_stat = dm_file_mutable->meta->getColumnStats().at(cd.id);
            col_stat.index_bytes = Poco::File(index_path).getSize();
            // Memorize what kind of vector index it is, so that we can correctly restore it when reading.
            col_stat.vector_index.emplace();
            col_stat.vector_index->set_index_kind(tipb::VectorIndexKind_Name(index_builder->definition->kind));
            col_stat.vector_index->set_distance_metric(
                tipb::VectorDistanceMetric_Name(index_builder->definition->distance_metric));
            col_stat.vector_index->set_dimensions(index_builder->definition->dimension);

            iw->include(index_file_name);
        };
        cd.type->enumerateStreams(callback);
    }

    dm_file_mutable->meta->bumpMetaVersion();
    iw->finalize(); // Note: There may be S3 uploads here.
}

DMFiles DMFileIndexWriter::build() const
{
    // Create a clone of existing DMFile instances by using DMFile::restore,
    // because later we will mutate some fields and persist these mutations.
    DMFiles cloned_dm_files{};

    auto delegate = options.path_pool->getStableDiskDelegator();
    for (const auto & dm_file : options.dm_files)
    {
        if (!options.disagg_ctx || !options.disagg_ctx->remote_data_store)
        {
            RUNTIME_CHECK(dm_file->parentPath() == delegate.getDTFilePath(dm_file->fileId()));
        }

        auto new_dmfile = DMFile::restore(
            options.file_provider,
            dm_file->fileId(),
            dm_file->pageId(),
            dm_file->parentPath(),
            DMFileMeta::ReadMode::all(),
            dm_file->metaVersion());

        cloned_dm_files.push_back(new_dmfile);
    }

    for (const auto & cloned_dmfile : cloned_dm_files)
    {
        buildIndexForFile(cloned_dmfile);
        // TODO: including the new index bytes in the file size.
        // auto res = dm_context.path_pool->getStableDiskDelegator().updateDTFileSize(
        //     new_dmfile->fileId(),
        //     new_dmfile->getBytesOnDisk());
        // RUNTIME_CHECK_MSG(res, "update dt file size failed, path={}", new_dmfile->path());
    }

    return cloned_dm_files;
}

} // namespace DB::DM
