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

#pragma once

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetInputStream.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyVectorIndexReader.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/VectorIndexBlockInputStream.h>


namespace DB::DM
{

class ColumnFileSetWithVectorIndexInputStream : public VectorIndexBlockInputStream
{
private:
    ColumnFileSetReader reader;

    std::vector<ColumnFileReaderPtr>::iterator cur_column_file_reader;
    size_t read_rows = 0;

    const IColumnFileDataProviderPtr data_provider;
    const ANNQueryInfoPtr ann_query_info;
    const BitmapFilterView valid_rows;
    // Global vector index cache
    const VectorIndexCachePtr vec_index_cache;
    const ColumnDefine vec_cd;
    const ColumnDefinesPtr rest_col_defs;

    // Set after load(). Top K search results in files with vector index.
    std::vector<VectorIndexViewer::Key> sorted_results;
    std::vector<ColumnFileTinyVectorIndexReaderPtr> tiny_readers;

    const ColumnFiles & column_files;

    const Block header;
    IColumn::Filter filter;

    bool loaded = false;

public:
    ColumnFileSetWithVectorIndexInputStream(
        const DMContext & context_,
        const ColumnFileSetSnapshotPtr & delta_snap_,
        const ColumnDefinesPtr & col_defs_,
        const RowKeyRange & segment_range_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ANNQueryInfoPtr & ann_query_info_,
        const BitmapFilterView && valid_rows_,
        ColumnDefine && vec_cd_,
        const ColumnDefinesPtr & rest_col_defs_,
        ReadTag read_tag_)
        : reader(context_, delta_snap_, col_defs_, segment_range_, read_tag_)
        , data_provider(data_provider_)
        , ann_query_info(ann_query_info_)
        , valid_rows(std::move(valid_rows_))
        , vec_index_cache(context_.global_context.getVectorIndexCache())
        , vec_cd(std::move(vec_cd_))
        , rest_col_defs(rest_col_defs_)
        , column_files(reader.snapshot->getColumnFiles())
        , header(toEmptyBlock(*(reader.col_defs)))
    {
        cur_column_file_reader = reader.column_file_readers.begin();
    }

    static SkippableBlockInputStreamPtr tryBuild(
        const DMContext & context,
        const ColumnFileSetSnapshotPtr & delta_snap,
        const ColumnDefinesPtr & col_defs,
        const RowKeyRange & segment_range_,
        const IColumnFileDataProviderPtr & data_provider,
        const ANNQueryInfoPtr & ann_query_info,
        const BitmapFilterPtr & bitmap_filter,
        size_t offset,
        ReadTag read_tag_);

    String getName() const override { return "ColumnFileSetWithVectorIndex"; }
    Block getHeader() const override { return header; }

    Block read() override;

    std::vector<VectorIndexViewer::SearchResult> load() override;

    void setSelectedRows(const std::span<const UInt32> & selected_rows) override;

private:
    Block readOtherColumns();

    void toNextFile(size_t current_file_index, size_t current_file_rows);
};

} // namespace DB::DM
