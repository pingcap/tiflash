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

#include <Columns/ColumnsCommon.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetWithVectorIndexInputStream.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Filter/WithANNQueryInfo.h>


namespace DB::DM
{

ColumnFileSetInputStreamPtr ColumnFileSetWithVectorIndexInputStream::tryBuild(
    const DMContext & context,
    const ColumnFileSetSnapshotPtr & delta_snap,
    const ColumnDefinesPtr & col_defs,
    const RowKeyRange & segment_range_,
    const IColumnFileDataProviderPtr & data_provider,
    const RSOperatorPtr & rs_operator,
    const BitmapFilterPtr & bitmap_filter,
    size_t offset,
    ReadTag read_tag_)
{
    auto fallback = [&]() {
        return std::make_shared<ColumnFileSetInputStream>(context, delta_snap, col_defs, segment_range_, read_tag_);
    };

    if (rs_operator == nullptr || bitmap_filter == nullptr)
        return fallback();

    auto filter_with_ann = std::dynamic_pointer_cast<WithANNQueryInfo>(rs_operator);
    if (filter_with_ann == nullptr)
        return fallback();

    auto ann_query_info = filter_with_ann->ann_query_info;
    if (!ann_query_info)
        return fallback();

    // Fast check: ANNQueryInfo is available in the whole read path. However we may not reading vector column now.
    bool is_matching_ann_query = false;
    for (const auto & cd : *col_defs)
    {
        if (cd.id == ann_query_info->column_id())
        {
            is_matching_ann_query = true;
            break;
        }
    }
    if (!is_matching_ann_query)
        return fallback();

    std::optional<ColumnDefine> vec_cd;
    auto rest_columns = std::make_shared<ColumnDefines>();
    rest_columns->reserve(col_defs->size() - 1);
    for (const auto & cd : *col_defs)
    {
        if (cd.id == ann_query_info->column_id())
            vec_cd.emplace(cd);
        else
            rest_columns->emplace_back(cd);
    }

    // No vector index column is specified, just use the normal logic.
    if (!vec_cd.has_value())
        return fallback();

    // All check passed. Let's read via vector index.
    return std::make_shared<ColumnFileSetWithVectorIndexInputStream>(
        context,
        delta_snap,
        col_defs,
        segment_range_,
        data_provider,
        ann_query_info,
        BitmapFilterView(bitmap_filter, offset, delta_snap->getRows()),
        std::move(*vec_cd),
        rest_columns,
        read_tag_);
}

Block ColumnFileSetWithVectorIndexInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    if (return_filter)
        return readImpl(res_filter);

    // If return_filter == false, we must filter by ourselves.

    FilterPtr filter = nullptr;
    auto res = readImpl(filter);
    if (filter != nullptr)
    {
        auto passed_count = countBytesInFilter(*filter);
        for (auto & col : res)
            col.column = col.column->filter(*filter, passed_count);
    }
    // filter == nullptr means all rows are valid and no need to filter.
    return res;
}

Block ColumnFileSetWithVectorIndexInputStream::readOtherColumns()
{
    auto reset_column_file_reader = (*cur_column_file_reader)->createNewReader(rest_col_defs, ReadTag::Query);
    Block block = reset_column_file_reader->readNextBlock();
    return block;
}

Block ColumnFileSetWithVectorIndexInputStream::readImpl(FilterPtr & res_filter)
{
    load();

    while (cur_column_file_reader != reader.column_file_readers.end())
    {
        // Skip ColumnFileDeleteRange
        if (*cur_column_file_reader == nullptr)
        {
            ++cur_column_file_reader;
            continue;
        }
        auto current_file_index = std::distance(reader.column_file_readers.begin(), cur_column_file_reader);
        // If has index, we can read the column by vector index.
        if (tiny_readers[current_file_index] != nullptr)
        {
            const auto file_rows = column_files[current_file_index]->getRows();
            auto to_next_file = [&]() {
                (*cur_column_file_reader).reset();
                ++cur_column_file_reader;
                read_rows += file_rows;
                tiny_readers[current_file_index].reset();
            };

            auto selected_row_begin = std::lower_bound(
                selected_rows.cbegin(),
                selected_rows.cend(),
                read_rows,
                [](const auto & row, UInt32 offset) { return row.key < offset; });
            auto selected_row_end = std::lower_bound(
                selected_row_begin,
                selected_rows.cend(),
                read_rows + file_rows,
                [](const auto & row, UInt32 offset) { return row.key < offset; });
            size_t selected_rows = std::distance(selected_row_begin, selected_row_end);
            // If all rows are filtered out, skip this file.
            if (selected_rows == 0)
            {
                to_next_file();
                continue;
            }

            // read vector type column by vector index
            auto tiny_reader = tiny_readers[current_file_index];
            auto vec_column = vec_cd.type->createColumn();
            const std::span file_selected_rows{selected_row_begin, selected_row_end};
            tiny_reader->read(vec_column, file_selected_rows, read_rows, file_rows);
            assert(vec_column->size() == file_rows);

            Block block;
            if (!rest_col_defs->empty())
            {
                block = readOtherColumns();
                assert(block.rows() == vec_column->size());
            }

            auto index = header.getPositionByName(vec_cd.name);
            block.insert(index, ColumnWithTypeAndName(std::move(vec_column), vec_cd.type, vec_cd.name));

            // Fill res_filter
            if (selected_rows == file_rows)
            {
                res_filter = nullptr;
            }
            else
            {
                filter.clear();
                filter.resize_fill(file_rows, 0);
                for (const auto & [rowid, _] : file_selected_rows)
                    filter[rowid - read_rows] = 1;
                res_filter = &filter;
            }

            // All rows in this ColumnFileTiny have been read.
            block.setStartOffset(read_rows);
            to_next_file();
            return block;
        }
        auto block = (*cur_column_file_reader)->readNextBlock();
        if (block)
        {
            block.setStartOffset(read_rows);
            read_rows += block.rows();
            res_filter = nullptr;
            return block;
        }
        else
        {
            (*cur_column_file_reader).reset();
            ++cur_column_file_reader;
        }
    }
    return {};
}

void ColumnFileSetWithVectorIndexInputStream::load()
{
    if (loaded)
        return;

    tiny_readers.reserve(column_files.size());
    UInt32 precedes_rows = 0;
    for (const auto & column_file : column_files)
    {
        if (auto * tiny_file = column_file->tryToTinyFile();
            tiny_file && tiny_file->hasIndex(ann_query_info->index_id()))
        {
            auto tiny_reader = std::make_shared<ColumnFileTinyVectorIndexReader>(
                *tiny_file,
                data_provider,
                ann_query_info,
                valid_rows.createSubView(precedes_rows, tiny_file->getRows()),
                vec_cd,
                vec_index_cache);
            auto sr = tiny_reader->load();
            for (auto & row : sr)
                row.key += precedes_rows;
            selected_rows.insert(selected_rows.end(), sr.begin(), sr.end());
            tiny_readers.push_back(tiny_reader);
            // avoid virutal function call
            precedes_rows += tiny_file->getRows();
        }
        else
        {
            tiny_readers.push_back(nullptr);
            precedes_rows += column_file->getRows();
        }
    }
    // Sort by distance
    std::sort(selected_rows.begin(), selected_rows.end(), [](const auto & lhs, const auto & rhs) {
        return lhs.distance < rhs.distance;
    });
    // Only keep the top_k rows.
    if (selected_rows.size() > ann_query_info->top_k())
        selected_rows.resize(ann_query_info->top_k());
    // Sort by key again.
    std::sort(selected_rows.begin(), selected_rows.end(), [](const auto & lhs, const auto & rhs) {
        return lhs.key < rhs.key;
    });

    loaded = true;
}

} // namespace DB::DM
