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

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>

namespace DB::DM
{

class ColumnFileTinyReader : public ColumnFileReader
{
private:
    const ColumnFileTiny & tiny_file;
    const IColumnFileDataProviderPtr data_provider;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnFileTinyReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_,
        const Columns & cols_data_cache_)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {}

    ColumnFileTinyReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , col_defs(col_defs_)
    {}

    /// This is a ugly hack to fast return PK & Version column.
    ColumnPtr getPKColumn();
    ColumnPtr getVersionColumn();

    std::pair<size_t, size_t> readRows(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range) override;

    Block readNextBlock() override;

    size_t skipNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag) override;
};

} // namespace DB::DM
