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

#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

class VectorIndexBlockInputStream : public SkippableBlockInputStream
{
public:
    bool getSkippedRows(size_t &) override
    {
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support getSkippedRows");
    }

    size_t skipNextBlock() override
    {
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support skipNextBlock");
    }

    Block readWithFilter(const IColumn::Filter &) override
    {
        // We don't support the normal late materialization, because
        // we are already doing it.
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support late materialization");
    }

    // Load vector index and search results.
    // Return the rowids of the selected rows.
    virtual std::vector<VectorIndexViewer::SearchResult> load() = 0;

    // Set the real selected rows offset (local offset). This is used to update Packs/ColumnFiles to read.
    // For example, DMFile should update DMFilePackFilter, only packs with selected rows will be read.
    virtual void setSelectedRows(const std::span<const UInt32> & selected_rows) = 0;
};

using VectorIndexBlockInputStreamPtr = std::shared_ptr<VectorIndexBlockInputStream>;

} // namespace DB::DM
