// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Index/InvertedIndex/CommonUtil.h>
#include <Storages/DeltaMerge/Index/LocalIndexWriter.h>

namespace DB::DM
{

/// Builds a InvertedIndex in memory.
template <typename T>
class InvertedIndexWriter : public LocalIndexWriter
{
public:
    using Key = T;
    using RowID = InvertedIndex::RowID;

public:
    explicit InvertedIndexWriter(IndexID index_id)
        : LocalIndexWriter(index_id)
    {}

    ~InvertedIndexWriter() override = default;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;
    void saveToFile(std::string_view path) const override;
    void saveToBuffer(WriteBuffer & write_buf) const override;
    void saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const override;

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::INVERTED_INDEX; }

public:
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.
    std::map<Key, std::vector<RowID>> index;
    double total_duration = 0;
    mutable size_t uncompressed_size = 0;
};

LocalIndexWriterPtr createInvertedIndexWriter(IndexID index_id, const TiDB::InvertedIndexDefinitionPtr & definition);

} // namespace DB::DM
