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

template <typename T>
class InvertedIndexWriterInternal
{
public:
    using Key = T;
    using RowID = InvertedIndex::RowID;

public:
    ~InvertedIndexWriterInternal();

    using ProceedCheckFn = LocalIndexWriter::ProceedCheckFn;
    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed);

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const;
    void saveToBuffer(WriteBuffer & write_buf) const;

public:
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.
    std::map<Key, std::vector<RowID>> index;
    mutable double total_duration = 0;
    mutable size_t uncompressed_size = 0;
};

template <typename T>
class InvertedIndexWriterInMemory : public LocalIndexWriterInMemory
{
public:
    explicit InvertedIndexWriterInMemory(IndexID index_id)
        : LocalIndexWriterInMemory(index_id)
        , writer()
    {}

    void saveToBuffer(WriteBuffer & write_buf) override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override
    {
        writer.addBlock(column, del_mark, should_proceed);
    }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override { writer.saveFileProps(pb_idx); }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::INVERTED_INDEX; }

private:
    InvertedIndexWriterInternal<T> writer;
};

// FIXME: The building process is still in-memory. Only the index file is saved to disk.
template <typename T>
class InvertedIndexWriterOnDisk : public LocalIndexWriterOnDisk
{
public:
    explicit InvertedIndexWriterOnDisk(IndexID index_id, std::string_view index_file)
        : LocalIndexWriterOnDisk(index_id, index_file)
        , writer()
    {}

    void saveToFile() const override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override
    {
        writer.addBlock(column, del_mark, should_proceed);
    }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override { writer.saveFileProps(pb_idx); }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::INVERTED_INDEX; }

private:
    InvertedIndexWriterInternal<T> writer;
};


LocalIndexWriterOnDiskPtr createOnDiskInvertedIndexWriter(
    IndexID index_id,
    std::string_view index_file,
    const TiDB::InvertedIndexDefinitionPtr & definition);

LocalIndexWriterInMemoryPtr createInMemoryInvertedIndexWriter(
    IndexID index_id,
    const TiDB::InvertedIndexDefinitionPtr & definition);

} // namespace DB::DM
