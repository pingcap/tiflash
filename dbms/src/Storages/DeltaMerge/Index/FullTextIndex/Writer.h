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
#include <Common/config.h>

#if ENABLE_CLARA
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Writer_fwd.h>
#include <Storages/DeltaMerge/Index/LocalIndexWriter.h>
#include <TiDB/Schema/FullTextIndex.h>
#include <clara_fts/src/index_writer.rs.h>

namespace DB::DM
{

// Light wrapper, adds Block IO and metrics.
class FullTextIndexWriterInMemory : public LocalIndexWriterInMemory
{
public:
    explicit FullTextIndexWriterInMemory(IndexID index_id, const TiDB::FullTextIndexDefinitionPtr & def_fts)
        : LocalIndexWriterInMemory(index_id)
        , inner(ClaraFTS::new_memory_index_writer(def_fts->parser_type))
    {}

    static FullTextIndexWriterInMemoryPtr create(IndexID index_id, const TiDB::FullTextIndexDefinitionPtr & def_fts)
    {
        return std::make_shared<FullTextIndexWriterInMemory>(index_id, def_fts);
    }

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;

protected:
    // FIXME (@Lloyd-Pottiger): For FullTextIndexWriterInMemory this can be only called once.
    void saveToBuffer(WriteBuffer & write_buf) override
    {
        // FIXME (@Lloyd-Pottiger): Avoid this extra allocation and copy.
        auto vec = inner->finalize();
        write_buf.write(reinterpret_cast<const char *>(vec.data()), vec.size());
    }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override
    {
        auto * pb_ft_idx = pb_idx->mutable_fulltext_index();
        pb_ft_idx->set_format_version(0);
    }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::FULLTEXT_INDEX; }

private:
    rust::Box<ClaraFTS::IndexWriterInMemory> inner;
};

// Light wrapper, adds Block IO and metrics.
class FullTextIndexWriterOnDisk : public LocalIndexWriterOnDisk
{
public:
    explicit FullTextIndexWriterOnDisk(
        IndexID index_id,
        const TiDB::FullTextIndexDefinitionPtr & def_fts,
        std::string_view index_file)
        : LocalIndexWriterOnDisk(index_id, index_file)
        , inner(ClaraFTS::new_disk_index_writer(def_fts->parser_type, rust::Str(index_file.data(), index_file.size())))
    {}

    static FullTextIndexWriterOnDiskPtr create(
        IndexID index_id,
        const TiDB::FullTextIndexDefinitionPtr & def_fts,
        std::string_view index_file)
    {
        return std::make_shared<FullTextIndexWriterOnDisk>(index_id, def_fts, index_file);
    }

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;

protected:
    // FIXME (@Lloyd-Pottiger): For FullTextIndexWriterOnDisk this can be only called once.
    void saveToFile() override { inner->finalize(); }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override
    {
        auto * pb_ft_idx = pb_idx->mutable_fulltext_index();
        pb_ft_idx->set_format_version(0);
    }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::FULLTEXT_INDEX; }

private:
    rust::Box<ClaraFTS::IndexWriterOnDisk> inner;
};

} // namespace DB::DM
#endif
