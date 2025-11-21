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

#include <Common/config.h> // For ENABLE_CLARA
#include <Poco/File.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Writer.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/Index/LocalIndexWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Writer.h>
#include <TiDB/Schema/TiDB.h>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/Index/FullTextIndex/Writer.h>
#endif

namespace DB::DM
{

LocalIndexWriterInMemoryPtr LocalIndexWriter::createInMemory(const LocalIndexInfo & index_info)
{
    switch (index_info.kind)
    {
    case TiDB::ColumnarIndexKind::Vector:
        return std::make_shared<VectorIndexWriterInMemory>(index_info.index_id, index_info.def_vector_index);
    case TiDB::ColumnarIndexKind::Inverted:
        return createInMemoryInvertedIndexWriter(index_info.index_id, index_info.def_inverted_index);
#if ENABLE_CLARA
    case TiDB::ColumnarIndexKind::FullText:
        return FullTextIndexWriterInMemory::create(index_info.index_id, index_info.def_fulltext_index);
#endif
    default:
        RUNTIME_CHECK_MSG(false, "Unsupported index kind: {}", magic_enum::enum_name(index_info.kind));
    }
}

LocalIndexWriterOnDiskPtr LocalIndexWriter::createOnDisk(std::string_view index_file, const LocalIndexInfo & index_info)
{
    switch (index_info.kind)
    {
    case TiDB::ColumnarIndexKind::Vector:
        return std::make_shared<VectorIndexWriterOnDisk>(index_info.index_id, index_file, index_info.def_vector_index);
    case TiDB::ColumnarIndexKind::Inverted:
        return createOnDiskInvertedIndexWriter(index_info.index_id, index_file, index_info.def_inverted_index);
#if ENABLE_CLARA
    case TiDB::ColumnarIndexKind::FullText:
        return FullTextIndexWriterOnDisk::create(index_info.index_id, index_info.def_fulltext_index, index_file);
#endif
    default:
        RUNTIME_CHECK_MSG(false, "Unsupported index kind: {}", magic_enum::enum_name(index_info.kind));
    }
}

dtpb::IndexFilePropsV2 LocalIndexWriterOnDisk::finalize()
{
    saveToFile();
    dtpb::IndexFilePropsV2 pb_idx;
    pb_idx.set_index_id(index_id);
    pb_idx.set_file_size(Poco::File(index_file.data()).getSize());
    pb_idx.set_kind(kind());
    saveFileProps(&pb_idx);
    return pb_idx;
}

dtpb::IndexFilePropsV2 LocalIndexWriterInMemory::finalize(
    WriteBuffer & write_buf,
    std::function<size_t()> get_materialized_size)
{
    saveToBuffer(write_buf);
    dtpb::IndexFilePropsV2 pb_idx;
    pb_idx.set_index_id(index_id);
    pb_idx.set_file_size(get_materialized_size());
    pb_idx.set_kind(kind());
    saveFileProps(&pb_idx);
    return pb_idx;
}

} // namespace DB::DM
