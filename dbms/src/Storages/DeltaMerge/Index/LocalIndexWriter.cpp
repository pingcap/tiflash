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

#include <Poco/File.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Writer.h>
#include <Storages/DeltaMerge/Index/LocalIndexWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Writer.h>

namespace DB::DM
{

LocalIndexWriterPtr LocalIndexWriter::createInMemory(const LocalIndexInfo & index_info)
{
    switch (index_info.kind)
    {
    case TiDB::ColumnarIndexKind::Vector:
        return VectorIndexWriterInternal::createInMemory(index_info.index_id, index_info.def_vector_index);
    case TiDB::ColumnarIndexKind::Inverted:
        return createInvertedIndexWriter(index_info.index_id, index_info.def_inverted_index);
    default:
        RUNTIME_CHECK_MSG(false, "Unsupported index kind: {}", magic_enum::enum_name(index_info.kind));
    }
}

LocalIndexWriterPtr LocalIndexWriter::createOnDisk(std::string_view index_file, const LocalIndexInfo & index_info)
{
    switch (index_info.kind)
    {
    case TiDB::ColumnarIndexKind::Vector:
        return VectorIndexWriterInternal::createOnDisk(index_file, index_info.index_id, index_info.def_vector_index);
    case TiDB::ColumnarIndexKind::Inverted:
        return createInvertedIndexWriter(index_info.index_id, index_info.def_inverted_index);
    default:
        RUNTIME_CHECK_MSG(false, "Unsupported index kind: {}", magic_enum::enum_name(index_info.kind));
    }
}

dtpb::IndexFilePropsV2 LocalIndexWriter::finalize(std::string_view path) const
{
    saveToFile(path);
    dtpb::IndexFilePropsV2 pb_idx;
    pb_idx.set_index_id(index_id);
    pb_idx.set_file_size(Poco::File(path.data()).getSize());
    pb_idx.set_kind(kind());
    saveFilePros(&pb_idx);
    return pb_idx;
}

dtpb::IndexFilePropsV2 LocalIndexWriter::finalize(WriteBuffer & write_buf) const
{
    saveToBuffer(write_buf);
    dtpb::IndexFilePropsV2 pb_idx;
    pb_idx.set_index_id(index_id);
    pb_idx.set_file_size(write_buf.count());
    pb_idx.set_kind(kind());
    saveFilePros(&pb_idx);
    return pb_idx;
}

} // namespace DB::DM
