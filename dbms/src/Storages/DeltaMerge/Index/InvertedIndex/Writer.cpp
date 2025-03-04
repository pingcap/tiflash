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

#include <Columns/ColumnNullable.h>
#include <Common/Stopwatch.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Writer.h>

#include <ext/scope_guard.h>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
} // namespace DB::ErrorCodes

namespace DB::DM
{

template <typename T>
void InvertedIndexWriter<T>::addBlock(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    ProceedCheckFn should_proceed)
{
    // Note: column may be nullable.
    const bool is_nullable = column.isColumnNullable();
    const auto * col_vector
        = is_nullable ? checkAndGetNestedColumn<ColumnVector<T>>(&column) : checkAndGetColumn<ColumnVector<T>>(&column);
    RUNTIME_CHECK_MSG(col_vector, "ColumnVector is expected, get: {}, T: {}", column.getName(), typeid(T).name());
    const auto & col_data = col_vector->getData();

    const auto * null_map = is_nullable ? &(checkAndGetColumn<ColumnNullable>(&column)->getNullMapData()) : nullptr;
    const auto * del_mark_data = del_mark ? &(del_mark->getData()) : nullptr;

    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    Stopwatch w_proceed_check(CLOCK_MONOTONIC_COARSE);

    for (size_t i = 0; i < col_data.size(); ++i)
    {
        auto row_offset = added_rows;
        ++added_rows;

        if (unlikely(i % 100 == 0 && w_proceed_check.elapsedSeconds() > 0.5))
        {
            // The check of should_proceed could be non-trivial, so do it not too often.
            w_proceed_check.restart();
            if (!should_proceed())
                throw Exception(ErrorCodes::ABORTED, "Index build is interrupted");
        }

        // Ignore rows with del_mark, as the column values are not meaningful.
        if (del_mark_data != nullptr && (*del_mark_data)[i])
            continue;

        // Ignore NULL values, as they are not meaningful to store in index.
        if (null_map && (*null_map)[i])
            continue;

        index[col_data[i]].push_back(row_offset);
    }
}

template <typename T>
void InvertedIndexWriter<T>::saveToFile(std::string_view path) const
{
    WriteBufferFromFile write_buf(path.data());
    saveToBuffer(write_buf);
    write_buf.sync();
    uncompressed_size = write_buf.count();
}

template <typename T>
void InvertedIndexWriter<T>::saveToBuffer(WriteBuffer & write_buf) const
{
    InvertedIndex::Meta<T> meta;

    // 1. write data by block
    size_t offset = 0;

    InvertedIndex::Block<T> block;
    size_t row_ids_size = 0;
    auto write_block = [&] {
        block.serialize(write_buf);
        size_t total_size = write_buf.count();
        meta.entries.emplace_back(offset, total_size - offset, block.entries.front().value, block.entries.back().value);
        block.entries.clear();
        offset = total_size;
        row_ids_size = 0;
    };

    for (const auto & [key, row_ids] : index)
    {
        block.entries.emplace_back(key, row_ids);
        row_ids_size += row_ids.size();

        // write block
        if (InvertedIndex::getBlockSize<T>(block.entries.size(), row_ids_size) >= InvertedIndex::BlockSize)
            write_block();
    }
    if (!block.entries.empty())
        write_block();

    // 2. write meta
    offset = write_buf.count();
    meta.serialize(write_buf);

    // 3. write meta size
    UInt32 meta_size = write_buf.count() - offset;
    write_buf.write(reinterpret_cast<const char *>(&meta_size), sizeof(meta_size));

    // 4. write magic flag
    write_buf.write(InvertedIndex::MagicFlag, InvertedIndex::MagicFlagLength);

    // 5. record uncompressed size
    write_buf.next();
    if (auto * compressed = dynamic_cast<CompressedWriteBuffer<true> *>(&write_buf); compressed)
        uncompressed_size = compressed->getUncompressedBytes();
    else if (auto * compressed = dynamic_cast<CompressedWriteBuffer<false> *>(&write_buf); compressed)
        uncompressed_size = compressed->getUncompressedBytes();
    else
        uncompressed_size = write_buf.count();
}

template <typename T>
void InvertedIndexWriter<T>::saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const
{
    auto * pb_inv_idx = pb_idx->mutable_inverted_index();
    pb_inv_idx->set_uncompressed_size(uncompressed_size);
}

template class InvertedIndexWriter<UInt8>;
template class InvertedIndexWriter<UInt16>;
template class InvertedIndexWriter<UInt32>;
template class InvertedIndexWriter<UInt64>;
template class InvertedIndexWriter<Int8>;
template class InvertedIndexWriter<Int16>;
template class InvertedIndexWriter<Int32>;
template class InvertedIndexWriter<Int64>;

LocalIndexWriterPtr createInvertedIndexWriter(IndexID index_id, const TiDB::InvertedIndexDefinitionPtr & definition)
{
    if (!definition)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid index kind or definition");

    if (definition->type_size == sizeof(UInt8) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<UInt8>>(index_id);
    }
    else if (definition->type_size == sizeof(Int8) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<Int8>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt16) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<UInt16>>(index_id);
    }
    else if (definition->type_size == sizeof(Int16) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<Int16>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt32) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<UInt32>>(index_id);
    }
    else if (definition->type_size == sizeof(Int32) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<Int32>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt64) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<UInt64>>(index_id);
    }
    else if (definition->type_size == sizeof(Int64) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriter<Int64>>(index_id);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type size {}", definition->type_size);
    }
}

} // namespace DB::DM
