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
#include <Common/TiFlashMetrics.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/WriteBufferFromFile.h>
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
void InvertedIndexWriterInternal<T>::addBlock(
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
void InvertedIndexWriterInternal<T>::saveToBuffer(WriteBuffer & write_buf) const
{
    size_t offset = 0;

    // 0. write version
    UInt8 version = magic_enum::enum_integer(InvertedIndex::Version::V1);
    writeIntBinary(version, write_buf);
    offset += sizeof(UInt8);

    InvertedIndex::Meta<T> meta;

    // 1. write data by block
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
    write_buf.write(InvertedIndex::MagicFlag.data(), InvertedIndex::MagicFlagLength);

    // 5. record uncompressed size
    uncompressed_size = write_buf.count();

    write_buf.next();
}

template <typename T>
void InvertedIndexWriterInternal<T>::saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const
{
    auto * pb_inv_idx = pb_idx->mutable_inverted_index();
    pb_inv_idx->set_uncompressed_size(uncompressed_size);
}

template <typename T>
InvertedIndexWriterInternal<T>::~InvertedIndexWriterInternal()
{
    GET_METRIC(tiflash_inverted_index_duration, type_build).Observe(total_duration);
    GET_METRIC(tiflash_inverted_index_active_instances, type_build).Decrement();
}

template <typename T>
void InvertedIndexWriterOnDisk<T>::saveToFile() const
{
    Stopwatch w;
    SCOPE_EXIT({ writer.total_duration += w.elapsedSeconds(); });

    WriteBufferFromFile write_buf(index_file);
    writer.saveToBuffer(write_buf);
    write_buf.sync();
}

template <typename T>
void InvertedIndexWriterInMemory<T>::saveToBuffer(WriteBuffer & write_buf)
{
    Stopwatch w;
    SCOPE_EXIT({ writer.total_duration += w.elapsedSeconds(); });

    writer.saveToBuffer(write_buf);
}

template class InvertedIndexWriterInternal<UInt8>;
template class InvertedIndexWriterInternal<UInt16>;
template class InvertedIndexWriterInternal<UInt32>;
template class InvertedIndexWriterInternal<UInt64>;
template class InvertedIndexWriterInternal<Int8>;
template class InvertedIndexWriterInternal<Int16>;
template class InvertedIndexWriterInternal<Int32>;
template class InvertedIndexWriterInternal<Int64>;
template class InvertedIndexWriterOnDisk<UInt8>;
template class InvertedIndexWriterOnDisk<UInt16>;
template class InvertedIndexWriterOnDisk<UInt32>;
template class InvertedIndexWriterOnDisk<UInt64>;
template class InvertedIndexWriterOnDisk<Int8>;
template class InvertedIndexWriterOnDisk<Int16>;
template class InvertedIndexWriterOnDisk<Int32>;
template class InvertedIndexWriterOnDisk<Int64>;
template class InvertedIndexWriterInMemory<UInt8>;
template class InvertedIndexWriterInMemory<UInt16>;
template class InvertedIndexWriterInMemory<UInt32>;
template class InvertedIndexWriterInMemory<UInt64>;
template class InvertedIndexWriterInMemory<Int8>;
template class InvertedIndexWriterInMemory<Int16>;
template class InvertedIndexWriterInMemory<Int32>;
template class InvertedIndexWriterInMemory<Int64>;

LocalIndexWriterOnDiskPtr createOnDiskInvertedIndexWriter(
    IndexID index_id,
    std::string_view index_file,
    const TiDB::InvertedIndexDefinitionPtr & definition)
{
    if (!definition)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid index kind or definition");

    if (definition->type_size == sizeof(UInt8) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<UInt8>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(Int8) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<Int8>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(UInt16) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<UInt16>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(Int16) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<Int16>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(UInt32) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<UInt32>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(Int32) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<Int32>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(UInt64) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<UInt64>>(index_id, index_file);
    }
    else if (definition->type_size == sizeof(Int64) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterOnDisk<Int64>>(index_id, index_file);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type size {}", definition->type_size);
    }
}

LocalIndexWriterInMemoryPtr createInMemoryInvertedIndexWriter(
    IndexID index_id,
    const TiDB::InvertedIndexDefinitionPtr & definition)
{
    if (!definition)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid index kind or definition");

    if (definition->type_size == sizeof(UInt8) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<UInt8>>(index_id);
    }
    else if (definition->type_size == sizeof(Int8) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<Int8>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt16) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<UInt16>>(index_id);
    }
    else if (definition->type_size == sizeof(Int16) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<Int16>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt32) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<UInt32>>(index_id);
    }
    else if (definition->type_size == sizeof(Int32) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<Int32>>(index_id);
    }
    else if (definition->type_size == sizeof(UInt64) && !definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<UInt64>>(index_id);
    }
    else if (definition->type_size == sizeof(Int64) && definition->is_signed)
    {
        return std::make_shared<InvertedIndexWriterInMemory<Int64>>(index_id);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type size {}", definition->type_size);
    }
}

} // namespace DB::DM
