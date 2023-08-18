// Copyright 2023 PingCAP, Inc.
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

#include <Core/Block.h>
#include <Core/SpillConfig.h>
#include <Poco/File.h>

#include <mutex>

namespace DB
{
class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;
class SpillHandler;
class CachedSpillHandler;
using CachedSpillHandlerPtr = std::shared_ptr<CachedSpillHandler>;

struct SpillDetails
{
    UInt64 rows = 0;
    UInt64 data_bytes_uncompressed = 0;
    UInt64 data_bytes_compressed = 0;
    SpillDetails() = default;
    SpillDetails(UInt64 rows_, UInt64 data_bytes_uncompressed_, UInt64 data_bytes_compressed_)
        : rows(rows_)
        , data_bytes_uncompressed(data_bytes_uncompressed_)
        , data_bytes_compressed(data_bytes_compressed_)
    {}
    void merge(const SpillDetails & other)
    {
        rows += other.rows;
        data_bytes_uncompressed += other.data_bytes_uncompressed;
        data_bytes_compressed += other.data_bytes_compressed;
    }
    void subtract(const SpillDetails & other)
    {
        assert(rows >= other.rows);
        rows -= other.rows;
        assert(data_bytes_uncompressed >= other.data_bytes_uncompressed);
        data_bytes_uncompressed -= other.data_bytes_uncompressed;
        assert(data_bytes_compressed >= other.data_bytes_compressed);
        data_bytes_compressed -= other.data_bytes_compressed;
    }
};
class SpilledFile : public Poco::File
{
public:
    SpilledFile(const String & file_name, const FileProviderPtr & file_provider_);
    ~SpilledFile() override;
    UInt64 getSpilledRows() const { return details.rows; }
    const SpillDetails & getSpillDetails() const { return details; }
    void updateSpillDetails(const SpillDetails & other_details) { details.merge(other_details); }
    void markFull() { is_full = true; }
    bool isFull() const { return is_full; }

private:
    SpillDetails details;
    bool is_full = false;
    FileProviderPtr file_provider;
};

struct SpilledFiles
{
    std::mutex spilled_files_mutex;
    /// immutable spilled files mean the file can not be append
    std::vector<std::unique_ptr<SpilledFile>> immutable_spilled_files;
    /// mutable spilled files means the next spill can append to the files
    std::vector<std::unique_ptr<SpilledFile>> mutable_spilled_files;
    void makeAllSpilledFilesImmutable();
    void commitSpilledFiles(std::vector<std::unique_ptr<SpilledFile>> && spilled_files);
};

class Spiller
{
public:
    Spiller(
        const SpillConfig & config,
        bool is_input_sorted,
        UInt64 partition_num,
        const Block & input_schema,
        const LoggerPtr & logger,
        Int64 spill_version = 1,
        bool release_spilled_file_on_restore = true);
    void spillBlocks(Blocks && blocks, UInt64 partition_id);
    SpillHandler createSpillHandler(UInt64 partition_id);
    CachedSpillHandlerPtr createCachedSpillHandler(
        const BlockInputStreamPtr & from,
        UInt64 partition_id,
        const std::function<bool()> & is_cancelled);
    /// spill blocks by reading from BlockInputStream, this is more memory friendly compared to spillBlocks
    void spillBlocksUsingBlockInputStream(
        const BlockInputStreamPtr & block_in,
        UInt64 partition_id,
        const std::function<bool()> & is_cancelled);
    /// max_stream_size == 0 means the spiller choose the stream size automatically
    BlockInputStreams restoreBlocks(
        UInt64 partition_id,
        UInt64 max_stream_size = 0,
        bool append_dummy_read_stream = false);
    UInt64 spilledRows(UInt64 partition_id);
    void finishSpill();
    bool hasSpilledData() const { return has_spilled_data; };
    /// only for test now
    bool releaseSpilledFileOnRestore() const { return release_spilled_file_on_restore; }
    void removeConstantColumns(Block & block) const;

private:
    friend class SpillHandler;
    String nextSpillFileName(UInt64 partition_id);
    std::pair<std::unique_ptr<SpilledFile>, bool> getOrCreateSpilledFile(UInt64 partition_id);
    bool isSpillFinished()
    {
        std::lock_guard lock(spill_finished_mutex);
        return spill_finished;
    }
    bool isAllConstant() const { return header_without_constants.columns() == 0; }
    void recordAllConstantBlockRows(UInt64 partition_id, UInt64 rows)
    {
        assert(isAllConstant());
        RUNTIME_CHECK_MSG(isSpillFinished() == false, "{}: spill after the spiller is finished.", config.spill_id);
        std::lock_guard lock(all_constant_mutex);
        all_constant_block_rows[partition_id] += rows;
    }

private:
    SpillConfig config;
    const bool is_input_sorted;
    const UInt64 partition_num;
    /// todo remove input_schema if spiller does not rely on BlockInputStream
    const Block input_schema;
    std::vector<size_t> const_column_indexes;
    Block header_without_constants;
    const LoggerPtr logger;
    std::mutex spill_finished_mutex;
    bool spill_finished = false;
    std::atomic<bool> has_spilled_data{false};
    static std::atomic<Int64> tmp_file_index;
    std::vector<std::unique_ptr<SpilledFiles>> spilled_files;

    // Used for the case that spilled blocks containing only constant columns.
    // Record the rows of these blocks.
    std::mutex all_constant_mutex;
    std::vector<UInt64> all_constant_block_rows;

    const Int64 spill_version = 1;
    /// If release_spilled_file_on_restore is true, the spilled file will be released once all the data in the spilled
    /// file is read, otherwise, the spilled file will be released when destruct the spiller. Currently, all the spilled
    /// file can be released on restore since it is only read once, but in the future if SharedScan(shared cte) need spill,
    /// the data may be restored multiple times and release_spilled_file_on_restore need to be set to false.
    const bool release_spilled_file_on_restore;
    bool enable_append_write = false;
};

using SpillerPtr = std::unique_ptr<Spiller>;

} // namespace DB
