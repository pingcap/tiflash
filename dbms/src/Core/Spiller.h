// Copyright 2023 PingCAP, Ltd.
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

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;
class SpillHandler;

struct SpillDetails
{
    size_t rows;
    size_t data_bytes_uncompressed;
    size_t data_bytes_compressed;
    SpillDetails() = default;
    SpillDetails(size_t rows_, size_t data_bytes_uncompressed_, size_t data_bytes_compressed_)
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
};
class SpilledFile : public Poco::File
{
public:
    SpilledFile(const String & file_name, const FileProviderPtr & file_provider_);
    ~SpilledFile() override;
    size_t getSpilledRows() const { return details.rows; }
    const SpillDetails & getSpillDetails() const { return details; }
    void updateSpillDetails(const SpillDetails & other_details) { details.merge(other_details); }

private:
    SpillDetails details;
    FileProviderPtr file_provider;
};

struct SpilledFiles
{
    std::mutex spilled_files_mutex;
    std::vector<std::unique_ptr<SpilledFile>> spilled_files;
};

class Spiller
{
public:
    Spiller(const SpillConfig & config, bool is_input_sorted, size_t partition_num, const Block & input_schema, const LoggerPtr & logger, Int64 spill_version = 1, bool release_spilled_file_on_restore = true);
    void spillBlocks(const Blocks & blocks, size_t partition_id);
    /// spill blocks by reading from BlockInputStream, this is more memory friendly compared to spillBlocks
    void spillBlocksUsingBlockInputStream(IBlockInputStream & block_in, size_t partition_id, const std::function<bool()> & is_cancelled);
    /// max_stream_size == 0 means the spiller choose the stream size automatically
    BlockInputStreams restoreBlocks(size_t partition_id, size_t max_stream_size = 0);
    size_t spilledRows(size_t partition_id);
    void finishSpill() { spill_finished = true; };
    bool hasSpilledData() const { return has_spilled_data; };
    /// only for test now
    bool releaseSpilledFileOnRestore() const { return release_spilled_file_on_restore; }

private:
    friend class SpillHandler;
    String nextSpillFileName(size_t partition_id);
    SpillHandler createSpillHandler(size_t partition_id);

    const SpillConfig config;
    const bool is_input_sorted;
    const size_t partition_num;
    /// todo remove input_schema if spiller does not rely on BlockInputStream
    const Block input_schema;
    const LoggerPtr logger;
    std::atomic<bool> spill_finished{false};
    std::atomic<bool> has_spilled_data{false};
    static std::atomic<Int64> tmp_file_index;
    std::vector<std::unique_ptr<SpilledFiles>> spilled_files;
    const Int64 spill_version = 1;
    /// If release_spilled_file_on_restore is true, the spilled file will be released once all the data in the spilled
    /// file is read, otherwise, the spilled file will be released when destruct the spiller. Currently, all the spilled
    /// file can be released on restore since it is only read once, but in the future if SharedScan(shared cte) need spill,
    /// the data may be restored multiple times and release_spilled_file_on_restore need to be set to false.
    const bool release_spilled_file_on_restore;
};

} // namespace DB
