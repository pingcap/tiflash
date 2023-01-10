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

class SpilledFile : public Poco::File
{
public:
    SpilledFile(const String & file_name, const FileProviderPtr & file_provider_);
    ~SpilledFile() override;
    void addSpilledDataSize(size_t added_size) { spilled_data_size += added_size; }
    size_t getSpilledDataSize() const { return spilled_data_size; }

private:
    size_t spilled_data_size = 0;
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
    Spiller(const SpillConfig & config, bool is_input_sorted, size_t partition_num, const Block & input_schema, const LoggerPtr & logger);
    void spillBlocks(const Blocks & blocks, size_t partition_id);
    /// max_stream_size == 0 means the spiller choose the stream size automatically
    BlockInputStreams restoreBlocks(size_t partition_id, size_t max_stream_size = 0);
    size_t spilledBlockDataSize(size_t partition_id);
    void finishSpill() { spill_finished = true; };
    bool hasSpilledData() { return has_spilled_data; };

private:
    String nextSpillFileName(size_t partition_id);

    const SpillConfig config;
    bool is_input_sorted;
    size_t partition_num;
    /// todo remove input_schema if spiller does not rely on BlockInputStream
    Block input_schema;
    LoggerPtr logger;
    std::atomic<bool> spill_finished{false};
    std::atomic<bool> has_spilled_data{false};
    static std::atomic<Int64> tmp_file_index;
    std::vector<std::unique_ptr<SpilledFiles>> spilled_files;
};

} // namespace DB
