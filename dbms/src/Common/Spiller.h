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
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>

namespace DB
{
class SpilledFile : public Poco::File
{
public:
    explicit SpilledFile(const String & file_name);
    ~SpilledFile() override;
    void addSpilledDataSize(size_t added_size) { spilled_data_size += added_size; }
    size_t getSpilledDataSize() const { return spilled_data_size; }

private:
    size_t spilled_data_size = 0;
};

struct SpilledFileStream
{
    ReadBufferFromFile file_in;
    CompressedReadBuffer<> compressed_in;
    BlockInputStreamPtr block_in;

    explicit SpilledFileStream(const std::string & path, const Block & header)
        : file_in(path)
        , compressed_in(file_in)
        , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, header, 0))
    {}
};

class SpilledFilesInputStream : public IProfilingBlockInputStream
{
public:
    SpilledFilesInputStream(const std::vector<String> & spilled_files, const Block & header_);
    Block getHeader() const override;
    String getName() const override;

protected:
    Block readImpl() override;

private:
    std::vector<String> spilled_files;
    size_t current_reading_file_index;
    Block header;
    std::unique_ptr<SpilledFileStream> current_file_stream;
};

class Spiller
{
public:
    Spiller(const String & id, bool is_input_sorted, size_t partition_num, const String & spill_dir, const Block & input_schema, LoggerPtr logger);
    bool spillBlocks(const Blocks & blocks, size_t partition_id);
    BlockInputStreams restoreBlocks(size_t partition_id, size_t max_stream_size);
    size_t spilledBlockDataSize(size_t partition_id);
    void finishSpill() { spill_finished = true; };

private:
    String nextSpillFileName(size_t partition_id);

    String id;
    bool is_input_sorted;
    size_t partition_num;
    String spill_dir;
    /// todo remove input_schema if spiller does not rely on BlockInputStream
    Block input_schema;
    LoggerPtr logger;
    bool spill_finished = false;
    static std::atomic<Int64> tmp_file_index;
    std::mutex spilled_files_mutex;
    std::vector<std::vector<std::unique_ptr<SpilledFile>>> spilled_files;
};

} // namespace DB
