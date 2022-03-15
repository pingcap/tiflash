// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromOStream.h>
#include <Poco/File.h>
#include <fcntl.h>
#include <sys/stat.h>


namespace DB
{
struct CompactCtxBase
{
    static constexpr char CompactFileName[8] = "compact";
    static constexpr size_t MagicNumber = 19910905;
    static constexpr short Version = 1;
};

struct CompactWriteCtx : public CompactCtxBase
{
    struct FilePosition
    {
        size_t begin;
        size_t end;
    };
    std::unordered_map<std::string, FilePosition> mark_map;

    std::string file_name;
    int fd;

    std::ostringstream mark_stream;

    WriteBufferFromFile plain_file;
    std::shared_ptr<HashingWriteBuffer> plain_hashing;

    CompactWriteCtx(std::string part_path, size_t buffer_size);

    void beginMark(std::string name);

    void endMark(std::string name);

    // write EOF flag for hashing buffer.
    void writeEOF(std::shared_ptr<HashingWriteBuffer>);

    void finalize(size_t);

    void mergeMarksStream(std::string, size_t);

private:
    size_t flushAllMarks();

    void writeFooter(size_t, size_t);
};

struct CompactReadCtx : public CompactCtxBase
{
    std::string compact_path;

    size_t rows_count;

    struct FilePosition
    {
        size_t begin;
        size_t end;
    };
    std::unordered_map<std::string, FilePosition> mark_map;

    CompactReadCtx(std::string compact_path_);

    bool hasColumn(std::string col);

    FilePosition getMarkRange(std::string name);

    size_t getMarksCount();

private:
    void loadFooter(ReadBufferFromFile & file);
};

using CompactWriteContextPtr = std::unique_ptr<CompactWriteCtx>;
using CompactReadContextPtr = std::shared_ptr<CompactReadCtx>;

struct CompactContextFactory
{
public:
    static CompactWriteContextPtr tryToGetCompactWriteCtxPtr(std::string part_path, size_t buffer_size)
    {
        return std::make_unique<CompactWriteCtx>(part_path + CompactCtxBase::CompactFileName, buffer_size);
    }
    static CompactReadContextPtr tryToGetCompactReadCtxPtr(std::string part_path)
    {
        std::string compact_path = part_path + CompactCtxBase::CompactFileName;
        if (!Poco::File(compact_path).exists())
        {
            return nullptr;
        }
        return std::make_shared<CompactReadCtx>(compact_path);
    }
};

} // namespace DB
