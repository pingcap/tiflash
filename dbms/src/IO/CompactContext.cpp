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

#include <Common/ProfileEvents.h>
#include <IO/CompactContext.h>
#include <IO/CompressedStream.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteHelpers.h>
#include <city.h>
#include <fcntl.h>
#include <sys/stat.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int UNKNOWN_FORMAT;
extern const int FORMAT_VERSION_TOO_OLD;
} // namespace ErrorCodes

CompactWriteCtx::CompactWriteCtx(std::string compact_path_, size_t buffer_size)
    : file_name(compact_path_)
    , fd(::open(file_name.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0666))
    , plain_file(WriteBufferFromFile(fd, file_name, buffer_size))
    , plain_hashing(std::make_shared<HashingWriteBuffer>(plain_file))
{
    if (fd == -1)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        throwFromErrno("Cannot open file " + file_name + " errno is :" + std::to_string(errno), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }
}

size_t CompactWriteCtx::flushAllMarks()
{
    plain_hashing->next();
    size_t offset_of_all_marks = plain_hashing->count();
    plain_hashing->write(mark_stream.str().c_str(), mark_stream.str().size());
    plain_hashing->next();
    return offset_of_all_marks;
}

void CompactWriteCtx::writeFooter(size_t rows_count, size_t offset_of_all_marks)
{
    size_t footerOffset = plain_hashing->count();
    writeIntBinary(mark_map.size(), *plain_hashing);
    for (auto it = mark_map.begin(); it != mark_map.end(); it++)
    {
        writeBinary(it->first, *plain_hashing);
        writeIntBinary(it->second.begin + offset_of_all_marks, *plain_hashing);
        writeIntBinary(it->second.end + offset_of_all_marks, *plain_hashing);
    }
    writeIntBinary(rows_count, *plain_hashing);
    writeIntBinary(footerOffset, *plain_hashing);
    writeIntBinary(MagicNumber << 2 | Version, *plain_hashing);
    plain_hashing->next();
}

void CompactWriteCtx::finalize(size_t rows_count)
{
    size_t offset_of_all_marks = flushAllMarks();
    writeFooter(rows_count, offset_of_all_marks);
}

void CompactWriteCtx::beginMark(std::string name)
{
    mark_map[name].begin = mark_stream.tellp();
}

void CompactWriteCtx::endMark(std::string name)
{
    mark_map[name].end = mark_stream.tellp();
}

void CompactWriteCtx::mergeMarksStream(std::string mark_str, size_t file_offset)
{
    std::istringstream read_mark_stream(mark_str);
    ReadBufferFromIStream read_buffer(read_mark_stream);
    WriteBufferFromOStream write_buffer(mark_stream);
    while (!read_buffer.eof())
    {
        size_t offset_in_file;
        size_t offset_in_block;
        readIntBinary(offset_in_file, read_buffer);
        readIntBinary(offset_in_block, read_buffer);
        offset_in_file += file_offset;
        writeIntBinary(offset_in_file, write_buffer);
        writeIntBinary(offset_in_block, write_buffer);
    }
}

void CompactWriteCtx::writeEOF(std::shared_ptr<HashingWriteBuffer> hashing)
{
    CityHash_v1_0_2::uint128 checksum;
    hashing->write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
    writeIntBinary(CompressionMethodByte::COL_END, *hashing);
}

CompactReadCtx::CompactReadCtx(std::string compact_path_)
{
    compact_path = compact_path_;
    auto plain_file = ReadBufferFromFile(compact_path);
    loadFooter(plain_file);
}

bool CompactReadCtx::hasColumn(std::string col)
{
    return mark_map.find(col) != mark_map.end();
}

size_t CompactReadCtx::getMarksCount()
{
    auto it = mark_map.begin();
    if (it == mark_map.end())
    {
        return 0;
    }
    return it->second.end - it->second.begin;
}

void CompactReadCtx::loadFooter(ReadBufferFromFile & file)
{
    int fd = file.getFD();
    off_t off = ::lseek(fd, -sizeof(size_t) * 3, SEEK_END);
    if (off == -1)
    {
        throw Exception("seek file error");
    }
    readIntBinary(rows_count, file);
    size_t offset;
    size_t magic_and_version;
    readIntBinary(offset, file);
    readIntBinary(magic_and_version, file);
    if ((magic_and_version >> 2) != MagicNumber)
    {
        throw Exception("Bad maigic number: " + DB::toString(magic_and_version >> 2), ErrorCodes::UNKNOWN_FORMAT);
    }
    if ((magic_and_version & (3)) != Version)
    {
        throw Exception("compact file's version is too old", ErrorCodes::FORMAT_VERSION_TOO_OLD);
    }
    file.seek(offset);
    size_t len;
    readIntBinary(len, file);
    for (size_t i = 0; i < len; i++)
    {
        std::string col_name;
        size_t begin;
        size_t end;
        readStringBinary(col_name, file);
        readIntBinary(begin, file);
        readIntBinary(end, file);

        if (end < begin)
        {
            throw Exception("get wrong marks range");
        }

        mark_map[col_name].begin = begin;
        mark_map[col_name].end = end;
    }
}

CompactReadCtx::FilePosition CompactReadCtx::getMarkRange(std::string name)
{
    auto it = mark_map.find(name);
    if (it == mark_map.end())
    {
        throw Exception("can't find mark: " + name);
    }
    return it->second;
}

} // namespace DB
