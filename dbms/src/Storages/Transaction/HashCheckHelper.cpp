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

#include <fcntl.h>

#include <Storages/Transaction/HashCheckHelper.h>
#include <Common/TiFlashException.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_SIZE_NOT_MATCH;
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int UNEXPECTED_END_OF_FILE;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int CANNOT_SEEK_THROUGH_FILE;
} // namespace ErrorCodes


namespace FileHashCheck
{
void readFileFully(const std::string & path, int fd, off_t file_offset, size_t read_size, char * data)
{

    if (-1 == ::lseek(fd, file_offset, SEEK_SET))
        throwFromErrno("Cannot seek through file " + path, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    char * pos = data;
    size_t remain = read_size;
    while (remain)
    {
        auto res = ::read(fd, pos, remain);
        if (-1 == res && errno != EINTR)
            throwFromErrno("Cannot read from file " + path, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        if (!res && errno != EINTR)
            throwFromErrno("End of file", ErrorCodes::UNEXPECTED_END_OF_FILE);

        remain -= res;
        pos += res;
    }
}

void checkObjectHashInFile(const std::string & path, const std::vector<size_t> & object_bytes, const BoolVec & use,
    const std::vector<uint128> & expected_hash_codes, size_t block_size)
{
    Poco::File file(path);
    size_t file_size = file.getSize();
    size_t total_size = 0;
    size_t max_size = 0;
    for (auto b : object_bytes)
    {
        total_size += b;
        max_size = std::max(max_size, b);
    }
    if (total_size != file_size)
            throw DB::TiFlashException("File size not match! Expected: " + DB::toString(total_size) + ", got: " + DB::toString(file_size), Errors::PageStorage::FileSizeNotMatch);

    char * object_data_buf = (char *)malloc(max_size);
    SCOPE_EXIT({ free(object_data_buf); });

    auto fd = open(path.c_str(), O_RDONLY);
    if (-1 == fd)
        throwFromErrno("Cannot open file " + path, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    SCOPE_EXIT({ ::close(fd); });

    off_t file_offset = 0;
    for (size_t index = 0; index < object_bytes.size(); ++index)
    {
        if (use[index])
        {
            uint128 hashcode{0, 0};
            size_t bytes = object_bytes[index];
            char * pos = object_data_buf;

            readFileFully(path, fd, file_offset, bytes, object_data_buf);

            while (bytes)
            {
                auto to_cal_bytes = std::min(bytes, block_size);
                hashcode = CityHash_v1_0_2::CityHash128WithSeed(pos, to_cal_bytes, hashcode);
                pos += to_cal_bytes;
                bytes -= to_cal_bytes;
            }

            if (hashcode != expected_hash_codes[index])
                throw Exception(
                    "File " + path + " hash code not match at object index: " + DB::toString(index), ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }

        file_offset += object_bytes[index];
    }
}
} // namespace FileHashCheck

} // namespace DB
