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

#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromTemporaryFile.h>
#include <Poco/Path.h>
#include <fcntl.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_SEEK_THROUGH_FILE;
} // namespace ErrorCodes


WriteBufferFromTemporaryFile::WriteBufferFromTemporaryFile(std::unique_ptr<Poco::TemporaryFile> && tmp_file_)
    : WriteBufferFromFile(tmp_file_->path(), DBMS_DEFAULT_BUFFER_SIZE, O_RDWR | O_TRUNC | O_CREAT, 0600)
    , tmp_file(std::move(tmp_file_))
{}


WriteBufferFromTemporaryFile::Ptr WriteBufferFromTemporaryFile::create(const std::string & tmp_dir)
{
    Poco::File(tmp_dir).createDirectories();

    /// NOTE: std::make_shared cannot use protected constructors
    return Ptr{new WriteBufferFromTemporaryFile(std::make_unique<Poco::TemporaryFile>(tmp_dir))};
}


class ReadBufferFromTemporaryWriteBuffer : public ReadBufferFromFile
{
public:
    static ReadBufferPtr createFrom(WriteBufferFromTemporaryFile * origin)
    {
        int fd = origin->getFD();
        std::string file_name = origin->getFileName();

        off_t res = lseek(fd, 0, SEEK_SET);
        if (-1 == res)
            throwFromErrno("Cannot reread temporary file " + file_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        return std::make_shared<ReadBufferFromTemporaryWriteBuffer>(fd, file_name, std::move(origin->tmp_file));
    }

    ReadBufferFromTemporaryWriteBuffer(
        int fd,
        const std::string & file_name,
        std::unique_ptr<Poco::TemporaryFile> && tmp_file_)
        : ReadBufferFromFile(fd, file_name)
        , tmp_file(std::move(tmp_file_))
    {}

    std::unique_ptr<Poco::TemporaryFile> tmp_file;
};


ReadBufferPtr WriteBufferFromTemporaryFile::getReadBufferImpl()
{
    /// ignore buffer, write all data to file and reread it
    next();

    auto res = ReadBufferFromTemporaryWriteBuffer::createFrom(this);

    /// invalidate FD to avoid close(fd) in destructor
    setFD(-1);
    file_name = {};

    return res;
}


WriteBufferFromTemporaryFile::~WriteBufferFromTemporaryFile() = default;


} // namespace DB
