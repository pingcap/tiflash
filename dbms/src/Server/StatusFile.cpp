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

#include "StatusFile.h"

#include <Common/TiFlashBuildInfo.h>
#include <IO/Buffer/LimitReadBuffer.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Poco/File.h>
#include <common/LocalDateTime.h>
#include <common/logger_useful.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>


namespace DB
{
StatusFile::StatusFile(const std::string & path_)
    : path(path_)
{
    /// If file already exists. NOTE Minor race condition.
    if (Poco::File(path).exists())
    {
        std::string contents;
        {
            ReadBufferFromFile in(path, 1024);
            LimitReadBuffer limit_in(in, 1024);
            readStringUntilEOF(contents, limit_in);
        }

        if (!contents.empty())
            LOG_INFO(
                &Poco::Logger::get("StatusFile"),
                "Status file {} already exists - unclean restart. Contents:\n{}",
                path,
                contents);
        else
            LOG_INFO(
                &Poco::Logger::get("StatusFile"),
                "Status file {} already exists and is empty - probably unclean hardware restart.",
                path);
    }

    fd = open(path.c_str(), O_WRONLY | O_CREAT, 0666);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + path);

    try
    {
        int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
        if (-1 == flock_ret)
        {
            if (errno == EWOULDBLOCK)
                throw Exception(
                    "Cannot lock file " + path + ". Another server instance in same directory is already running.");
            else
                throwFromErrno("Cannot lock file " + path);
        }

        if (0 != ftruncate(fd, 0))
            throwFromErrno("Cannot ftruncate " + path);

        if (0 != lseek(fd, 0, SEEK_SET))
            throwFromErrno("Cannot lseek " + path);

        /// Write information about current server instance to the file.
        {
            WriteBufferFromFileDescriptor out(fd, 1024);
            out << "PID: " << getpid() << "\n"
                << "Started at: " << LocalDateTime(time(nullptr)) << "\n"
                << "Version: " << TiFlashBuildInfo::getReleaseVersion() << "\n";
        }
    }
    catch (...)
    {
        close(fd);
        throw;
    }
}


StatusFile::~StatusFile()
{
    char buf[128];

    if (0 != close(fd))
        LOG_ERROR(
            &Poco::Logger::get("StatusFile"),
            "Cannot close file {}, errno: {}, strerror: {}",
            path,
            errno,
            strerror_r(errno, buf, sizeof(buf)));

    if (0 != unlink(path.c_str()))
        LOG_ERROR(
            &Poco::Logger::get("StatusFile"),
            "Cannot unlink file {}, errno: {}, strerror: {}",
            path,
            errno,
            strerror_r(errno, buf, sizeof(buf)));
}

} // namespace DB
