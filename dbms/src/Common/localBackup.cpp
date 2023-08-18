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

#include <Common/Exception.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <port/unistd.h>
#include <sys/stat.h>

#include <iostream>
#include <string>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_DEEP_RECURSION;
extern const int DIRECTORY_ALREADY_EXISTS;
} // namespace ErrorCodes
} // namespace DB


static void localBackupImpl(const Poco::Path & source_path, const Poco::Path & destination_path, size_t level)
{
    if (level >= 1000)
        throw DB::Exception("Too deep recursion", DB::ErrorCodes::TOO_DEEP_RECURSION);

    Poco::File(destination_path).createDirectories();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(source_path); dir_it != dir_end; ++dir_it)
    {
        Poco::Path source = dir_it.path();
        Poco::Path destination = destination_path;
        destination.append(dir_it.name());

        if (!dir_it->isDirectory())
        {
            dir_it->setReadOnly();

            std::string source_str = source.toString();
            std::string destination_str = destination.toString();

            /** We are trying to create a hard link.
              * If it already exists, we check that source and destination point to the same inode.
              */
            if (0 != link(source_str.c_str(), destination_str.c_str()))
            {
                if (errno == EEXIST)
                {
                    auto link_errno = errno;

                    struct stat source_descr;
                    struct stat destination_descr;

                    if (0 != lstat(source_str.c_str(), &source_descr))
                        DB::throwFromErrno("Cannot stat " + source_str);

                    if (0 != lstat(destination_str.c_str(), &destination_descr))
                        DB::throwFromErrno("Cannot stat " + destination_str);

                    if (source_descr.st_ino != destination_descr.st_ino)
                        DB::throwFromErrno(
                            "Destination file " + destination_str + " is already exist and have different inode.",
                            0,
                            link_errno);
                }
                else
                    DB::throwFromErrno("Cannot link " + source_str + " to " + destination_str);
            }
        }
        else
        {
            localBackupImpl(source, destination, level + 1);
        }
    }
}

void localBackup(const Poco::Path & source_path, const Poco::Path & destination_path)
{
    if (Poco::File(destination_path).exists() && Poco::DirectoryIterator(destination_path) != Poco::DirectoryIterator())
    {
        throw DB::Exception(
            "Directory " + destination_path.toString() + " already exists and is not empty.",
            DB::ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    }

    size_t try_no = 0;
    const size_t max_tries = 10;

    /** Files in the directory can be permanently added and deleted.
      * If some file is deleted during an attempt to make a backup, then try again,
      *  because it's important to take into account any new files that might appear.
      */
    while (true)
    {
        try
        {
            localBackupImpl(source_path, destination_path, 0);
        }
        catch (const DB::ErrnoException & e)
        {
            if (e.getErrno() != ENOENT)
                throw;

            ++try_no;
            if (try_no == max_tries)
                throw;

            continue;
        }
        catch (const Poco::FileNotFoundException & e)
        {
            ++try_no;
            if (try_no == max_tries)
                throw;

            continue;
        }

        break;
    }
}
