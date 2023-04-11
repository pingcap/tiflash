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

#include <Poco/File.h>

#include <set>
#include <string>

namespace DB
{
struct FileWithTimestamp
{
    std::string path;
    time_t modification_time;

    FileWithTimestamp(const std::string & path_, time_t modification_time_)
        : path(path_)
        , modification_time(modification_time_)
    {}

    bool operator<(const FileWithTimestamp & rhs) const { return path < rhs.path; }

    static bool isTheSame(const FileWithTimestamp & lhs, const FileWithTimestamp & rhs)
    {
        return (lhs.modification_time == rhs.modification_time) && (lhs.path == rhs.path);
    }
};

struct FilesChangesTracker
{
    std::set<FileWithTimestamp> files;

    bool valid() const
    {
        return !files.empty();
    }

    void addIfExists(const std::string & path)
    {
        if (!path.empty() && Poco::File(path).exists())
        {
            files.emplace(path, Poco::File(path).getLastModified().epochTime());
        }
    }
    bool isDifferOrNewerThan(const FilesChangesTracker & rhs) const
    {
        return (files.size() != rhs.files.size()) || !std::equal(files.begin(), files.end(), rhs.files.begin(), FileWithTimestamp::isTheSame);
    }
};
} // namespace DB
