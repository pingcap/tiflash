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

#pragma once

#include <Poco/File.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

#include <map>
#include <string>


namespace DB
{
/// stores the sizes of all columns, and can check whether the columns are corrupted
class FileChecker
{
private:
    /// File name -> size.
    using Map = std::map<std::string, size_t>;

public:
    using Files = std::vector<Poco::File>;

    explicit FileChecker(const std::string & file_info_path_);
    void setPath(const std::string & file_info_path_);
    void update(const Poco::File & file);
    void update(const Files::const_iterator & begin, const Files::const_iterator & end);

    /// Check the files whose parameters are specified in sizes.json
    bool check() const;

private:
    void initialize();
    void updateImpl(const Poco::File & file);
    void save() const;
    void load(Map & map) const;

    std::string files_info_path;
    std::string tmp_files_info_path;

    /// The data from the file is read lazily.
    Map map;
    bool initialized = false;

    Poco::Logger * log = &Poco::Logger::get("FileChecker");
};

} // namespace DB
