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
#include <Poco/ArchiveStrategy.h>
namespace Poco
{
template <class DT>
class TiFlashArchiveByTimestampsStrategy : public ArchiveByTimestampStrategy<DT>
{
public:
    inline static const std::string suffix_fmt = "%Y-%m-%d-%H:%M:%S.%i";
    LogFile * archive(LogFile * p_file) override
    {
        std::string path = p_file->path();
        delete p_file;
        std::string arch_path = path;
        arch_path.append(".");
        DateTimeFormatter::append(arch_path, DT().timestamp(), suffix_fmt);

        if (this->exists(arch_path))
            this->archiveByNumber(arch_path);
        else
            this->moveFile(path, arch_path);

        return new LogFile(path);
    }
};
} // namespace Poco
