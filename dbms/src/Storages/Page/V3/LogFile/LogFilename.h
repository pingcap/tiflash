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

#include <Storages/Page/V3/LogFile/LogFormat.h>

#include <set>

namespace DB::PS::V3
{
enum LogFileStage
{
    Invalid,
    Temporary,
    Normal,
};

struct LogFilename
{
    const LogFileStage stage;
    const Format::LogNumberType log_num;
    const Format::LogNumberType level_num;
    const String parent_path;

    static constexpr const char * LOG_FILE_PREFIX_NORMAL = "log";
    static constexpr const char * LOG_FILE_PREFIX_TEMP = ".temp.log";

    static LogFilename parseFrom(const String & parent_path, const String & filename, LoggerPtr log);

    inline String filename(LogFileStage file_stage) const
    {
        assert(file_stage != LogFileStage::Invalid);
        return fmt::format(
            "{}_{}_{}",
            ((file_stage == LogFileStage::Temporary) ? LOG_FILE_PREFIX_TEMP : LOG_FILE_PREFIX_NORMAL),
            log_num,
            level_num);
    }

    inline String fullname(LogFileStage file_stage) const
    {
        assert(file_stage != LogFileStage::Invalid);
        assert(!parent_path.empty());
        return fmt::format("{}/{}", parent_path, filename(file_stage));
    }
};

struct LogFilenameCmp
{
    bool operator()(const LogFilename & lhs, const LogFilename & rhs) const
    {
        if (lhs.log_num == rhs.log_num)
            return lhs.level_num < rhs.level_num;
        return lhs.log_num < rhs.log_num;
    }
};
using LogFilenameSet = std::set<LogFilename, LogFilenameCmp>;

} // namespace DB::PS::V3
