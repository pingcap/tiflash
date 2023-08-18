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

#include <Storages/Page/V3/LogFile/LogFormat.h>

#include <boost/container_hash/hash_fwd.hpp>
#include <set>

namespace DB::PS::V3
{
enum class LogFileStage
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
    UInt64 snap_seq; // used to memorize the max seq in checkpoint file
    const size_t bytes_on_disk;
    const String parent_path;

    static constexpr const char * LOG_FILE_PREFIX_NORMAL = "log";
    static constexpr const char * LOG_FILE_PREFIX_TEMP = ".temp.log";

    static LogFilename parseFrom(const String & parent_path, const String & filename, LoggerPtr log, size_t bytes = 0);

    inline String filename(LogFileStage file_stage) const
    {
        assert(file_stage != LogFileStage::Invalid);
        auto suffix = (snap_seq == 0) ? "" : fmt::format("_{}", snap_seq);
        return fmt::format(
            "{}_{}_{}{}",
            ((file_stage == LogFileStage::Temporary) ? LOG_FILE_PREFIX_TEMP : LOG_FILE_PREFIX_NORMAL),
            log_num,
            level_num,
            suffix);
    }

    inline String fullname(LogFileStage file_stage) const
    {
        assert(file_stage != LogFileStage::Invalid);
        assert(!parent_path.empty());
        return fmt::format("{}/{}", parent_path, filename(file_stage));
    }

    bool operator==(const LogFilename & other) const
    {
        return log_num == other.log_num && level_num == other.level_num;
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

namespace std
{
template <>
struct hash<DB::PS::V3::LogFilename>
{
    size_t operator()(const DB::PS::V3::LogFilename & name) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash_value(name.log_num));
        boost::hash_combine(seed, boost::hash_value(name.level_num));
        return seed;
    }
};
} // namespace std
