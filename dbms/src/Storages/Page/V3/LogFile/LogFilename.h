#pragma once

#include <Storages/Page/V3/LogFile/LogFormat.h>

#include <set>

namespace Poco
{
class Logger;
}

namespace DB::PS::V3
{
struct LogFileName
{
    const Format::LogFileStage stage;
    const Format::LogNumberType log_num;
    const Format::LogNumberType level_num;
    const String parent_path;

    static LogFileName parseFrom(const String parent_path, const String & filename, Poco::Logger * log);
};

struct LogFileNameCmp
{
    bool operator()(const LogFileName & lhs, const LogFileName & rhs) const
    {
        if (lhs.log_num == rhs.log_num)
            return lhs.level_num < rhs.level_num;
        return lhs.log_num < rhs.log_num;
    }
};
using LogFilenameSet = std::set<LogFileName, LogFileNameCmp>;

} // namespace DB::PS::V3
