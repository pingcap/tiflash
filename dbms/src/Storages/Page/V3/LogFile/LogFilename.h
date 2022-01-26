#pragma once

#include <Storages/Page/V3/LogFile/LogFormat.h>

#include <set>

namespace Poco
{
class Logger;
}

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

    static LogFilename parseFrom(const String & parent_path, const String & filename, Poco::Logger * log);

    inline String filename(LogFileStage stage) const
    {
        assert(stage != LogFileStage::Invalid);
        return fmt::format(
            "{}_{}_{}",
            ((stage == LogFileStage::Temporary) ? LOG_FILE_PREFIX_TEMP : LOG_FILE_PREFIX_NORMAL),
            log_num,
            level_num);
    }

    inline String fullname(LogFileStage stage) const
    {
        assert(stage != LogFileStage::Invalid);
        assert(!parent_path.empty());
        return fmt::format("{}/{}", parent_path, filename(stage));
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
