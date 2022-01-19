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

    static constexpr const char * LOG_FILE_PREFIX_NORMAL = "log";
    static constexpr const char * LOG_FILE_PREFIX_TEMP = ".temp.log";

    static LogFileName parseFrom(const String parent_path, const String & filename, Poco::Logger * log);

    inline String filename(Format::LogFileStage s) const
    {
        assert(s != Format::LogFileStage::Invalid);
        return fmt::format(
            "{}_{}_{}",
            ((s == Format::LogFileStage::Temporary) ? LOG_FILE_PREFIX_TEMP : LOG_FILE_PREFIX_NORMAL),
            log_num,
            level_num);
    }

    inline String fullname(Format::LogFileStage s) const
    {
        assert(s != Format::LogFileStage::Invalid);
        return fmt::format("{}/{}", parent_path, filename(s));
    }
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
