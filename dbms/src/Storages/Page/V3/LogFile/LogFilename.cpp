#include <Common/StringUtils/StringUtils.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB::PS::V3
{
LogFilename LogFilename::parseFrom(const String parent_path, const String & filename, Poco::Logger * log)
{
    if (!startsWith(filename, LOG_FILE_PREFIX_TEMP) && !startsWith(filename, LOG_FILE_PREFIX_NORMAL))
    {
        LOG_FMT_INFO(log, "Not log file, ignored {}", filename);
        return {Format::LogFileStage::Invalid, 0, 0, ""};
    }
    Strings ss;
    boost::split(ss, filename, boost::is_any_of("_"));
    if (ss.size() != 3)
    {
        LOG_FMT_INFO(log, "Unrecognized log file, ignored {}", filename);
        return {Format::LogFileStage::Invalid, 0, 0, ""};
    }
    Format::LogNumberType log_num = std::stoull(ss[1]);
    Format::LogNumberType level_num = std::stoull(ss[2]);
    if (ss[0] == LOG_FILE_PREFIX_TEMP)
    {
        return {Format::LogFileStage::Temporary, log_num, level_num, parent_path};
    }
    else if (ss[0] == LOG_FILE_PREFIX_NORMAL)
    {
        return {Format::LogFileStage::Normal, log_num, level_num, parent_path};
    }
    LOG_FMT_INFO(log, "Unrecognized log file prefix, ignored {}", filename);
    return {Format::LogFileStage::Invalid, 0, 0, ""};
}

} // namespace DB::PS::V3
