#include <Common/StringUtils/StringUtils.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <stdexcept>

namespace DB::PS::V3
{
LogFilename LogFilename::parseFrom(const String & parent_path, const String & filename, Poco::Logger * log)
{
    if (!startsWith(filename, LOG_FILE_PREFIX_TEMP) && !startsWith(filename, LOG_FILE_PREFIX_NORMAL))
    {
        LOG_FMT_INFO(log, "Ignore not log file [dir={}] [file={}]", parent_path, filename);
        return {LogFileStage::Invalid, 0, 0, ""};
    }
    Strings ss;
    boost::split(ss, filename, boost::is_any_of("_"));
    if (ss.size() != 3)
    {
        LOG_FMT_INFO(log, "Ignore unrecognized log file [dir={}] [file={}]", parent_path, filename);
        return {LogFileStage::Invalid, 0, 0, ""};
    }

    String err_msg;
    try
    {
        Format::LogNumberType log_num = std::stoull(ss[1]);
        Format::LogNumberType level_num = std::stoull(ss[2]);
        if (ss[0] == LOG_FILE_PREFIX_TEMP)
        {
            return {LogFileStage::Temporary, log_num, level_num, parent_path};
        }
        else if (ss[0] == LOG_FILE_PREFIX_NORMAL)
        {
            return {LogFileStage::Normal, log_num, level_num, parent_path};
        }
    }
    catch (std::invalid_argument & e)
    {
        err_msg = e.what();
    }
    catch (std::out_of_range & e)
    {
        err_msg = e.what();
    }
    LOG_FMT_INFO(log, "Ignore unrecognized log file [dir={}] [file={}] [err={}]", parent_path, filename, err_msg);
    return {LogFileStage::Invalid, 0, 0, ""};
}

} // namespace DB::PS::V3
