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

#include <Common/Logger.h>
#include <Common/StringUtils/StringUtils.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <stdexcept>

namespace DB::PS::V3
{
LogFilename LogFilename::parseFrom(const String & parent_path, const String & filename, LoggerPtr log, size_t bytes)
{
    if (!startsWith(filename, LOG_FILE_PREFIX_TEMP) && !startsWith(filename, LOG_FILE_PREFIX_NORMAL))
    {
        LOG_INFO(log, "Ignore not log file [dir={}] [file={}]", parent_path, filename);
        return {LogFileStage::Invalid, 0, 0, 0, 0, ""};
    }
    std::vector<String> ss;
    boost::split(ss, filename, boost::is_any_of("_"));
    if (ss.size() != 3 && ss.size() != 4)
    {
        LOG_INFO(log, "Ignore unrecognized log file [dir={}] [file={}]", parent_path, filename);
        return {LogFileStage::Invalid, 0, 0, 0, 0, ""};
    }

    String err_msg;
    try
    {
        Format::LogNumberType log_num = std::stoull(ss[1]);
        Format::LogNumberType level_num = std::stoull(ss[2]);
        UInt64 snap_seq = (ss.size() == 4) ? std::stoull(ss[3]) : 0;
        if (ss[0] == LOG_FILE_PREFIX_TEMP)
        {
            return {LogFileStage::Temporary, log_num, level_num, snap_seq, bytes, parent_path};
        }
        else if (ss[0] == LOG_FILE_PREFIX_NORMAL)
        {
            return {LogFileStage::Normal, log_num, level_num, snap_seq, bytes, parent_path};
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
    LOG_INFO(log, "Ignore unrecognized log file [dir={}] [file={}] [err={}]", parent_path, filename, err_msg);
    return {LogFileStage::Invalid, 0, 0, 0, 0, ""};
}

} // namespace DB::PS::V3
