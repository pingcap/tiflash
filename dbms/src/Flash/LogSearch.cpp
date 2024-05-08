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

#include <Common/StringUtils/StringUtils.h>
#include <Flash/LogSearch.h>
#include <Poco/InflatingStream.h>
#include <boost_wrapper/string.h>

#include <boost/algorithm/string/predicate.hpp>
#include <magic_enum.hpp>

namespace DB
{
using ::diagnosticspb::LogLevel;
using ::diagnosticspb::LogMessage;

static time_t fast_mktime(struct tm * tm)
{
    thread_local struct tm cache = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
    thread_local time_t time_cache = 0;
    time_t result;
    time_t hmsarg;

    hmsarg = 3600 * tm->tm_hour + 60 * tm->tm_min + tm->tm_sec;

    if (cache.tm_mday == tm->tm_mday && cache.tm_mon == tm->tm_mon && cache.tm_year == tm->tm_year)
    {
        result = time_cache + hmsarg;
        tm->tm_isdst = cache.tm_isdst;
    }
    else
    {
        cache.tm_mday = tm->tm_mday;
        cache.tm_mon = tm->tm_mon;
        cache.tm_year = tm->tm_year;
        time_cache = mktime(&cache);
        tm->tm_isdst = cache.tm_isdst;
        result = (-1 == time_cache) ? -1 : time_cache + hmsarg;
    }

    return result;
}

std::optional<LogMessage> LogIterator::next()
{
    for (;;)
    {
        LogEntry entry;
        std::optional<Error> maybe_err = readLog(entry);
        if (maybe_err.has_value())
        {
            Error err = maybe_err.value();
            if (!err_info.has_value())
            {
                switch (err.tp)
                {
                case Error::Type::UNEXPECTED_LOG_HEAD:
                    err_info = std::make_pair(cur_lineno, err.tp);
                    break;
                default:
                    break;
                }
            }
            if (err.tp != Error::Type::EOI && err.tp != Error::Type::UNKNOWN)
            {
                continue;
            }
            else
            {
                return {};
            }
        }

        LogLevel level;
        switch (entry.level)
        {
        case LogEntry::Level::Trace:
            level = LogLevel::Trace;
            break;
        case LogEntry::Level::Debug:
            level = LogLevel::Debug;
            break;
        case LogEntry::Level::Info:
            level = LogLevel::Info;
            break;
        case LogEntry::Level::Warn:
            level = LogLevel::Warn;
            break;
        case LogEntry::Level::Error:
            level = LogLevel::Error;
            break;
        default:
            level = LogLevel::UNKNOWN;
            break;
        }

        if (match(entry.time, level, entry.message.data(), entry.message.size()))
        {
            LogMessage msg;
            msg.set_time(entry.time);
            msg.set_level(level);
            msg.set_message(std::string(entry.message));
            return msg;
        }
    }
}

bool LogIterator::match(const int64_t time, const LogLevel level, const char * c, size_t sz) const
{
    // Check time range
    if (time >= end_time || time < start_time)
        return false;

    // Check level
    if (std::find(levels.begin(), levels.end(), level) == levels.end() && !levels.empty())
        return false;

    // Grep
    re2::StringPiece content{c, sz};
    for (auto && regex : compiled_patterns)
    {
        if (!RE2::PartialMatch(content, *regex))
            return false;
    }

    return true;
}


static inline bool read_int(const char * s, size_t n, int & result)
{
    result = 0;
    for (size_t i = 0; i < n; ++i)
    {
        result *= 10;
        int x = s[i] - '0';
        if (x < 0 || x > 9)
            return false;
        result += x;
    }
    return true;
}

static inline bool read_sint(const char * s, size_t n, int & result)
{
    result = 0;
    int start = 0;
    int sign = 1;
    if (s[start] == '+')
    {
        start++;
    }
    else if (s[start] == '-')
    {
        start++;
        sign = -1;
    }
    read_int(s + start, n - start, result);
    result *= sign;
    return true;
}

bool LogIterator::readLevel(size_t limit, const char * s, size_t & level_start, size_t & level_size)
{
    level_start = 33;
    level_size = 0;
    if (level_start >= limit)
        return false;
    if (s[level_start] != '[')
        return false;
    level_start++;
    while (true)
    {
        if (level_size > 7)
        {
            // max length is 5
            return false;
        }
        if (level_start + level_size >= limit)
            return false;
        if (s[level_start + level_size] == ']')
            break;
        if (s[level_start + level_size] == '\0')
            return false;
        level_size++;
    }
    return true;
}

bool LogIterator::readDate(
    size_t limit,
    const char * s,
    int & y,
    int & m,
    int & d,
    int & H,
    int & M,
    int & S,
    int & MS,
    int & TZH,
    int & TZM)
{
    if (31 >= limit)
        return false;
    if (!read_int(s + 1, 4, y))
        return false;
    if (!read_int(s + 6, 2, m))
        return false;
    if (!read_int(s + 9, 2, d))
        return false;
    if (!read_int(s + 12, 2, H))
        return false;
    if (!read_int(s + 15, 2, M))
        return false;
    if (!read_int(s + 18, 2, S))
        return false;
    if (!read_int(s + 21, 3, MS))
        return false;
    if (!read_sint(s + 25, 3, TZH))
        return false;
    if (!read_int(s + 29, 2, TZM))
        return false;
    return true;
}


std::optional<LogIterator::Error> LogIterator::readLog(LogEntry & entry)
{
    if (!log_input_stream)
    {
        if (log_input_stream.eof())
            return std::optional<Error>(Error{Error::Type::EOI});
        else
            return std::optional<Error>(Error{Error::Type::UNKNOWN});
    }

    std::getline(log_input_stream, line);
    cur_lineno++;

    thread_local char prev_time_buff[35] = {0};
    thread_local time_t prev_time_t;

    constexpr size_t timestamp_finish_offset = 32;
    constexpr size_t log_level_start_finish_offset = 33;
    const char * loglevel_start;
    size_t loglevel_size;
    size_t loglevel_s;

    if (strncmp(prev_time_buff, line.data(), timestamp_finish_offset) == 0)
    {
        // If we can reuse prev_time
        entry.time = prev_time_t;

        if (!LogIterator::readLevel(line.size(), line.data(), loglevel_s, loglevel_size))
        {
            return std::optional<Error>(Error{Error::Type::UNEXPECTED_LOG_HEAD});
        }
        else
        {
            loglevel_start = line.data() + loglevel_s;
        }
    }
    else
    {
        int milli_second;
        int timezone_hour, timezone_min;
        std::tm time{};
        int year, month, day, hour, minute, second;
        if (LogIterator::readDate(
                line.size(),
                line.data(),
                year,
                month,
                day,
                hour,
                minute,
                second,
                milli_second,
                timezone_hour,
                timezone_min)
            && LogIterator::readLevel(line.size(), line.data(), loglevel_s, loglevel_size))
        {
            loglevel_start = line.data() + loglevel_s;
        }
        else
        {
            return std::optional<Error>(Error{Error::Type::UNEXPECTED_LOG_HEAD});
        }
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        time.tm_hour = hour;
        time.tm_min = minute;
        time.tm_sec = second;
        time_t ctime = fast_mktime(&time) * 1000; // milliseconds
        ctime += milli_second; // truncate microseconds
        entry.time = ctime;

        memset(prev_time_buff, 0, sizeof prev_time_buff);
        strncpy(prev_time_buff, line.data(), timestamp_finish_offset);
        prev_time_t = ctime;
    }

    if (entry.time > end_time)
        return std::optional<Error>(Error{Error::Type::EOI});

    if (memcmp(loglevel_start, "TRACE", loglevel_size) == 0)
    {
        entry.level = LogEntry::Level::Trace;
    }
    else if (memcmp(loglevel_start, "DEBUG", loglevel_size) == 0)
    {
        entry.level = LogEntry::Level::Debug;
    }
    else if (memcmp(loglevel_start, "INFO", loglevel_size) == 0)
    {
        entry.level = LogEntry::Level::Info;
    }
    else if (memcmp(loglevel_start, "WARN", loglevel_size) == 0)
    {
        entry.level = LogEntry::Level::Warn;
    }
    else if (memcmp(loglevel_start, "ERROR", loglevel_size) == 0)
    {
        entry.level = LogEntry::Level::Error;
    }
    if (loglevel_size == 0)
        return std::optional<Error>(Error{Error::Type::INVALID_LOG_LEVEL});

    size_t message_begin = log_level_start_finish_offset + loglevel_size + 3; // [] and a space
    if (line.size() <= message_begin)
    {
        return std::optional<Error>(Error{Error::Type::UNEXPECTED_LOG_HEAD});
    }
    entry.message = std::string_view(line);
    entry.message = entry.message.substr(message_begin, std::string_view::npos);
    return {};
}

void LogIterator::init()
{
    // Check empty, if empty then fill with ".*"
    if (patterns.empty() || (patterns.size() == 1 && patterns[0].empty()))
    {
        patterns = {".*"};
    }
    for (auto && pattern : patterns)
    {
        compiled_patterns.push_back(std::make_unique<RE2>(pattern));
    }
}


LogIterator::~LogIterator()
{
    if (err_info.has_value())
    {
        LOG_DEBUG(
            log,
            "LogIterator search end with error {} at line {}.",
            magic_enum::enum_name(err_info->second),
            err_info->first);
    }
}

// read approximate timestamp, round up to second, return -1 if failed.
int64_t readApproxiTimestamp(const char * start, const char * date_format)
{
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    int milli_second;

    if (std::sscanf(start, date_format, &year, &month, &day, &hour, &minute, &second, &milli_second) != 7)
    {
        return -1;
    }

    std::tm time{};
    time.tm_year = year - 1900;
    time.tm_mon = month - 1;
    time.tm_mday = day;
    time.tm_hour = hour;
    time.tm_min = minute;
    time.tm_sec = second + (milli_second ? 1 : 0); // ignore millisecond
    time_t ctime = mktime(&time) * 1000;

    return ctime;
}

// if name ends with date format and timestamp < start-time, return true.
// otherwise(can NOT tell ts) return false.
bool filterLogEndDatetime(
    const std::string & path,
    const std::string & example,
    const char * date_format,
    const int64_t start_time)
{
    if (path.size() > example.size())
    {
        auto tso = readApproxiTimestamp(path.data() + path.size() - example.size(), date_format);
        if (tso == -1)
            return false;
        if (tso < start_time)
            return true;
    }
    return false;
}

static const std::string gz_suffix = ".gz";

// if timestamp of log file could be told, return whether it can be filtered.
bool FilterFileByDatetime(
    const std::string & path,
    const std::vector<std::string> & ignore_log_file_prefixes,
    const int64_t start_time)
{
    static const std::string date_format_example = "0000-00-00-00:00:00.000";
    static const char * date_format = "%d-%d-%d-%d:%d:%d.%d";

    static const std::string raftstore_proxy_date_format_example = "0000-00-00T00:00:00.000.log";
    static const char * raftstore_proxy_date_format = "%d-%d-%dT%d-%d-%d.%d";

    for (const auto & ignore_log_file_prefix : ignore_log_file_prefixes)
    {
        if (!ignore_log_file_prefix.empty() && startsWith(path, ignore_log_file_prefix))
            return true;
    }
    if (endsWith(path, gz_suffix))
    {
        if (path.size() <= gz_suffix.size() + date_format_example.size())
            return false;

        auto date_str
            = std::string(path.end() - gz_suffix.size() - date_format_example.size(), path.end() - gz_suffix.size());

        if (auto ts = readApproxiTimestamp(date_str.data(), date_format); ts == -1)
        {
            return false;
        }
        else if (ts < start_time)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        // filter proxy log end datetime
        return filterLogEndDatetime(path, raftstore_proxy_date_format_example, raftstore_proxy_date_format, start_time);
    }
}

// if path ends with `.gz`, try to read by `Poco::InflatingInputStream`
void ReadLogFile(const std::string & path, std::function<void(std::istream &)> && cb)
{
    if (endsWith(path, gz_suffix))
    {
        std::ifstream istr(path, std::ios::binary);
        Poco::InflatingInputStream inflater(istr, Poco::InflatingStreamBuf::STREAM_GZIP);
        cb(inflater);
        istr.close();
    }
    else
    {
        std::ifstream istr(path);
        cb(istr);
        istr.close();
    }
}


} // namespace DB
