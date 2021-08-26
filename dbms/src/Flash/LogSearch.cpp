#include <Flash/LogSearch.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
using ::diagnosticspb::LogLevel;
using ::diagnosticspb::LogMessage;

static time_t fast_mktime(struct tm * tm)
{
    thread_local struct tm cache = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
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

bool LogIterator::next(::diagnosticspb::LogMessage & msg)
{
    for (;;)
    {
        auto result = readLog();
        if (auto err = std::get_if<Error>(&result); err)
        {
            if (err->tp != Error::Type::EOI)
            {
                LOG_ERROR(log, "readLog error: " << err->extra_msg);
            }
            return false;
        }

        auto entry = std::get<LogEntry>(result);
        msg.set_time(entry.time);
        msg.set_message(entry.message);
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
        msg.set_level(level);

        if (match(msg))
        {
            return true;
        }
    }
}

std::optional<LogMessage> LogIterator::next()
{
    LogMessage msg;
    if (!next(msg))
    {
        return {};
    }
    return msg;
}

bool LogIterator::match(const LogMessage & log_msg) const
{
    // Check time range
    if (log_msg.time() >= end_time || log_msg.time() < start_time)
        return false;

    // Check level
    if (std::find(levels.begin(), levels.end(), log_msg.level()) == levels.end() && !levels.empty())
        return false;

    // Grep
    auto & content = log_msg.message();
    for (auto && regex : compiled_patterns)
    {
        if (!RE2::PartialMatch(content, *regex))
            return false;
    }

    return true;
}

LogIterator::Result<LogIterator::LogEntry> LogIterator::readLog()
{
    if (!*log_file)
    {
        if (log_file->eof())
            return Error{Error::Type::EOI};
        else
            return Error{Error::Type::UNKNOWN};
    }

    std::getline(*log_file, line);

    LogEntry entry;

    thread_local char level_buff[20];
    thread_local char prev_time_buff[35] = {0};
    thread_local time_t prev_time_t;

    constexpr size_t kTimestampFinishOffset = 32;
    constexpr size_t kLogLevelStartFinishOffset = 33;

    if (strncmp(prev_time_buff, line.data(), kTimestampFinishOffset) == 0)
    {
        // If we can reuse prev_time
        entry.time = prev_time_t;

        std::sscanf(line.data() + kLogLevelStartFinishOffset, "[%[^]]s]", level_buff);
    }
    else
    {
        int milli_second;
        int timezone_hour, timezone_min;
        std::tm time{};
        int year, month, day, hour, minute, second;
        if (std::sscanf(line.data(), "[%d/%d/%d %d:%d:%d.%d %d:%d] [%20[^]]s]", &year, &month, &day, &hour, &minute, &second, &milli_second,
                &timezone_hour, &timezone_min, level_buff)
            != 10)
        {
            return Error{Error::Type::UNEXPECTED_LOG_HEAD};
        }
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        time.tm_hour = hour;
        time.tm_min = minute;
        time.tm_sec = second;
        time_t ctime = fast_mktime(&time) * 1000; // milliseconds
        ctime += milli_second;                    // truncate microseconds
        entry.time = ctime;

        memset(prev_time_buff, 0, sizeof prev_time_buff);
        strncpy(prev_time_buff, line.data(), kTimestampFinishOffset);
        prev_time_t = ctime;
    }

    if (entry.time > end_time)
        return Error{Error::Type::EOI};

    size_t level_buff_len = 0;
    if (memcmp(level_buff, "TRACE", 5) == 0)
    {
        entry.level = LogEntry::Level::Trace;
        level_buff_len = 5;
    }
    else if (memcmp(level_buff, "DEBUG", 5) == 0)
    {
        entry.level = LogEntry::Level::Debug;
        level_buff_len = 5;
    }
    else if (memcmp(level_buff, "INFO", 4) == 0)
    {
        entry.level = LogEntry::Level::Info;
        level_buff_len = 4;
    }
    else if (memcmp(level_buff, "WARN", 4) == 0)
    {
        entry.level = LogEntry::Level::Warn;
        level_buff_len = 4;
    }
    else if (memcmp(level_buff, "ERROR", 5) == 0)
    {
        entry.level = LogEntry::Level::Error;
        level_buff_len = 5;
    }
    if (level_buff_len == 0 || level_buff[level_buff_len] != '\0')
        return Error{Error::Type::INVALID_LOG_LEVEL, "level: " + std::string(level_buff)};

    size_t message_begin = kLogLevelStartFinishOffset + level_buff_len + 3;
    if (line.size() <= message_begin)
    {
        return Error{Error::Type::UNEXPECTED_LOG_HEAD};
    }
    entry.message.assign(line.begin() + message_begin, line.end());
    return entry;
}

void LogIterator::init()
{
    // Check empty, if empty then fill with ".*"
    if (patterns.size() == 0 || (patterns.size() == 1 && patterns[0] == ""))
    {
        patterns = {".*"};
    }
    for (auto && pattern : patterns)
    {
        compiled_patterns.push_back(std::make_unique<RE2>(pattern));
    }
}

} // namespace DB
