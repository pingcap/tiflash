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
        LogEntry entry;
        Error err = readLog(entry);
        if (err.tp != Error::Type::OK)
        {
            if (err.tp != Error::Type::EOI)
            {
                LOG_ERROR(log, "readLog error: " << err.extra_msg);
            }
            return false;
        }

        msg.set_time(entry.time);
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

        if (match(msg, entry.message.data(), entry.message.size()))
        {
            std::string tmp_message{entry.message};
            msg.set_message(std::move(tmp_message));
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

bool LogIterator::match(const LogMessage & log_msg, const char * c, size_t sz) const
{
    // Check time range
    if (log_msg.time() >= end_time || log_msg.time() < start_time)
        return false;

    // Check level
    if (std::find(levels.begin(), levels.end(), log_msg.level()) == levels.end() && !levels.empty())
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

static inline bool read_level(size_t limit, const char * s, size_t & level_start, size_t & level_size)
{
    level_start = 33;
    level_size = 0;
    if (level_start >= limit)
        return false;
    if (s[level_start] != '[')
        return false;
    level_start++;
    while (1)
    {
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

static inline bool read_date(
    size_t limit, const char * s, int & y, int & m, int & d, int & H, int & M, int & S, int & MS, int & TZH, int & TZM)
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


LogIterator::Error LogIterator::readLog(LogEntry & entry)
{
    if (!*log_file)
    {
        if (log_file->eof())
            return Error{Error::Type::EOI};
        else
            return Error{Error::Type::UNKNOWN};
    }

    std::getline(*log_file, line);

    thread_local char level_buff[20] = {0};
    thread_local char prev_time_buff[35] = {0};
    thread_local time_t prev_time_t;

    constexpr size_t kTimestampFinishOffset = 32;
    constexpr size_t kLogLevelStartFinishOffset = 33;
    const char * loglevel_start;
    size_t loglevel_size;
    size_t loglevel_s;

    if (strncmp(prev_time_buff, line.data(), kTimestampFinishOffset) == 0)
    {
        // If we can reuse prev_time
        entry.time = prev_time_t;

        if (!read_level(line.size(), line.data(), loglevel_s, loglevel_size))
        {
            std::sscanf(line.data() + kLogLevelStartFinishOffset, "[%[^]]s]", level_buff);
            loglevel_start = level_buff;
            loglevel_size = strlen(level_buff);
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
        if (read_date(line.size(), line.data(), year, month, day, hour, minute, second, milli_second, timezone_hour, timezone_min)
            && read_level(line.size(), line.data(), loglevel_s, loglevel_size))
        {
            loglevel_start = line.data() + loglevel_s;
        }
        else
        {
            if (std::sscanf(line.data(), "[%d/%d/%d %d:%d:%d.%d %d:%d] [%20[^]]s]", &year, &month, &day, &hour, &minute, &second,
                    &milli_second, &timezone_hour, &timezone_min, level_buff)
                != 10)
            {
                return Error{Error::Type::UNEXPECTED_LOG_HEAD};
            }
            loglevel_start = level_buff;
            loglevel_size = strlen(level_buff);
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
        return Error{Error::Type::INVALID_LOG_LEVEL, "level: " + std::string(level_buff)};

    size_t message_begin = kLogLevelStartFinishOffset + loglevel_size + 3; // [] and a space
    if (line.size() <= message_begin)
    {
        return Error{Error::Type::UNEXPECTED_LOG_HEAD};
    }
    entry.message = std::string_view(line);
    entry.message = entry.message.substr(message_begin, std::string_view::npos);
    return Error{Error::Type::OK};
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
