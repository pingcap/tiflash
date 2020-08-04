#include <Flash/LogSearch.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
using ::diagnosticspb::LogLevel;
using ::diagnosticspb::LogMessage;

std::optional<LogMessage> LogIterator::next()
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
            return {};
        }

        auto entry = std::get<LogEntry>(result);
        LogMessage msg;
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
            return msg;
        }
    }
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
    for (auto & regex : patterns)
    {
        if (!RE2::PartialMatch(content, regex))
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

    std::string line;
    std::getline(*log_file, line);

    LogEntry entry;

    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    int milli_second;
    int timezone_hour;
    int timezone_min;

    char level_buff[20];

    std::sscanf(line.data(), "[%d/%d/%d %d:%d:%d.%d %d:%d] [%[^]]s]", &year, &month, &day, &hour, &minute, &second, &milli_second,
        &timezone_hour, &timezone_min, level_buff);

    {
        std::tm time;
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        time.tm_hour = hour;
        time.tm_min = minute;
        time.tm_sec = second;

        time_t ctime = mktime(&time) * 1000; // milliseconds
        ctime += milli_second;               // truncate microseconds

        entry.time = ctime;
        if (entry.time > end_time)
            return Error{Error::Type::EOI};
    }

    {
        std::string level_str(level_buff);
        if (level_str == "TRACE")
            entry.level = LogEntry::Level::Trace;
        else if (level_str == "DEBUG")
            entry.level = LogEntry::Level::Debug;
        else if (level_str == "INFO")
            entry.level = LogEntry::Level::Info;
        else if (level_str == "WARN")
            entry.level = LogEntry::Level::Warn;
        else if (level_str == "ERROR")
            entry.level = LogEntry::Level::Error;
        else
            return Error{Error::Type::INVALID_LOG_LEVEL, "level: " + level_str};
    }

    std::stringstream ss;
    {
        ss << line;
        std::string temp;
        // Trim timestamp and level sections
        ss >> temp >> temp >> temp >> temp;
        ss.seekg(1, std::ios::cur);
    }

    std::getline(ss, entry.message);

    return entry;
}

} // namespace DB
