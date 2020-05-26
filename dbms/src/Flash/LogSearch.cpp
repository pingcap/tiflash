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
    std::stringstream buff;

    // TiFlash log format: YYYY.MM.DD hh:mm:ss.mmmmmm [ ThreadID ] <Level> channel: message
    RE2 head_line_pattern("^\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{6}\\s\\[\\s\\d+\\s\\]\\s\\<\\w+\\>\\s.*");
    if (RE2::FullMatch(line, head_line_pattern))
    {
        buff << line;
        while (getline(*log_file, line))
        {
            if (!RE2::FullMatch(line, head_line_pattern))
            {
                buff << "\n";
                buff << line;
                if (log_file->eof())
                    break;
            }
            else
            {
                size_t offset = line.size();
                if (log_file->eof())
                {
                    // We should call clear before seek if the fstream has reached end.
                    log_file->clear();
                }
                // Seek back to start of the line. Notice that getline will omit '\n', so we should handle it manually.
                log_file->seekg(-(offset + 1), log_file->cur);
                break;
            }
        }
    }
    else
    {
        return Error{Error::Type::UNEXPECTED_LOG_HEAD, line};
    }

    LogEntry entry;

    std::string log_content = buff.str();

    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    int milli_second;

    int thread_id;
    char level_buff[20];

    std::sscanf(log_content.data(), "%d.%d.%d %d:%d:%d.%d [ %d ] %s ", &year, &month, &day, &hour, &minute, &second, &milli_second,
        &thread_id, level_buff);

    {
        std::tm time;
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        time.tm_hour = hour;
        time.tm_min = minute;
        time.tm_sec = second;

        time_t ctime = mktime(&time) * 1000; // milliseconds
        ctime += milli_second / 1000;        // truncate microseconds

        entry.time = ctime;
        if (entry.time > end_time)
            return Error{Error::Type::EOI};
    }

    {
        std::string level_str(level_buff);
        if (level_str == "<Trace>")
            entry.level = LogEntry::Level::Trace;
        else if (level_str == "<Debug>")
            entry.level = LogEntry::Level::Debug;
        else if (level_str == "<Information>")
            entry.level = LogEntry::Level::Info;
        else if (level_str == "<Warning>")
            entry.level = LogEntry::Level::Warn;
        else if (level_str == "<Error>")
            entry.level = LogEntry::Level::Error;
        else
            return Error{Error::Type::INVALID_LOG_LEVEL, "level: " + level_str};
    }

    entry.thread_id = thread_id;

    {
        std::istringstream ss(log_content);
        int offset = 33 + std::to_string(thread_id).size() + std::string(level_buff).size();
        ss.seekg(offset, ss.beg);

        std::string message(std::istreambuf_iterator<char>(ss), {});
        entry.message = message;
    }

    return entry;
}

} // namespace DB
