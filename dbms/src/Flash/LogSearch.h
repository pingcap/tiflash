#pragma once

#include <Poco/File.h>
#include <common/logger_useful.h>
#include <re2/re2.h>

#include <boost/noncopyable.hpp>
#include <fstream>
#include <istream>
#include <memory>
#include <variant>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class LogIterator : private boost::noncopyable
{
public:
    explicit LogIterator(int64_t _start_time, int64_t _end_time, const std::vector<::diagnosticspb::LogLevel> & _levels,
        const std::vector<std::string> & _patterns, std::shared_ptr<std::istream> _log_file)
        : start_time(_start_time),
          end_time(_end_time),
          levels(_levels),
          patterns(_patterns),
          log_file(_log_file),
          log(&Poco::Logger::get("LogIterator"))
    {
        // Check empty, if empty then fill with ".*"
        if (patterns.size() == 0 || (patterns.size() == 1 && patterns[0] == ""))
        {
            patterns = {".*"};
        }
    }

public:
    static constexpr size_t MAX_MESSAGE_SIZE = 4096;

public:
    std::optional<::diagnosticspb::LogMessage> next();

public:
    struct Error
    {
        enum Type
        {
            EOI,
            INVALID_LOG_LEVEL,
            UNEXPECTED_LOG_HEAD,
            UNKNOWN
        };
        Type tp;
        std::string extra_msg = "";
    };

    template <typename T>
    using Result = std::variant<T, Error>;

    struct LogEntry
    {
        int64_t time;
        size_t thread_id;

        enum Level
        {
            Trace,
            Debug,
            Info,
            Warn,
            Error
        };
        Level level;

        std::string channel;
        std::string message;
    };

private:
    static Result<::diagnosticspb::LogMessage> parseLog(const std::string & log_content);
    bool match(const ::diagnosticspb::LogMessage & log_msg) const;

    Result<LogEntry> readLog();

private:
    int64_t start_time;
    int64_t end_time;
    std::vector<::diagnosticspb::LogLevel> levels;
    std::vector<std::string> patterns;
    std::shared_ptr<std::istream> log_file;

    Poco::Logger * log;
};

}; // namespace DB
