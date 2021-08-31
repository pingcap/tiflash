#pragma once

#include <Poco/File.h>
#include <common/logger_useful.h>
#include <re2/re2.h>

#include <boost/noncopyable.hpp>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <variant>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class LogIterator : private boost::noncopyable
{
public:
    explicit LogIterator(int64_t _start_time, int64_t _end_time, const std::vector<::diagnosticspb::LogLevel> & _levels, const std::vector<std::string> & _patterns, std::unique_ptr<std::istream> && _log_file)
        : start_time(_start_time)
        , end_time(_end_time)
        , levels(_levels)
        , patterns(_patterns)
        , log_file(std::move(_log_file))
        , log(&Poco::Logger::get("LogIterator"))
        , cur_lineno(0)
    {
        init();
    }

    ~LogIterator();

public:
    static constexpr size_t MAX_MESSAGE_SIZE = 4096;

public:
    std::optional<::diagnosticspb::LogMessage> next();
    bool next(::diagnosticspb::LogMessage & msg);
    static bool read_level(size_t limit, const char * s, size_t & level_start, size_t & level_size);
    static bool read_date(
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
        int & TZM);

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

        std::string_view message;
    };

private:
    static Result<::diagnosticspb::LogMessage> parseLog(const std::string & log_content);
    bool match(const ::diagnosticspb::LogMessage & log_msg, const char * c, size_t sz) const;
    void init();

    std::optional<Error> readLog(LogEntry &);

private:
    int64_t start_time;
    int64_t end_time;
    std::vector<::diagnosticspb::LogLevel> levels;
    std::vector<std::string> patterns;
    std::vector<std::unique_ptr<RE2>> compiled_patterns;
    std::unique_ptr<std::istream> log_file;
    std::string line;

    Poco::Logger * log;

    uint32_t cur_lineno;
    std::optional<std::pair<uint32_t, Error::Type>> err_info; // <lineno, Error::Type>
};

}; // namespace DB
