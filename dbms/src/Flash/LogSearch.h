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

#pragma once

#include <Common/Logger.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <re2/re2.h>

#include <boost/noncopyable.hpp>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class LogIterator : private boost::noncopyable
{
public:
    explicit LogIterator(
        int64_t _start_time,
        int64_t _end_time,
        const std::vector<::diagnosticspb::LogLevel> & _levels,
        const std::vector<std::string> & _patterns,
        std::istream & _log_input_stream)
        : start_time(_start_time)
        , end_time(_end_time)
        , levels(_levels)
        , patterns(_patterns)
        , log_input_stream(_log_input_stream)
        , log(Logger::get())
        , cur_lineno(0)
    {
        init();
    }

    ~LogIterator();

public:
    static constexpr size_t MAX_MESSAGE_SIZE = 4096;

public:
    std::optional<::diagnosticspb::LogMessage> next();
    static bool readLevel(size_t limit, const char * s, size_t & level_start, size_t & level_size);
    static bool readDate(
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
    bool match(int64_t time, diagnosticspb::LogLevel level, const char * c, size_t sz) const;
    void init();

    std::optional<Error> readLog(LogEntry &);

private:
    int64_t start_time;
    int64_t end_time;
    std::vector<::diagnosticspb::LogLevel> levels;
    std::vector<std::string> patterns;
    std::vector<std::unique_ptr<RE2>> compiled_patterns;
    std::istream & log_input_stream;
    std::string line;

    LoggerPtr log;

    uint32_t cur_lineno;
    std::optional<std::pair<uint32_t, Error::Type>> err_info; // <lineno, Error::Type>
};

void ReadLogFile(const std::string & path, std::function<void(std::istream &)> && cb);

bool FilterFileByDatetime(
    const std::string & path,
    const std::vector<std::string> & ignore_log_file_prefixes,
    int64_t start_time);


}; // namespace DB
