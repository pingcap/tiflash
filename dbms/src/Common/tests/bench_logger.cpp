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
#include <Common/UnifiedLogFormatter.h>
#include <Common/tests/TestChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <benchmark/benchmark.h>
#include <common/logger_useful.h>

namespace DB
{
namespace bench
{
static void UnifiedLogFormatterBM(benchmark::State & state)
{
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;
    auto text = fmt::format(
        " GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}"
        ", num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
        elapsed_sec,
        beg.first,
        beg.second,
        end.first,
        end.second,
        min.first,
        min.second,
        num_files,
        num_legacy,
        num_compact,
        num_removed);

    UnifiedLogFormatter formatter;
    for (auto _ : state)
    {
        std::string formatted_text;
        Poco::Message msg(
            /*source*/ "log_name",
            /*text*/ text,
            /*prio*/ Poco::Message::PRIO_INFORMATION,
            /*file*/ &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)],
            /*line*/ __LINE__);
        formatter.format(msg, formatted_text);
    }
}
BENCHMARK(UnifiedLogFormatterBM);

class LogBM : public benchmark::Fixture
{
protected:
    /* some data for test formatting */
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;

public:
    void SetUp(const ::benchmark::State & /*state*/) override
    {
        Poco::AutoPtr<Poco::Channel> channel(new TestChannel());
        Poco::AutoPtr<Poco::Formatter> formatter(new UnifiedLogFormatter<false>());
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
    }
};

#define BENCHMARK_LOGGER(Name, ...)            \
    BENCHMARK_F(LogBM, PocoLogger_##Name)      \
    (benchmark::State & state)                 \
    {                                          \
        auto * log = &Poco::Logger::root();    \
        __VA_ARGS__;                           \
    }                                          \
    BENCHMARK_F(LogBM, PropsLogger_##Name)     \
    (benchmark::State & state)                 \
    {                                          \
        auto log = Logger::get("trace_id=50"); \
        __VA_ARGS__;                           \
    }

BENCHMARK_LOGGER(WithoutFmt, {
    for (auto _ : state)
    {
        LOG_INFO(log, " GC exit within 5 sec.");
    }
})

BENCHMARK_LOGGER(FmtShort, {
    for (auto _ : state)
    {
        LOG_INFO(log, " GC exit within {} sec.", elapsed_sec);
    }
})

BENCHMARK_LOGGER(FmtLong, {
    for (auto _ : state)
    {
        LOG_INFO(
            log,
            " GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}"
            ", num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
            elapsed_sec,
            beg.first,
            beg.second,
            end.first,
            end.second,
            min.first,
            min.second,
            num_files,
            num_legacy,
            num_compact,
            num_removed);
    }
})


} // namespace bench

} // namespace DB
