// Copyright 2022 PingCAP, Ltd.
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

#include <Common/UnifiedLogPatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/PatternFormatter.h>
#include <benchmark/benchmark.h>
#include <common/logger_useful.h>

#include <iomanip>

namespace DB
{
namespace bench
{
static void UnifiedLogFormatterBM(benchmark::State & state)
{
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;

    UnifiedLogPatternFormatter formatter;
    for (auto _ : state)
    {
        std::string formatted_text;
        auto text = fmt::format(" GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}"
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
        Poco::Message msg(
            /*source*/ "log_name",
            /*text*/ text,
            /*prio*/ Poco::Message::PRIO_INFORMATION,
            /*file*/ &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)],
            /*line*/ __LINE__);
        formatter.format(msg, formatted_text);
    }
}
BENCHMARK(UnifiedLogFormatterBM)->Iterations(1000000);

class UnifiedLogBM : public benchmark::Fixture
{
protected:
    static constexpr size_t num_repeat = 10000;
    static constexpr auto * log_file_path = "/tmp/unified_logger_test";

    /* some data for test formatting */
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;

public:
    void SetUp(const ::benchmark::State & /*state*/)
    {
        if (Poco::File f(log_file_path); f.exists())
        {
            f.remove();
        }
    }

    static Poco::Logger * getLogger()
    {
        Poco::AutoPtr<Poco::FileChannel> channel(new Poco::FileChannel(log_file_path));
        Poco::AutoPtr<UnifiedLogPatternFormatter> formatter(new UnifiedLogPatternFormatter());
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
        return &Poco::Logger::get("UnifiedLogBM");
    }
};

BENCHMARK_DEFINE_F(UnifiedLogBM, ShortOldStream)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(log, " GC exit within {} sec.", elapsed_sec);
        }
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, ShortOldStream)->Iterations(200);

BENCHMARK_DEFINE_F(UnifiedLogBM, ShortOldFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(log, " GC exit within {} sec.", elapsed_sec);
        }
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, ShortOldFmt)->Iterations(200);

BENCHMARK_DEFINE_F(UnifiedLogBM, ShortFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(log, " GC exit within {} sec.", elapsed_sec);
        }
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, ShortFmt)->Iterations(200);

BENCHMARK_DEFINE_F(UnifiedLogBM, LoogOldStream)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(
                log,
                " GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}, num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
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
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, LoogOldStream)->Iterations(200);

BENCHMARK_DEFINE_F(UnifiedLogBM, LoogOldFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(
                log,
                "GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}, num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
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
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, LoogOldFmt)->Iterations(200);

BENCHMARK_DEFINE_F(UnifiedLogBM, LoogFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_FMT_INFO(
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
    }
}
BENCHMARK_REGISTER_F(UnifiedLogBM, LoogFmt)->Iterations(200);

} // namespace bench

} // namespace DB