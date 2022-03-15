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

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <benchmark/benchmark.h>
#include <common/logger_useful.h>

#include <iomanip>

namespace bench
{
class LoggerMacroBM : public benchmark::Fixture
{
protected:
    static constexpr size_t num_repeat = 10000;
    static constexpr auto * log_file_path = "/tmp/logger_test";

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
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("[%H:%M:%S.%i %Z] [%p] [%U(%u)]: %t"));
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
        return &Poco::Logger::get("LoggerMacroBM");
    }
};

BENCHMARK_DEFINE_F(LoggerMacroBM, ShortOldStream)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_INFO(log, " GC exit within " << elapsed_sec << " sec.");
        }
    }
}
BENCHMARK_REGISTER_F(LoggerMacroBM, ShortOldStream)->Iterations(200);

BENCHMARK_DEFINE_F(LoggerMacroBM, ShortOldFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_INFO(log, fmt::format(" GC exit within {} sec.", elapsed_sec));
        }
    }
}
BENCHMARK_REGISTER_F(LoggerMacroBM, ShortOldFmt)->Iterations(200);

BENCHMARK_DEFINE_F(LoggerMacroBM, ShortFmt)
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
BENCHMARK_REGISTER_F(LoggerMacroBM, ShortFmt)->Iterations(200);

BENCHMARK_DEFINE_F(LoggerMacroBM, LoogOldStream)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_INFO(
                log,
                " GC exit within " << std::setprecision(2) << elapsed_sec << " sec. PageFiles from " //
                                   << beg.first << "_" << beg.second << " to "
                                   << end.first << "_" << end.second //
                                   << ", min writing " << min.first << "_" << min.second
                                   << ", num files: " << num_files << ", num legacy:" << num_legacy
                                   << ", compact legacy archive files: " << num_compact
                                   << ", remove data files: " << num_removed);
        }
    }
}
BENCHMARK_REGISTER_F(LoggerMacroBM, LoogOldStream)->Iterations(200);

BENCHMARK_DEFINE_F(LoggerMacroBM, LoogOldFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            LOG_INFO(
                log,
                fmt::format(
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
                    num_removed));
        }
    }
}
BENCHMARK_REGISTER_F(LoggerMacroBM, LoogOldFmt)->Iterations(200);

BENCHMARK_DEFINE_F(LoggerMacroBM, LoogFmt)
(benchmark::State & state)
{
    auto * log = getLogger();
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;
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
BENCHMARK_REGISTER_F(LoggerMacroBM, LoogFmt)->Iterations(200);

} // namespace bench
