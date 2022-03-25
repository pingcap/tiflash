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

#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
namespace DB
{
/// read blocks directly from Union, then broadcast or partition blocks and encode them, later put them into sending tunnels
/// it should be an output stream but derived from the inputSteam
class ExchangeSenderBlockInputStream : public IProfilingBlockInputStream
{
public:
    ExchangeSenderBlockInputStream(const BlockInputStreamPtr & input, std::unique_ptr<DAGResponseWriter> writer, const std::shared_ptr<LogWithPrefix> & log_ = nullptr)
        : writer(std::move(writer))
        , log(getLogWithPrefix(log_, name))
    {
        children.push_back(input);
    }
    static constexpr auto name = "ExchangeSender";
    String getName() const override { return name; }
    Block getHeader() const override { return children.back()->getHeader(); }
    void readSuffix() override
    {
        writer->finishWrite();
        LOG_FMT_DEBUG(log, "finish write with {} rows", total_rows);
    }

protected:
    Block readImpl() override;

private:
    std::unique_ptr<DAGResponseWriter> writer;
<<<<<<< HEAD
    std::shared_ptr<LogWithPrefix> log;
=======
    const LoggerPtr log;
    size_t total_rows = 0;
>>>>>>> 928e919b1c (Add some log to make it easier to observe data skew (#4404))
};

} // namespace DB
