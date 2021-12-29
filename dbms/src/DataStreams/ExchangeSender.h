#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
namespace DB
{
/// read blocks directly from Union, then broadcast or partition blocks and encode them, later put them into sending tunnels
/// it should be an output stream but derived from the inputSteam
class ExchangeSender : public IProfilingBlockInputStream
{
public:
    ExchangeSender(const BlockInputStreamPtr & input, std::unique_ptr<DAGResponseWriter> writer_, const std::shared_ptr<LogWithPrefix> & log_ = nullptr)
        : writer(std::move(writer_))
        , log(getLogWithPrefix(log_, getName()))
    {
        children.push_back(input);
        writer->parent = this;
    }
    String getName() const override { return "ExchangeSender"; }
    Block getHeader() const override { return children.back()->getHeader(); }
    void readSuffix() override
    {
        writer->finishWrite();
    }

protected:
    Block readImpl() override;

private:
    std::unique_ptr<DAGResponseWriter> writer;
    std::shared_ptr<LogWithPrefix> log;
};

} // namespace DB
