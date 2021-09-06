#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
namespace DB
{
/// read blocks directly from Union, then broadcast or partition blocks and encode them, later put them into sending tunnels
class ExchangeSender : public IProfilingBlockInputStream
{
public:
    ExchangeSender(const BlockInputStreamPtr & input, std::unique_ptr<DAGResponseWriter> writer)
        : writer(std::move(writer))
        , log(&Poco::Logger::get("ExchangeSender"))
    {
        children.push_back(input);
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
    Poco::Logger * log;
};

} // namespace DB
