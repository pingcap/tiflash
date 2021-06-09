#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
namespace DB
{

class HashJoinBuildBlockInputStream : public IProfilingBlockInputStream
{
public:
    HashJoinBuildBlockInputStream(const BlockInputStreamPtr & input, JoinPtr join_): join(std::move(join_)) {
        children.push_back(input);
        join->setFinishBuildTable(false);
    }
    String getName() const override { return "HashJoinBuildBlockInputStream"; }
    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override;

private:
    JoinPtr join;
};

} // namespace DB