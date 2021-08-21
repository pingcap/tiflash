#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Flash/Mpp/MPPHandler.h>
#include <common/logger_useful.h>

namespace DB
{

class HashJoinBuildBlockInputStream : public IProfilingBlockInputStream
{
public:
    HashJoinBuildBlockInputStream(const BlockInputStreamPtr & input, JoinPtr join_,size_t stream_index_, Logger * mpp_task_log_ = nullptr)
        :stream_index(stream_index_), mpp_task_log(mpp_task_log_)
    {
        children.push_back(input);
        join = join_;
    }

    String getName() const override { return "HashJoinBuildBlockInputStream"; }
    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    Logger * mpp_task_log;
    JoinPtr join;
    size_t stream_index;
};

} // namespace DB
