#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
class ExpressionActions;

/** Executes a certain expression over the block.
  * Expression should have join action.
  */
class HashJoinProbeBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    static constexpr auto NAME = "HashJoinProbe";

public:
    HashJoinProbeBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & expression_,
        const LogWithPrefixPtr & log_);

    String getName() const override { return NAME; }
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    const LogWithPrefixPtr log;
    ExpressionActionsPtr expression;
};

} // namespace DB
