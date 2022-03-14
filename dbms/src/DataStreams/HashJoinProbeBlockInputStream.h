#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
class ExpressionActions;

/** Executes a certain expression over the block.
  * Basically the same as ExpressionBlockInputStream,
  * but requires that there must be a join probe action in the Expression.
  *
  * The join probe action is different from the general expression
  * and needs to be executed after join hash map building.
  * We should separate it from the ExpressionBlockInputStream.
  */
class HashJoinProbeBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    static constexpr auto name = "HashJoinProbe";

public:
    HashJoinProbeBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & expression_,
        const LogWithPrefixPtr & log_);

    String getName() const override { return name; }
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    const LogWithPrefixPtr log;
    ExpressionActionsPtr expression;
};

} // namespace DB
