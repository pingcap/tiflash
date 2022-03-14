#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const ExpressionActionsPtr & expression_,
    const LogWithPrefixPtr & log_)
    : log(getMPPTaskLog(log_, NAME))
{
    children.push_back(input);

    bool has_join_probe_action = false;
    if (expression_)
    {
        for (const auto & action : expression_->getActions())
        {
            if (action.type == ExpressionAction::Type::JOIN)
            {
                has_join_probe_action = true;
                break;
            }
        }
    }
    if (!has_join_probe_action)
    {
        throw Exception("join probe expression should have join action", ErrorCodes::LOGICAL_ERROR);
    }
    expression = expression_;
}

Block HashJoinProbeBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    expression->execute(res);
    return res;
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;
    expression->execute(res);
    return res;
}

} // namespace DB
