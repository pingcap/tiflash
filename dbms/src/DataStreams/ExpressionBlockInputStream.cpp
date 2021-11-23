#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{
ExpressionBlockInputStream::ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, const LogWithPrefixPtr & log_)
    : expression(expression_)
    , log(getMPPTaskLog(log_, getName()))
{
    children.push_back(input);
}

String ExpressionBlockInputStream::getName() const
{
    return "Expression";
}

Block ExpressionBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
}

Block ExpressionBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    expression->execute(res);
    return res;
}

Block ExpressionBlockInputStream::readImpl()
{
    auto timer = newTimer(Timeline::PULL);
    Block res = children.back()->read();
    timer.switchTo(Timeline::SELF);

    if (!res)
        return res;

    expression->execute(res);

    return res;
}

} // namespace DB
