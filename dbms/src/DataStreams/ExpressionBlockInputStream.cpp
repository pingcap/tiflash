#include <Interpreters/ExpressionActions.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <chrono>


namespace DB
{
ExpressionBlockInputStream::ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, Logger * mpp_task_log_)
    : expression(expression_), mpp_task_log(mpp_task_log_)
{
    children.push_back(input);
}

String ExpressionBlockInputStream::getName() const { return "Expression"; }

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
    Block res = children.back()->read();

    if (!res)
        return res;

    expression->execute(res);

    return res;
}

void ExpressionBlockInputStream::readSuffixImpl()
{
    if (mpp_task_log != nullptr)
    {
        String action_name("no action");
        if (expression != nullptr && expression->getActions().size() != 0)
            expression->getActions()[0].toString();

        LOG_TRACE(mpp_task_log, "ExpressionBlockInputStream-" << action_name
            << "total time:" << std::to_string(info.execution_time / 1000000UL) + "ms"
            << " total rows: " << info.rows
            << " total blocks: " << info.blocks
            << " total bytes:" << info.bytes);
    }
}

}
