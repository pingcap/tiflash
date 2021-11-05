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
    std::stringstream log_str;
    log_str << "Expression \t"
            << "type: " << expression->getActions()[0].type << "\t"
            << "input_columns" << "system"
    std::string res = "Expression \t" +
            "type: \t" +
                      "input_columns";
    return ;
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
    Block res = children.back()->read();
    if (!res)
        return res;
    expression->execute(res);
    return res;
}

} // namespace DB
