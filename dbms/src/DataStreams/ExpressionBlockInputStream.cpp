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
    Block res = children.back()->read();
    if (!res)
        return res;
    expression->execute(res);
    return res;
}

void ExpressionBlockInputStream::dumpExtra(std::ostream & ostr) const
{
    ostr << "expression: [ actions: {";
    if (!expression->getActions().empty())
    {
        ostr << expression->getActions()[0].toString();
        for (size_t i = 1; i < expression->getActions().size(); ++i)
            ostr << "; " << expression->getActions()[i].toString();
    }
    ostr << "} input: {";
    const auto & input_columns = expression->getRequiredColumnsWithTypes();
    if (!input_columns.empty())
    {
        auto iter = input_columns.cbegin();
        ostr << iter->name << '(' << iter->type->getName() << ')';
        for (; iter != input_columns.cend(); ++iter)
        {
            ostr << ", " <<  iter->name << '(' << iter->type->getName() << ')';
        }
    }
    ostr << "} output: {";
    const auto & output = expression->getSampleBlock();
    if (output.columns() > 0)
    {
        const auto & first = output.getByPosition(0);
        ostr << first.name << '(' << first.type->getName() << ')';
        for (size_t i = 1; i < output.columns(); ++i)
        {
            const auto & c = output.getByPosition(i);
            ostr << ", " <<  c.name << '(' << c.type->getName() << ')';
        }
    }
    ostr << "} ]";
}

} // namespace DB
