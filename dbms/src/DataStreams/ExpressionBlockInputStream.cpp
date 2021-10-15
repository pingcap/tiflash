#include <DataStreams/DumpUtils.h>
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
    const auto & actions = expression->getActions();
    dumpIter(actions.cbegin(), actions.cend(), ostr, [](const auto & s, std::ostream & os) { os << s.toString(); }, "; ");
    ostr << "} input: {";
    const auto & input_columns = expression->getRequiredColumnsWithTypes();
    dumpIter(input_columns.cbegin(), input_columns.cend(), ostr, [](const auto & s, std::ostream & os) { os << s.name << '(' << s.type->getName() << ')'; });
    ostr << "} output: {";
    const auto & output = expression->getSampleBlock();
    dumpIter(output.cbegin(), output.cend(), ostr, [](const auto & s, std::ostream & os) { os << s.name << '(' << s.type->getName() << ')'; });
    ostr << "} ]";
}

} // namespace DB
