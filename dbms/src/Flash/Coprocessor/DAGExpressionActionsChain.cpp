#include <Flash/Coprocessor/DAGExpressionActionsChain.h>
#include <fmt/core.h>

namespace DB
{
void DAGExpressionActionsChain::Step::setCallback(String desc_, DAGExpressionActionsChain::Step::Callback callback_)
{
    if (!desc.empty() || callback)
        throw Exception(
            fmt::format("Try to overwrite step callback. old desc: {}, new desc: {}.", desc, desc_),
            ErrorCodes::LOGICAL_ERROR);

    if (desc_.empty() || !callback_)
        throw Exception(
            fmt::format("Try to write empty step callback. desc: {}.", desc_),
            ErrorCodes::LOGICAL_ERROR);

    desc = std::move(desc_);
    callback = std::move(callback_);
}

void DAGExpressionActionsChain::addStep()
{
    if (steps.empty())
        throw Exception("Cannot add action to empty DAGExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumnsWithTypeAndName();
    steps.push_back(Step(std::make_shared<ExpressionActions>(columns, settings)));
}

void DAGExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        Names required_output = steps[i].required_output;
        if (i + 1 < static_cast<int>(steps.size()))
        {
            for (const auto & it : steps[i + 1].actions->getRequiredColumnsWithTypes())
                required_output.push_back(it.name);
        }
        steps[i].actions->finalize(required_output);
    }

    /// When possible, move the ARRAY JOIN from earlier steps to later steps.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        ExpressionAction action;
        if (steps[i - 1].actions->popUnusedArrayJoin(steps[i - 1].required_output, action))
            steps[i].actions->prependArrayJoin(action, steps[i - 1].actions->getSampleBlock());
    }

    /// Adding the ejection of unnecessary columns to the beginning of each step.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        size_t columns_from_previous = steps[i - 1].actions->getSampleBlock().columns();

        /// If unnecessary columns are formed at the output of the previous step, we'll add them to the beginning of this step.
        /// Except when we drop all the columns and lose the number of rows in the block.
        if (!steps[i].actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > steps[i].actions->getRequiredColumnsWithTypes().size())
            steps[i].actions->prependProjectInput();
    }

    for (const auto & step : steps)
    {
        if (!step.actions->getActions().empty() && !step.callback)
            throw Exception("Forgot to handle actions, desc:" + step.desc, ErrorCodes::LOGICAL_ERROR);

        if (step.callback)
            step.callback(step.actions);
    }
}

std::string DAGExpressionActionsChain::dumpChain()
{
    std::stringstream ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "desc " << steps[i].desc << "\n";
        ss << "required output:\n";
        for (const std::string & name : steps[i].required_output)
            ss << name << "\n";
        ss << "\n"
           << steps[i].actions->dumpActions() << "\n";
    }

    return ss.str();
}
} // namespace DB
