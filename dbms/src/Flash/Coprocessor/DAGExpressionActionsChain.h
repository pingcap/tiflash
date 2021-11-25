#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{
/** The sequence of transformations over the block.
  * It is assumed that the result of each step is fed to the input of the next step.
  * Used to execute parts of the query individually.
  *
  * For example, you can create a chain of two steps:
  *     1) evaluate the expression in the WHERE clause,
  *     2) calculate the expression in the SELECT section,
  * and between the two steps do the filtering by value in the WHERE clause.
  */
struct DAGExpressionActionsChain
{
    struct Step
    {
        String desc;
        ExpressionActionsPtr actions;
        Names required_output;

        using Callback = std::function<void(const ExpressionActionsPtr &)>;
        Callback callback;

        Step(const ExpressionActionsPtr & actions_ = nullptr, const Names & required_output_ = Names())
            : actions(actions_)
            , required_output(required_output_)
        {}

        void setCallback(String desc_, Callback callback_);
    };

    using Steps = std::vector<Step>;

    Settings settings;
    Steps steps;

    void addStep();

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ExpressionActionsPtr getLastActions()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back().actions;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back();
    }

    std::string dumpChain();
};
} // namespace DB
