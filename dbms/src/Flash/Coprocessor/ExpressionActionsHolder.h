#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{
template<typename NameAndTypeArray>
Names toNames(const NameAndTypeArray & columns)
{
    Names names;
    names.reserve(columns.size());
    for (const auto & column : columns)
        names.push_back(column.name);
    return names;
}

struct ExpressionActionsHolder
{
    ExpressionActionsPtr actions;
    Names required_outputs;

    ExpressionActionsHolder(const NamesAndTypesList & input_columns_, const Settings & settings_)
        : actions(std::make_shared<ExpressionActions>(input_columns_, settings_))
    {}

    template<typename T>
    void setRequiredOutputs(T && required_outputs_)
    {
        required_outputs = std::forward<T>(required_outputs_);
    }

    void appendRequiredOutput(const String & required_output)
    {
        required_outputs.push_back(required_output);
    }

    ExpressionActionsPtr & getActions()
    {
        return actions;
    }

    ExpressionActionsPtr toActions()
    {
        assert(!required_outputs.empty());
        actions->finalize(required_outputs);
        return std::move(actions);
    }
};
} // namespace DB
