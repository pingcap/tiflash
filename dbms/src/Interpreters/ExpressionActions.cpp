// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>

#include <optional>
#include <set>

namespace DB
{
namespace ErrorCodes
{
extern const int DUPLICATE_COLUMN;
extern const int UNKNOWN_IDENTIFIER;
extern const int UNKNOWN_ACTION;
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
extern const int TOO_MANY_TEMPORARY_COLUMNS;
extern const int TOO_MANY_TEMPORARY_NON_CONST_COLUMNS;
} // namespace ErrorCodes


Names ExpressionAction::getNeededColumns() const
{
    Names res = argument_names;

    for (const auto & column : projections)
        res.push_back(column.first);

    if (!source_name.empty())
        res.push_back(source_name);

    return res;
}


ExpressionAction ExpressionAction::applyFunction(const FunctionBuilderPtr & function_,
                                                 const std::vector<std::string> & argument_names_,
                                                 std::string result_name_,
                                                 const TiDB::TiDBCollatorPtr & collator_)
{
    if (result_name_.empty())
    {
        result_name_ = function_->getName() + "(";
        for (size_t i = 0; i < argument_names_.size(); ++i)
        {
            if (i)
                result_name_ += ", ";
            result_name_ += argument_names_[i];
        }
        result_name_ += ")";
    }

    ExpressionAction a;
    a.type = APPLY_FUNCTION;
    a.result_name = result_name_;
    a.function_builder = function_;
    a.argument_names = argument_names_;
    a.collator = collator_;
    return a;
}

ExpressionAction ExpressionAction::addColumn(const ColumnWithTypeAndName & added_column_)
{
    ExpressionAction a;
    a.type = ADD_COLUMN;
    a.result_name = added_column_.name;
    a.result_type = added_column_.type;
    a.added_column = added_column_.column;
    return a;
}

ExpressionAction ExpressionAction::removeColumn(const std::string & removed_name)
{
    ExpressionAction a;
    a.type = REMOVE_COLUMN;
    a.source_name = removed_name;
    return a;
}

ExpressionAction ExpressionAction::copyColumn(const std::string & from_name, const std::string & to_name)
{
    ExpressionAction a;
    a.type = COPY_COLUMN;
    a.source_name = from_name;
    a.result_name = to_name;
    return a;
}

ExpressionAction ExpressionAction::project(const NamesWithAliases & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projections = projected_columns_;
    return a;
}

ExpressionAction ExpressionAction::project(const Names & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projections.resize(projected_columns_.size());
    for (size_t i = 0; i < projected_columns_.size(); ++i)
        a.projections[i] = NameWithAlias(projected_columns_[i], "");
    return a;
}

ExpressionAction ExpressionAction::ordinaryJoin(std::shared_ptr<const Join> join_, const NamesAndTypesList & columns_added_by_join_)
{
    ExpressionAction a;
    a.type = JOIN;
    a.join = join_;
    a.columns_added_by_join = columns_added_by_join_;
    return a;
}


void ExpressionAction::prepare(Block & sample_block)
{
    /** Constant expressions should be evaluated, and put the result in sample_block.
      */

    switch (type)
    {
    case APPLY_FUNCTION:
    {
        if (sample_block.has(result_name))
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

        bool all_const = true;

        ColumnNumbers arguments(argument_names.size());
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            arguments[i] = sample_block.getPositionByName(argument_names[i]);
            ColumnPtr col = sample_block.safeGetByPosition(arguments[i]).column;
            if (!col || !col->isColumnConst())
                all_const = false;
        }

        /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
        if (all_const && function->isSuitableForConstantFolding())
        {
            size_t result_position = sample_block.columns();

            ColumnWithTypeAndName new_column;
            new_column.name = result_name;
            new_column.type = result_type;
            sample_block.insert(std::move(new_column));

            function->execute(sample_block, arguments, result_position);

            /// If the result is not a constant, just in case, we will consider the result as unknown.
            ColumnWithTypeAndName & col = sample_block.safeGetByPosition(result_position);
            if (!col.column->isColumnConst())
            {
                col.column = nullptr;
            }
            else
            {
                /// All constant (literal) columns in block are added with size 1.
                /// But if there was no columns in block before executing a function, the result has size 0.
                /// Change the size to 1.

                if (col.column->empty())
                    col.column = col.column->cloneResized(1);
            }
        }
        else
        {
            sample_block.insert({nullptr, result_type, result_name});
        }

        break;
    }

    case JOIN:
    {
        /// in case of coprocessor task, the join is always not null, but if the query comes from
        /// clickhouse client, the join maybe null, skip updating column type if join is null
        // todo find a new way to update the column type so the type can always be updated.
        if (join != nullptr && join->getKind() == ASTTableJoin::Kind::Right)
        {
            /// update the column type for left block
            std::unordered_set<String> keys;
            for (const auto & n : join->getLeftJoinKeys())
            {
                keys.insert(n);
            }
            for (const auto & p : sample_block.getColumnsWithTypeAndName())
            {
                if (keys.find(p.name) == keys.end() && !p.type->isNullable())
                {
                    /// for right join, if the column in sample_block is not join key, then
                    /// convert it's type to nullable
                    auto & column_with_name = sample_block.getByName(p.name);
                    column_with_name.type = makeNullable(column_with_name.type);
                    if (column_with_name.column != nullptr)
                        column_with_name.column = makeNullable(column_with_name.column);
                }
            }
        }

        for (const auto & col : columns_added_by_join)
            sample_block.insert(ColumnWithTypeAndName(nullptr, col.type, col.name));

        break;
    }

    case PROJECT:
    {
        Block new_block;

        for (auto & projection : projections)
        {
            const std::string & name = projection.first;
            const std::string & alias = projection.second;
            ColumnWithTypeAndName column = sample_block.getByName(name);
            if (!alias.empty())
                column.name = alias;
            new_block.insert(std::move(column));
        }

        sample_block.swap(new_block);
        break;
    }

    case REMOVE_COLUMN:
    {
        sample_block.erase(source_name);
        break;
    }

    case ADD_COLUMN:
    {
        if (sample_block.has(result_name))
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

        sample_block.insert(ColumnWithTypeAndName(added_column, result_type, result_name));
        break;
    }

    case COPY_COLUMN:
    {
        result_type = sample_block.getByName(source_name).type;
        sample_block.insert(ColumnWithTypeAndName(sample_block.getByName(source_name).column, result_type, result_name));
        break;
    }

    default:
        throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
    }
}


void ExpressionAction::execute(Block & block) const
{
    if (type == REMOVE_COLUMN || type == COPY_COLUMN)
        if (!block.has(source_name))
            throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    if (type == ADD_COLUMN || type == COPY_COLUMN || type == APPLY_FUNCTION)
        if (block.has(result_name))
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    switch (type)
    {
    case APPLY_FUNCTION:
    {
        ColumnNumbers arguments(argument_names.size());
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            if (!block.has(argument_names[i]))
                throw Exception("Not found column: '" + argument_names[i] + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            arguments[i] = block.getPositionByName(argument_names[i]);
        }

        size_t num_columns_without_result = block.columns();
        block.insert({nullptr, result_type, result_name});

        function->execute(block, arguments, num_columns_without_result);

        break;
    }

    //TODO: Clean up all Join logic in ExpressionAction
    case JOIN:
    {
        ProbeProcessInfo probe_process_info(0);
        probe_process_info.block = block;
        join->joinBlock(probe_process_info);
        break;
    }

    case PROJECT:
    {
        Block new_block;

        for (const auto & projection : projections)
        {
            const std::string & name = projection.first;
            const std::string & alias = projection.second;
            ColumnWithTypeAndName column = block.getByName(name);
            if (!alias.empty())
                column.name = alias;
            new_block.insert(std::move(column));
        }

        block.swap(new_block);

        break;
    }

    case REMOVE_COLUMN:
        block.erase(source_name);
        break;

    case ADD_COLUMN:
        block.insert({added_column->cloneResized(block.rows()), result_type, result_name});
        break;

    case COPY_COLUMN:
        block.insert({block.getByName(source_name).column, result_type, result_name});
        break;

    default:
        throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
    }
}


void ExpressionAction::executeOnTotals(Block & block) const
{
    if (type != JOIN)
        execute(block);
    else
        join->joinTotals(block);
}

String ExpressionAction::toString() const
{
    std::stringstream ss;
    switch (type)
    {
    case ADD_COLUMN:
        ss << "ADD " << result_name << " "
           << (result_type ? result_type->getName() : "(no type)") << " "
           << (added_column ? added_column->getName() : "(no column)");
        break;

    case REMOVE_COLUMN:
        ss << "REMOVE " << source_name;
        break;

    case COPY_COLUMN:
        ss << "COPY " << result_name << " = " << source_name;
        break;

    case APPLY_FUNCTION:
        ss << "FUNCTION " << result_name << " "
           << (result_type ? result_type->getName() : "(no type)") << " = "
           << (function ? function->getName() : "(no function)") << "(";
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            if (i)
                ss << ", ";
            ss << argument_names[i];
        }
        ss << ")";
        break;

    case JOIN:
        ss << "JOIN ";
        for (auto it = columns_added_by_join.begin(); it != columns_added_by_join.end(); ++it)
        {
            if (it != columns_added_by_join.begin())
                ss << ", ";
            ss << it->name;
        }
        break;

    case PROJECT:
        ss << "PROJECT ";
        for (size_t i = 0; i < projections.size(); ++i)
        {
            if (i)
                ss << ", ";
            ss << projections[i].first;
            if (!projections[i].second.empty() && projections[i].second != projections[i].first)
                ss << " AS " << projections[i].second;
        }
        break;

    default:
        throw Exception("Unexpected Action type", ErrorCodes::LOGICAL_ERROR);
    }
    return ss.str();
}

void ExpressionActions::addInput(const ColumnWithTypeAndName & column)
{
    input_columns.emplace_back(column.name, column.type);
    sample_block.insert(column);
}

void ExpressionActions::addInput(const NameAndTypePair & column)
{
    addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));
}

void ExpressionActions::add(const ExpressionAction & action, Names & out_new_columns)
{
    addImpl(action, out_new_columns);
}

void ExpressionActions::add(const ExpressionAction & action)
{
    Names new_names;
    addImpl(action, new_names);
}

void ExpressionActions::addImpl(ExpressionAction action, Names & new_names)
{
    if (sample_block.has(action.result_name))
        return;

    if (!action.result_name.empty())
        new_names.push_back(action.result_name);

    if (action.type == ExpressionAction::APPLY_FUNCTION)
    {
        ColumnsWithTypeAndName arguments(action.argument_names.size());
        for (size_t i = 0; i < action.argument_names.size(); ++i)
        {
            if (!sample_block.has(action.argument_names[i]))
                throw Exception("Unknown identifier: '" + action.argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
            arguments[i] = sample_block.getByName(action.argument_names[i]);
        }

        action.function = action.function_builder->build(arguments, action.collator);
        action.result_type = action.function->getReturnType();
    }

    action.prepare(sample_block);
    actions.push_back(action);
}

void ExpressionActions::prependProjectInput()
{
    actions.insert(actions.begin(), ExpressionAction::project(getRequiredColumns()));
}

void ExpressionActions::execute(Block & block) const
{
    for (const auto & action : actions)
        action.execute(block);
}

void ExpressionActions::executeOnTotals(Block & block) const
{
    /// If there is `totals` in the subquery for JOIN, but we do not have totals, then take the block with the default values instead of `totals`.
    if (!block)
    {
        bool has_totals_in_join = false;
        for (const auto & action : actions)
        {
            if (action.join && action.join->hasTotals())
            {
                has_totals_in_join = true;
                break;
            }
        }

        if (has_totals_in_join)
        {
            for (const auto & name_and_type : input_columns)
            {
                auto column = name_and_type.type->createColumn();
                column->insertDefault();
                block.insert(ColumnWithTypeAndName(std::move(column), name_and_type.type, name_and_type.name));
            }
        }
        else
            return; /// There's nothing to JOIN.
    }

    for (const auto & action : actions)
        action.executeOnTotals(block);
}

std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
    std::optional<size_t> min_size;
    String res;

    for (const auto & column : columns)
    {
        /// @todo resolve evil constant
        size_t size = column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100;

        if (!min_size || size < *min_size)
        {
            min_size = size;
            res = column.name;
        }
    }

    if (!min_size)
        throw Exception("No available columns", ErrorCodes::LOGICAL_ERROR);

    return res;
}

void ExpressionActions::finalize(const Names & output_columns)
{
    NameSet final_columns;
    for (const auto & name : output_columns)
    {
        if (!sample_block.has(name))
            throw Exception("Unknown column: " + name + ", there are only columns "
                                + sample_block.dumpNames(),
                            ErrorCodes::UNKNOWN_IDENTIFIER);
        final_columns.insert(name);
    }

    /// Which columns are needed to perform actions from the current to the last.
    NameSet needed_columns = final_columns;
    /// Which columns nobody will touch from the current action to the last.
    NameSet unmodified_columns;

    {
        NamesAndTypesList sample_columns = sample_block.getNamesAndTypesList();
        for (auto & sample_column : sample_columns)
            unmodified_columns.insert(sample_column.name);
    }

    /// Let's go from the end and maintain set of required columns at this stage.
    /// We will throw out unnecessary actions, although usually they are absent by construction.
    for (int i = static_cast<int>(actions.size()) - 1; i >= 0; --i)
    {
        ExpressionAction & action = actions[i];
        Names in = action.getNeededColumns();

        if (action.type == ExpressionAction::PROJECT)
        {
            needed_columns = NameSet(in.begin(), in.end());
            unmodified_columns.clear();
        }
        else
        {
            std::string out = action.result_name;
            if (!out.empty())
            {
                /// If the result is not used and there are no side effects, throw out the action.
                if (!needed_columns.count(out) && (action.type == ExpressionAction::APPLY_FUNCTION || action.type == ExpressionAction::ADD_COLUMN || action.type == ExpressionAction::COPY_COLUMN))
                {
                    actions.erase(actions.begin() + i);

                    if (unmodified_columns.count(out))
                    {
                        sample_block.erase(out);
                        unmodified_columns.erase(out);
                    }

                    continue;
                }

                unmodified_columns.erase(out);
                needed_columns.erase(out);

                /** If the function is a constant expression, then replace the action by adding a column-constant - result.
                  * That is, we perform constant folding.
                  */
                if (action.type == ExpressionAction::APPLY_FUNCTION && sample_block.has(out))
                {
                    auto & result = sample_block.getByName(out);
                    if (result.column)
                    {
                        action.type = ExpressionAction::ADD_COLUMN;
                        action.result_type = result.type;
                        action.added_column = result.column;
                        action.function_builder = nullptr;
                        action.function = nullptr;
                        action.argument_names.clear();
                        in.clear();
                    }
                }
            }

            needed_columns.insert(in.begin(), in.end());
        }
    }

    /// We will not throw out all the input columns, so as not to lose the number of rows in the block.
    if (needed_columns.empty() && !input_columns.empty())
        needed_columns.insert(getSmallestColumn(input_columns));

    /// We will not leave the block empty so as not to lose the number of rows in it.
    if (final_columns.empty() && !input_columns.empty())
        final_columns.insert(getSmallestColumn(input_columns));

    for (auto it = input_columns.begin(); it != input_columns.end();)
    {
        auto it0 = it;
        ++it;
        if (!needed_columns.count(it0->name))
        {
            if (unmodified_columns.count(it0->name))
                sample_block.erase(it0->name);
            input_columns.erase(it0);
        }
    }

    /*    std::cerr << "\n";
    for (const auto & action : actions)
        std::cerr << action.toString() << "\n";
    std::cerr << "\n";*/

    /// Deletes unnecessary temporary columns.

    /// If the column after performing the function `refcount = 0`, it can be deleted.
    std::map<String, int> columns_refcount;

    for (const auto & name : final_columns)
        ++columns_refcount[name];

    for (const auto & action : actions)
    {
        if (!action.source_name.empty())
            ++columns_refcount[action.source_name];

        for (const auto & name : action.argument_names)
            ++columns_refcount[name];

        for (const auto & name_alias : action.projections)
            ++columns_refcount[name_alias.first];
    }

    Actions new_actions;
    new_actions.reserve(actions.size());

    for (const auto & action : actions)
    {
        new_actions.push_back(action);

        auto process = [&](const String & name) {
            auto refcount = --columns_refcount[name];
            if (refcount <= 0)
            {
                new_actions.push_back(ExpressionAction::removeColumn(name));
                if (sample_block.has(name))
                    sample_block.erase(name);
            }
        };

        if (!action.source_name.empty())
            process(action.source_name);

        for (const auto & name : action.argument_names)
            process(name);

        /// For `projection`, there is no reduction in `refcount`, because the `project` action replaces the names of the columns, in effect, already deleting them under the old names.
    }

    actions.swap(new_actions);
}


std::string ExpressionActions::dumpActions() const
{
    std::stringstream ss;

    ss << "input:\n";
    for (const auto & input_column : input_columns)
        ss << input_column.name << " " << input_column.type->getName() << "\n";

    ss << "\nactions:\n";
    for (const auto & action : actions)
        ss << action.toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (const auto & output_column : output_columns)
        ss << output_column.name << " " << output_column.type->getName() << "\n";

    return ss.str();
}

BlockInputStreamPtr ExpressionActions::createStreamWithNonJoinedDataIfFullOrRightJoin(const Block & source_header, size_t index, size_t step, size_t max_block_size) const
{
    for (const auto & action : actions)
        if (action.join && (action.join->needReturnNonJoinedData()))
            return action.join->createStreamWithNonJoinedRows(source_header, index, step, max_block_size);

    return {};
}

void ExpressionActionsChain::addStep()
{
    if (steps.empty())
        throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumnsWithTypeAndName();
    steps.push_back(Step(std::make_shared<ExpressionActions>(columns)));
}

void ExpressionActionsChain::finalize()
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
}

std::string ExpressionActionsChain::dumpChain()
{
    std::stringstream ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "required output:\n";
        for (const std::string & name : steps[i].required_output)
            ss << name << "\n";
        ss << "\n"
           << steps[i].actions->dumpActions() << "\n";
    }

    return ss.str();
}

} // namespace DB
