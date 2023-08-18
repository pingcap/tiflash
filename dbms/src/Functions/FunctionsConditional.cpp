// Copyright 2023 PingCAP, Inc.
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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionsTransform.h>
#include <Interpreters/castColumn.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}

void registerFunctionsConditional(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIf>();
    factory.registerFunction<FunctionMultiIf>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpression");
}


/// Implementation of FunctionMultiIf.

FunctionPtr FunctionMultiIf::create(const Context & context)
{
    return std::make_shared<FunctionMultiIf>(context);
}

String FunctionMultiIf::getName() const
{
    return name;
}


void FunctionMultiIf::executeImpl(Block & block, const ColumnNumbers & args, size_t result) const
{
    /** We will gather values from columns in branches to result column,
      *  depending on values of conditions.
      */
    struct Instruction
    {
        const IColumn * condition = nullptr;
        const IColumn * source = nullptr;

        bool condition_always_true = false;
        bool condition_is_nullable = false;
        bool source_is_constant = false;
    };

    std::vector<Instruction> instructions;
    instructions.reserve(args.size() / 2 + 1);

    Columns converted_columns_holder;
    converted_columns_holder.reserve(instructions.size());

    const DataTypePtr & return_type = block.getByPosition(result).type;

    for (size_t i = 0; i < args.size(); i += 2)
    {
        Instruction instruction;
        size_t source_idx = i + 1;

        if (source_idx == args.size())
        {
            /// The last, "else" branch can be treated as a branch with always true condition "else if (true)".
            --source_idx;
            instruction.condition_always_true = true;
        }
        else
        {
            const ColumnWithTypeAndName & cond_col = block.getByPosition(args[i]);

            /// We skip branches that are always false.
            /// If we encounter a branch that is always true, we can finish.

            if (cond_col.column->onlyNull())
                continue;

            if (cond_col.column->isColumnConst())
            {
                Field value = typeid_cast<const ColumnConst &>(*cond_col.column).getField();
                if (value.isNull())
                    continue;
                if (value.get<UInt64>() == 0)
                    continue;
                instruction.condition_always_true = true;
            }
            else
            {
                if (cond_col.column->isColumnNullable())
                    instruction.condition_is_nullable = true;

                instruction.condition = cond_col.column.get();
            }
        }

        const ColumnWithTypeAndName & source_col = block.getByPosition(args[source_idx]);
        if (source_col.type->equals(*return_type))
        {
            instruction.source = source_col.column.get();
        }
        else
        {
            /// Cast all columns to result type.
            converted_columns_holder.emplace_back(castColumn(source_col, return_type, context));
            instruction.source = converted_columns_holder.back().get();
        }

        if (instruction.source && instruction.source->isColumnConst())
            instruction.source_is_constant = true;

        instructions.emplace_back(std::move(instruction));

        if (instructions.back().condition_always_true)
            break;
    }

    size_t rows = block.rows();
    MutableColumnPtr res = return_type->createColumn();

    for (size_t i = 0; i < rows; ++i)
    {
        for (const auto & instruction : instructions)
        {
            bool insert = false;

            if (instruction.condition_always_true)
                insert = true;
            else if (!instruction.condition_is_nullable)
                insert = static_cast<const ColumnUInt8 &>(*instruction.condition).getData()[i];
            else
            {
                const auto & condition_nullable = static_cast<const ColumnNullable &>(*instruction.condition);
                const auto & condition_nested = static_cast<const ColumnUInt8 &>(condition_nullable.getNestedColumn());
                const NullMap & condition_null_map = condition_nullable.getNullMapData();

                insert = !condition_null_map[i] && condition_nested.getData()[i];
            }

            if (insert)
            {
                if (!instruction.source_is_constant)
                    res->insertFrom(*instruction.source, i);
                else
                    res->insertFrom(static_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);

                break;
            }
        }
    }

    block.getByPosition(result).column = std::move(res);
}

DataTypePtr FunctionMultiIf::getReturnTypeImpl(const DataTypes & args) const
{
    /// Arguments are the following: cond1, then1, cond2, then2, ... condN, thenN, else.

    auto for_conditions = [&args](auto && f) {
        size_t conditions_end = args.size() - 1;
        for (size_t i = 0; i < conditions_end; i += 2)
            f(args[i]);
    };

    auto for_branches = [&args](auto && f) {
        size_t branches_end = args.size();
        for (size_t i = 1; i < branches_end; i += 2)
            f(args[i]);
        f(args.back());
    };

    if (!(args.size() >= 3 && args.size() % 2 == 1))
        throw Exception{
            "Invalid number of arguments for function " + getName(),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    for_conditions([&](const DataTypePtr & arg) {
        const IDataType * nested_type;
        /// Conditions must be UInt8, Nullable(UInt8) or Null.
        if (arg->isNullable())
        {
            if (arg->onlyNull())
                return;

            const auto & nullable_type = static_cast<const DataTypeNullable &>(*arg);
            nested_type = nullable_type.getNestedType().get();
        }
        else
        {
            nested_type = arg.get();
        }

        if (!checkDataType<DataTypeUInt8>(nested_type))
            throw Exception{
                "Illegal type " + arg->getName()
                    + " of argument (condition) "
                      "of function "
                    + getName() + ". Must be UInt8.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    });

    DataTypes types_of_branches;
    types_of_branches.reserve(args.size() / 2 + 1);

    for_branches([&](const DataTypePtr & arg) { types_of_branches.emplace_back(arg); });

    return getLeastSupertype(types_of_branches);
}
} // namespace DB
