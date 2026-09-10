// Copyright 2026 PingCAP, Inc.
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

#include <Columns/ColumnFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <TestUtils/FunctionTestUtils.h>

#include <algorithm>

namespace DB::tests
{
class ShortCircuit : public FunctionTest
{
protected:
    void add(ExpressionActions & actions, const String & function, const Names & arguments, const String & result)
    {
        actions.add(
            ExpressionAction::applyFunction(FunctionFactory::instance().get(function, *context), arguments, result));
    }

    size_t lazyCount(const ExpressionActions & actions)
    {
        return std::count_if(actions.getActions().begin(), actions.getActions().end(), [](const auto & action) {
            return action.is_lazy_executed;
        });
    }
};

TEST_F(ShortCircuit, AndOrDivision)
{
    for (const String name : {"and", "or", "two_value_and"})
    {
        const bool is_or = name == "or";
        Block input({
            createColumn<Int64>({0, 2, 0, 4}, "denominator"),
            createConstColumn<Int64>(4, 12, "numerator"),
            createConstColumn<Int64>(4, 0, "zero"),
            createConstColumn<Int64>(4, 5, "five"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, is_or ? "equals" : "notEquals", {"denominator", "zero"}, "guard");
        add(actions, "intDiv", {"numerator", "denominator"}, "division");
        add(actions, "greater", {"division", "five"}, "comparison");
        add(actions, name, {"guard", "comparison"}, "result");
        actions.finalize({"result"});
        ASSERT_EQ(lazyCount(actions), 2);

        // The same immutable plan and captures can be used for multiple blocks.
        for (size_t run = 0; run < 2; ++run)
        {
            Block block = input;
            actions.execute(block);
            ASSERT_COLUMN_EQ(createColumn<UInt8>({is_or, 1, is_or, 0}), block.getByName("result"));
            for (const auto & column : block)
                ASSERT_EQ(checkAndGetShortCircuitArgument(column.column), nullptr);
        }
        Block empty = input.cloneEmpty();
        actions.execute(empty);
        ASSERT_EQ(empty.getByName("result").column->size(), 0);
    }
}

TEST_F(ShortCircuit, RequiredDivisionStillThrows)
{
    Block input({createColumn<Int64>({0, 1}, "denominator"), createConstColumn<Int64>(2, 1, "one")});
    for (const Names & arguments : {Names{"one", "division"}, Names{"division", "one"}})
    {
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"one", "denominator"}, "division");
        add(actions, "and", arguments, "result");
        actions.finalize({"result"});
        Block block = input;
        ASSERT_THROW(actions.execute(block), Exception);
    }
}

TEST_F(ShortCircuit, CastPreservesDeferredSubtree)
{
    for (const String name : {"CAST", "tidb_cast"})
    {
        Block input({
            createColumn<Int64>({0, 2, 0, 4}, "denominator"),
            createColumn<UInt8>({0, 1, 0, 1}, "guard"),
            createConstColumn<Int64>(4, 12, "numerator"),
            createConstColumn<String>(4, "Float64", "type"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"numerator", "denominator"}, "division");
        add(actions, name, {"division", "type"}, "cast");
        add(actions, "and", {"guard", "cast"}, "result");
        actions.finalize({"result"});
        ASSERT_EQ(lazyCount(actions), 2);
        actions.execute(input);
        ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 0, 1}), input.getByName("result"));
    }
}

TEST_F(ShortCircuit, NullableTruthTables)
try
{
    Block input({
        createColumn<Nullable<Float64>>({0, 0, 0, -0.5, -0.5, -0.5, {}, {}, {}}, "left"),
        createColumn<Nullable<Float64>>({0, 0.5, {}, 0, 0.5, {}, 0, 0.5, {}}, "right"),
        createConstColumn<Float64>(9, 0, "zero"),
    });
    for (const String name : {"and", "or", "two_value_and"})
    {
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "plus", {"right", "zero"}, "deferred_right");
        add(actions, name, {"left", "deferred_right"}, "result");
        actions.finalize({"result"});
        ASSERT_EQ(lazyCount(actions), 1);
        Block block = input;
        actions.execute(block);
        ASSERT_COLUMN_EQ(
            executeFunction(name, {input.getByName("left"), input.getByName("right")}, nullptr, true),
            block.getByName("result"));
    }
}
CATCH

TEST_F(ShortCircuit, NullIsNotFalseForThreeValuedAnd)
{
    Block input({
        createColumn<Nullable<UInt8>>({{}}, "null"),
        createColumn<Int64>({0}, "zero"),
        createConstColumn<Int64>(1, 1, "one"),
    });
    for (const String name : {"and", "or", "two_value_and"})
    {
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"one", "zero"}, "division");
        add(actions, name, {"null", "division"}, "result");
        actions.finalize({"result"});
        Block block = input;
        if (name == "two_value_and")
        {
            actions.execute(block);
            ASSERT_COLUMN_EQ(createColumn<UInt8>({0}), block.getByName("result"));
        }
        else
            ASSERT_THROW(actions.execute(block), Exception);
    }
}

TEST_F(ShortCircuit, VariadicWithOnlyNullAndConstants)
try
{
    auto null_column = createOnlyNullColumn(4);
    null_column.name = "null";
    for (const UInt8 constant : {0, 1})
    {
        Block input({
            createColumn<UInt8>({0, 1, 0, 1}, "first"),
            createColumn<Nullable<Int64>>({{}, 0, 2, {}}, "last"),
            createConstColumn<UInt8>(4, constant, "constant"),
            createConstColumn<Int64>(4, 0, "zero"),
            null_column,
        });
        for (const String name : {"and", "or", "two_value_and"})
        {
            ExpressionActions actions(input.getColumnsWithTypeAndName());
            add(actions, "plus", {"last", "zero"}, "deferred_last");
            add(actions, name, {"first", "null", "constant", "deferred_last"}, "result");
            actions.finalize({"result"});
            Block block = input;
            actions.execute(block);
            auto expected = executeFunction(
                name,
                {input.getByName("first"), null_column, input.getByName("constant"), input.getByName("last")},
                nullptr,
                true);
            if (auto materialized = expected.column->convertToFullColumnIfConst())
                expected.column = std::move(materialized);
            ASSERT_COLUMN_EQ(expected, block.getByName("result"));
        }
    }
}
CATCH

TEST_F(ShortCircuit, AllSelectedAndAllSkipped)
{
    for (const UInt8 selected : {0, 1})
    {
        Block input({
            createConstColumn<UInt8>(3, selected, "guard"),
            createColumn<Int64>({selected, selected, selected}, "denominator"),
            createConstColumn<Int64>(3, 1, "one"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"one", "denominator"}, "division");
        add(actions, "and", {"guard", "division"}, "result");
        actions.finalize({"result"});
        actions.execute(input);
        ASSERT_COLUMN_EQ(createColumn<UInt8>({selected, selected, selected}), input.getByName("result"));
    }
}

TEST_F(ShortCircuit, FailedConstantFoldingIsDeferred)
{
    for (const UInt8 selected : {0, 1})
    {
        Block input({
            createColumn<UInt8>({selected, selected}, "guard"),
            createConstColumn<Int64>(2, 0, "zero"),
            createConstColumn<Int64>(2, 1, "one"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        ASSERT_NO_THROW(add(actions, "intDiv", {"one", "zero"}, "division"));
        add(actions, "and", {"guard", "division"}, "result");
        actions.finalize({"result"});
        if (selected)
            ASSERT_THROW(actions.execute(input), Exception);
        else
        {
            actions.execute(input);
            ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0}), input.getByName("result"));
        }
    }
}

TEST_F(ShortCircuit, SharedExpressionUsesIndependentMasks)
{
    Block input({
        createColumn<Int64>({0, 2, 3}, "denominator"),
        createColumn<UInt8>({0, 1, 0}, "left_guard"),
        createColumn<UInt8>({0, 0, 1}, "right_guard"),
        createConstColumn<Int64>(3, 6, "six"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "intDiv", {"six", "denominator"}, "shared");
    add(actions, "and", {"left_guard", "shared"}, "left_result");
    add(actions, "and", {"right_guard", "shared"}, "right_result");
    actions.finalize({"left_result", "right_result"});
    ASSERT_EQ(lazyCount(actions), 1);
    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 0}), input.getByName("left_result"));
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 1}), input.getByName("right_result"));
}

TEST_F(ShortCircuit, SharedEagerConsumerAndOutputPreventDeferral)
{
    for (const bool shared_output : {false, true})
    {
        Block input({
            createColumn<Int64>({0, 1}, "denominator"),
            createConstColumn<Int64>(2, 1, "one"),
            createConstColumn<Int64>(2, 0, "zero"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"one", "denominator"}, "division");
        add(actions, "and", {"zero", "division"}, "guarded");
        Names outputs{"guarded"};
        if (shared_output)
            outputs.push_back("division");
        else
        {
            add(actions, "plus", {"division", "one"}, "unguarded");
            outputs.push_back("unguarded");
        }
        actions.finalize(outputs);
        ASSERT_EQ(lazyCount(actions), 0);
        ASSERT_THROW(actions.execute(input), Exception);
    }
}

TEST_F(ShortCircuit, NestedLogicalFunctionsAndAliases)
{
    Block input({
        createColumn<Int64>({0, 2, 3, 0}, "denominator"),
        createColumn<UInt8>({0, 1, 1, 0}, "outer_guard"),
        createColumn<UInt8>({0, 0, 1, 0}, "inner_guard"),
        createConstColumn<Int64>(4, 6, "six"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "intDiv", {"six", "denominator"}, "division");
    actions.add(ExpressionAction::copyColumn("division", "alias"));
    add(actions, "or", {"inner_guard", "alias"}, "nested");
    add(actions, "and", {"outer_guard", "nested"}, "result");
    actions.finalize({"result"});
    ASSERT_EQ(lazyCount(actions), 2);
    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 1, 0}), input.getByName("result"));
}

TEST_F(ShortCircuit, TypeErrorsRemainStrict)
{
    Block input({createColumn<String>({"text"}, "text"), createConstColumn<Int64>(1, 0, "zero")});
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    ASSERT_THROW(add(actions, "and", {"zero", "text"}, "result"), Exception);
}

TEST_F(ShortCircuit, JsonSubtreeIsSkippedIncludingDynamicPath)
{
    for (const bool invalid_selected_path : {false, true})
    {
        Block input({
            createColumn<String>({"invalid json", R"({"a": 1})", "", R"({"b": 2})"}, "document"),
            createColumn<String>({"invalid path", invalid_selected_path ? "invalid path" : "$.a", "[", "$.a"}, "path"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "json_valid_string", {"document"}, "guard");
        add(actions, "cast_string_as_json", {"document"}, "json");
        add(actions, "json_extract", {"json", "path"}, "extracted");
        add(actions, "isNotNull", {"extracted"}, "present");
        add(actions, "and", {"guard", "present"}, "result");
        // This is a projected expression, not an analyzer filter with a JSON-specific guard.
        actions.finalize({"result"});
        ASSERT_EQ(lazyCount(actions), 3);
        if (invalid_selected_path)
            ASSERT_THROW(actions.execute(input), Exception);
        else
        {
            actions.execute(input);
            ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 0, 0}), input.getByName("result"));
        }
    }
}

TEST_F(ShortCircuit, ConstantInvalidJsonIsSkipped)
{
    Block input({
        createConstColumn<String>(2, "invalid json", "document"),
        createConstColumn<UInt8>(2, 0, "guard"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "cast_string_as_json", {"document"}, "json");
    add(actions, "isNotNull", {"json"}, "present");
    add(actions, "and", {"guard", "present"}, "result");
    actions.finalize({"result"});
    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0}), input.getByName("result"));
}

TEST_F(ShortCircuit, RowDependentFunctionRemainsEager)
{
    Block input({
        createColumn<Int64>({10, 20, 30}, "value"),
        createColumn<UInt8>({0, 1, 1}, "guard"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "runningDifference", {"value"}, "difference");
    add(actions, "and", {"guard", "difference"}, "result");
    actions.finalize({"result"});
    ASSERT_EQ(lazyCount(actions), 0);
    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 1}), input.getByName("result"));
}

TEST_F(ShortCircuit, AppendActionsAfterFinalize)
{
    Block input({
        createColumn<Int64>({0, 2}, "denominator"),
        createColumn<UInt8>({0, 1}, "guard"),
        createConstColumn<Int64>(2, 6, "six"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "intDiv", {"six", "denominator"}, "division");
    add(actions, "and", {"guard", "division"}, "result");
    actions.finalize({"result"});
    add(actions, "not", {"result"}, "negated");
    actions.add(ExpressionAction::project(NamesWithAliases{{"negated", "output"}}));
    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 0}), input.getByName("output"));
}
} // namespace DB::tests
