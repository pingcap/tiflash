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

    static size_t lazyCount(const ExpressionActions & actions)
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

TEST_F(ShortCircuit, UnrelatedHelperColumnHasDifferentSize)
{
    for (const auto & name : {"and", "or", "two_value_and"})
    {
        const bool is_or = String(name) == "or";
        Block input({
            createColumn<Int8>({}, "match_helper"),
            createColumn<UInt8>({is_or, !is_or, !is_or}, "guard"),
            createColumn<Int64>({0, 2, 3}, "denominator"),
            createConstColumn<Int64>(3, 6, "six"),
        });
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"six", "denominator"}, "division");
        add(actions, name, {"guard", "division"}, "result");
        actions.finalize({"match_helper", "result"});
        ASSERT_EQ(lazyCount(actions), 1);
        actions.execute(input);
        ASSERT_COLUMN_EQ(createColumn<UInt8>({is_or, 1, 1}), input.getByName("result"));
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
        createColumn<Nullable<Int64>>({0, 0, 0, -1, -1, -1, {}, {}, {}}, "left"),
        createColumn<Nullable<Int64>>({0, 1, {}, 0, 1, {}, 0, 1, {}}, "right"),
        createConstColumn<Int64>(9, 1, "one"),
    });
    for (const String name : {"and", "or", "two_value_and"})
    {
        ExpressionActions actions(input.getColumnsWithTypeAndName());
        add(actions, "intDiv", {"right", "one"}, "deferred_right");
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
            createConstColumn<Int64>(4, 1, "one"),
            null_column,
        });
        for (const String name : {"and", "or", "two_value_and"})
        {
            ExpressionActions actions(input.getColumnsWithTypeAndName());
            add(actions, "intDiv", {"last", "one"}, "deferred_last");
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

TEST_F(ShortCircuit, DeeplyNestedLogicalFunctions)
{
    // outer OR (middle AND (inner OR intDiv(6, denominator)))
    // Three levels of mixed nesting: the deferred argument of the outer `or` is
    // an `and` subtree whose deferred argument is an `or` subtree. The external
    // mask is composed with each level's guard row-wise (no filter/expand
    // copies in between), and every division-by-zero row must be protected by
    // whichever guard saturates first.
    Block input({
        createColumn<Int64>({0, 0, 2, 5, 3, 0, 4, 0}, "denominator"),
        createColumn<UInt8>({1, 0, 0, 0, 0, 0, 0, 0}, "outer_guard"),
        createColumn<UInt8>({1, 0, 1, 1, 1, 0, 1, 1}, "middle_guard"),
        createColumn<UInt8>({1, 1, 1, 0, 1, 1, 0, 1}, "inner_guard"),
        createConstColumn<Int64>(8, 6, "six"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "intDiv", {"six", "denominator"}, "division");
    add(actions, "or", {"inner_guard", "division"}, "inner_or");
    add(actions, "and", {"middle_guard", "inner_or"}, "middle_and");
    add(actions, "or", {"outer_guard", "middle_and"}, "result");
    actions.finalize({"result"});
    // intDiv, the inner or and the middle and are lazy; the top-level or is eager.
    ASSERT_EQ(lazyCount(actions), 3);
    actions.execute(input);
    // Row 0 is protected by outer saturation, rows 1/5 by middle saturation,
    // row 7 by inner saturation; row 3 really evaluates 6 DIV 5.
    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 0, 1, 1, 1, 0, 1, 1}), input.getByName("result"));
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

TEST_F(ShortCircuit, FunctionChainMaskPassthrough)
{
    /// json_contains_path(cast_string_as_json(document), 'one', path) is a
    /// deferred function chain: reduceWithMask materializes the nested cast
    /// in place by passing the consumer's mask down (both levels are guarded
    /// by the same guard column), then the head's native executeWithMask
    /// override skips the masked-off rows without any filter/expand copies.
    /// Rows where the guard is false skip parsing the invalid JSON documents.
    Block input({
        createColumn<String>({"invalid json", R"({"a": 1})", R"({"b": 2})", "bad"}, "document"),
        createConstColumn<String>(4, "one", "mode"),
        createConstColumn<String>(4, "$.a", "path"),
        createColumn<UInt8>({0, 1, 1, 0}, "guard"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "cast_string_as_json", {"document"}, "json");
    add(actions, "json_contains_path", {"json", "mode", "path"}, "contains");
    add(actions, "and", {"guard", "contains"}, "result");
    actions.finalize({"result"});
    ASSERT_EQ(lazyCount(actions), 2);

    actions.execute(input);
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 1, 0, 0}), input.getByName("result"));
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

TEST_F(ShortCircuit, JsonUnquoteSkipsMaskedRows)
{
    /// json_unquote is a canThrow function with its own executeWithMask
    /// override, so the deferred chain equals(json_unquote(document), 'a')
    /// materializes in place: masked-off rows keep a null placeholder and are
    /// never unquoted. json_unquote takes raw JSON text (not JsonBinary), so
    /// the document column feeds it directly.
    Block input({
        createColumn<String>({R"("a")", R"("a")"}, "document"),
        createColumn<UInt8>({0, 1}, "guard"),
        createConstColumn<String>(2, "a", "expected"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "json_unquote", {"document"}, "unquoted");
    add(actions, "equals", {"unquoted", "expected"}, "equal");
    add(actions, "and", {"guard", "equal"}, "result");
    actions.finalize({"result"});
    actions.execute(input);
    /// json_unquote returns Nullable(String), so and() propagates Nullable
    ASSERT_COLUMN_EQ(createColumn<Nullable<UInt8>>({0, 1}), input.getByName("result"));
}

TEST_F(ShortCircuit, FullEvaluationReadsPlaceholdersSafely)
{
    /// cast_json_as_string has no executeWithMask override, so the default
    /// implementation evaluates it on all rows - including the masked-off
    /// rows, whose cast_string_as_json input is a placeholder. That
    /// placeholder must be a readable "null" JSON document: if it were an
    /// empty string, the JSON parser inside cast_json_as_string would read
    /// past the column data on the masked-off rows. The guard keeps the
    /// garbage results out of the final output.
    Block input({
        createColumn<String>({"invalid json", R"({"a": 1})", R"({"b": 2})", "bad"}, "document"),
        createColumn<UInt8>({0, 1, 1, 0}, "guard"),
        createConstColumn<UInt64>(4, 0, "zero"),
    });
    ExpressionActions actions(input.getColumnsWithTypeAndName());
    add(actions, "cast_string_as_json", {"document"}, "json");
    add(actions, "cast_json_as_string", {"json"}, "text");
    add(actions, "length", {"text"}, "size");
    add(actions, "greater", {"size", "zero"}, "nonempty");
    add(actions, "and", {"guard", "nonempty"}, "result");
    actions.finalize({"result"});
    actions.execute(input);
    /// cast_string_as_json marks masked-off rows as NULL, and the nullability
    /// propagates through the chain into and()'s result
    ASSERT_COLUMN_EQ(createColumn<Nullable<UInt8>>({0, 1, 1, 0}), input.getByName("result"));
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
