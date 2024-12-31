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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/NestedUtils.h>
#include <Dictionaries/IDictionary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Join.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/formatAST.h>
#include <Poco/String.h>
#include <Poco/Util/Application.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageSet.h>

#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
extern const int UNKNOWN_IDENTIFIER;
extern const int CYCLIC_ALIASES;
extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
extern const int TOO_MANY_ROWS;
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
extern const int INCORRECT_ELEMENT_OF_SET;
extern const int ALIAS_REQUIRED;
extern const int EMPTY_NESTED_TABLE;
extern const int NOT_AN_AGGREGATE;
extern const int UNEXPECTED_EXPRESSION;
extern const int DUPLICATE_COLUMN;
extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
extern const int ILLEGAL_AGGREGATION;
extern const int SUPPORT_IS_DISABLED;
extern const int TOO_DEEP_AST;
extern const int TOO_BIG_AST;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes


/** Calls to these functions in the GROUP BY statement would be
  * replaced by their immediate argument.
  */
const std::unordered_set<String> injective_function_names{
    "negate",
    "bitNot",
    "reverse",
    "reverseUTF8",
    "toString",
    "toFixedString",
    "IPv4NumToString",
    "IPv4StringToNum",
    "hex",
    "unhex",
    "bitmaskToList",
    "bitmaskToArray",
    "tuple",
    "regionToName",
    "concatAssumeInjective",
};

const std::unordered_set<String> possibly_injective_function_names{
    "dictGetString",
    "dictGetUInt8",
    "dictGetUInt16",
    "dictGetUInt32",
    "dictGetUInt64",
    "dictGetInt8",
    "dictGetInt16",
    "dictGetInt32",
    "dictGetInt64",
    "dictGetFloat32",
    "dictGetFloat64",
    "dictGetDate",
    "dictGetDateTime"};

namespace
{
void removeDuplicateColumns(NamesAndTypesList & columns)
{
    std::set<String> names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            columns.erase(it++);
    }
}

} // namespace


ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & ast_,
    const Context & context_,
    const StoragePtr & storage_,
    const NamesAndTypesList & source_columns_,
    const Names & required_result_columns_,
    size_t subquery_depth_,
    bool do_global_,
    const SubqueriesForSets & subqueries_for_set_)
    : ast(ast_)
    , context(context_)
    , settings(context.getSettingsRef())
    , subquery_depth(subquery_depth_)
    , source_columns(source_columns_)
    , required_result_columns(required_result_columns_.begin(), required_result_columns_.end())
    , storage(storage_)
    , do_global(do_global_)
    , subqueries_for_sets(subqueries_for_set_)
{
    select_query = typeid_cast<ASTSelectQuery *>(ast.get());

    if (!storage && select_query)
    {
        auto select_database = select_query->database();
        auto select_table = select_query->table();

        if (select_table && !typeid_cast<const ASTSelectWithUnionQuery *>(select_table.get())
            && !typeid_cast<const ASTFunction *>(select_table.get()))
        {
            String database = select_database ? typeid_cast<const ASTIdentifier &>(*select_database).name : "";
            const String & table = typeid_cast<const ASTIdentifier &>(*select_table).name;
            storage = context.tryGetTable(database, table);
        }
    }

    if (storage && source_columns.empty())
        source_columns = storage->getColumns().getAllPhysical();
    else
        removeDuplicateColumns(source_columns);

    addAliasColumns();

    translateQualifiedNames();

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, settings).perform();

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    addASTAliases(ast);

    /// Common subexpression elimination. Rewrite rules.
    normalizeTree();

    /// Remove unneeded columns according to 'required_source_columns'.
    /// Leave all selected columns in case of DISTINCT;
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause();

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries();

    /// Optimize if with constant condition after constants was substituted instead of sclalar subqueries.
    optimizeIfWithConstantCondition();

    /// GROUP BY injective function elimination.
    optimizeGroupBy();

    /// Remove duplicate items from ORDER BY.
    optimizeOrderBy();

    // Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy();

    /// Delete the unnecessary from `source_columns` list. Create `unknown_required_source_columns`. Form `columns_added_by_join`.
    collectUsedColumns();

    /// external_tables, subqueries_for_sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables();

    /// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
    /// This analysis should be performed after processing global subqueries, because otherwise,
    /// if the aggregate function contains a global subquery, then `analyzeAggregation` method will save
    /// in `aggregate_descriptions` the information about the parameters of this aggregate function, among which
    /// global subquery. Then, when you call `initGlobalSubqueriesAndExternalTables` method, this
    /// the global subquery will be replaced with a temporary table, resulting in aggregate_descriptions
    /// will contain out-of-date information, which will lead to an error when the query is executed.
    analyzeAggregation();
}


void ExpressionAnalyzer::translateQualifiedNames()
{
    String database_name;
    String table_name;
    String alias;

    if (!select_query || !select_query->tables || select_query->tables->children.empty())
        return;

    auto & element = static_cast<ASTTablesInSelectQueryElement &>(*select_query->tables->children[0]);

    auto & table_expression = static_cast<ASTTableExpression &>(*element.table_expression);

    if (table_expression.database_and_table_name)
    {
        const auto & identifier = static_cast<const ASTIdentifier &>(*table_expression.database_and_table_name);

        alias = identifier.tryGetAlias();

        if (table_expression.database_and_table_name->children.empty())
        {
            database_name = context.getCurrentDatabase();
            table_name = identifier.name;
        }
        else
        {
            if (table_expression.database_and_table_name->children.size() != 2)
                throw Exception(
                    "Logical error: number of components in table expression not equal to two",
                    ErrorCodes::LOGICAL_ERROR);

            database_name = static_cast<const ASTIdentifier &>(*identifier.children[0]).name;
            table_name = static_cast<const ASTIdentifier &>(*identifier.children[1]).name;
        }
    }
    else if (table_expression.table_function)
    {
        alias = table_expression.table_function->tryGetAlias();
    }
    else if (table_expression.subquery)
    {
        alias = table_expression.subquery->tryGetAlias();
    }
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);

    translateQualifiedNamesImpl(ast, database_name, table_name, alias);
}


void ExpressionAnalyzer::translateQualifiedNamesImpl(
    ASTPtr & ast,
    const String & database_name,
    const String & table_name,
    const String & alias)
{
    if (auto * ident = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        if (ident->kind == ASTIdentifier::Column)
        {
            /// It is compound identifier
            if (!ast->children.empty())
            {
                size_t num_components = ast->children.size();
                size_t num_qualifiers_to_strip = 0;

                /// database.table.column
                if (num_components >= 3 && !database_name.empty()
                    && static_cast<const ASTIdentifier &>(*ast->children[0]).name == database_name
                    && static_cast<const ASTIdentifier &>(*ast->children[1]).name == table_name)
                {
                    num_qualifiers_to_strip = 2;
                }

                /// table.column or alias.column. If num_components > 2, it is like table.nested.column.
                if (num_components >= 2
                    && ((!table_name.empty()
                         && static_cast<const ASTIdentifier &>(*ast->children[0]).name == table_name)
                        || (!alias.empty() && static_cast<const ASTIdentifier &>(*ast->children[0]).name == alias)))
                {
                    num_qualifiers_to_strip = 1;
                }

                if (num_qualifiers_to_strip)
                {
                    /// plain column
                    if (num_components - num_qualifiers_to_strip == 1)
                    {
                        String node_alias = ast->tryGetAlias();
                        ast = ast->children.back();
                        static_cast<ASTIdentifier &>(*ast.get()).can_be_alias = false;
                        if (!node_alias.empty())
                            ast->setAlias(node_alias);
                    }
                    else
                    /// nested column
                    {
                        ident->children.erase(
                            ident->children.begin(),
                            ident->children.begin() + num_qualifiers_to_strip);
                        String new_name;
                        for (const auto & child : ident->children)
                        {
                            if (!new_name.empty())
                                new_name += '.';
                            new_name += static_cast<const ASTIdentifier &>(*child).name;
                        }
                        ident->can_be_alias = false;
                        ident->name = new_name;
                    }
                }
            }
        }
    }
    else if (typeid_cast<ASTQualifiedAsterisk *>(ast.get()))
    {
        if (ast->children.size() != 1)
            throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

        auto * ident = typeid_cast<ASTIdentifier *>(ast->children[0].get());
        if (!ident)
            throw Exception(
                "Logical error: qualified asterisk must have identifier as its child",
                ErrorCodes::LOGICAL_ERROR);

        size_t num_components = ident->children.size();
        if (num_components > 2)
            throw Exception(
                "Qualified asterisk cannot have more than two qualifiers",
                ErrorCodes::UNKNOWN_ELEMENT_IN_AST);

        /// database.table.*, table.* or alias.*
        if ((num_components == 2 && !database_name.empty()
             && static_cast<const ASTIdentifier &>(*ident->children[0]).name == database_name
             && static_cast<const ASTIdentifier &>(*ident->children[1]).name == table_name)
            || (num_components == 0
                && ((!table_name.empty() && ident->name == table_name) || (!alias.empty() && ident->name == alias))))
        {
            /// Replace to plain asterisk.
            ast = std::make_shared<ASTAsterisk>();
        }
    }
    else
    {
        for (auto & child : ast->children)
        {
            /// Do not go to FROM, JOIN, subqueries.
            if (!typeid_cast<const ASTTableExpression *>(child.get())
                && !typeid_cast<const ASTSelectWithUnionQuery *>(child.get()))
            {
                translateQualifiedNamesImpl(child, database_name, table_name, alias);
            }
        }
    }
}


void ExpressionAnalyzer::optimizeIfWithConstantCondition()
{
    optimizeIfWithConstantConditionImpl(ast, aliases);
}

bool ExpressionAnalyzer::tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value) const
{
    /// numeric constant in condition
    if (const auto * literal = typeid_cast<ASTLiteral *>(condition.get()))
    {
        if (literal->value.getType() == Field::Types::Int64 || literal->value.getType() == Field::Types::UInt64)
        {
            value = literal->value.get<Int64>();
            return true;
        }
    }

    /// cast of numeric constant in condition to UInt8
    if (const auto * function = typeid_cast<ASTFunction *>(condition.get()))
    {
        if (function->name == "CAST")
        {
            if (auto * expr_list = typeid_cast<ASTExpressionList *>(function->arguments.get()))
            {
                const ASTPtr & type_ast = expr_list->children.at(1);
                if (const auto * type_literal = typeid_cast<ASTLiteral *>(type_ast.get()))
                {
                    if (type_literal->value.getType() == Field::Types::String
                        && type_literal->value.get<std::string>() == "UInt8")
                        return tryExtractConstValueFromCondition(expr_list->children.at(0), value);
                }
            }
        }
    }

    return false;
}

void ExpressionAnalyzer::optimizeIfWithConstantConditionImpl(
    ASTPtr & current_ast,
    ExpressionAnalyzer::Aliases & aliases) const
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        auto * function_node = typeid_cast<ASTFunction *>(child.get());
        if (!function_node || function_node->name != "if")
        {
            optimizeIfWithConstantConditionImpl(child, aliases);
            continue;
        }

        optimizeIfWithConstantConditionImpl(function_node->arguments, aliases);
        auto * args = typeid_cast<ASTExpressionList *>(function_node->arguments.get());

        ASTPtr condition_expr = args->children.at(0);
        ASTPtr then_expr = args->children.at(1);
        ASTPtr else_expr = args->children.at(2);


        bool condition;
        if (tryExtractConstValueFromCondition(condition_expr, condition))
        {
            ASTPtr replace_ast = condition ? then_expr : else_expr;
            ASTPtr child_copy = child;
            String replace_alias = replace_ast->tryGetAlias();
            String if_alias = child->tryGetAlias();

            if (replace_alias.empty())
            {
                replace_ast->setAlias(if_alias);
                child = replace_ast;
            }
            else
            {
                /// Only copy of one node is required here.
                /// But IAST has only method for deep copy of subtree.
                /// This can be a reason of performance degradation in case of deep queries.
                ASTPtr replace_ast_deep_copy = replace_ast->clone();
                replace_ast_deep_copy->setAlias(if_alias);
                child = replace_ast_deep_copy;
            }

            if (!if_alias.empty())
            {
                auto alias_it = aliases.find(if_alias);
                if (alias_it != aliases.end() && alias_it->second.get() == child_copy.get())
                    alias_it->second = child;
            }
        }
    }
}

void ExpressionAnalyzer::analyzeAggregation()
{
    /** Find aggregation keys (aggregation_keys), information about aggregate functions (aggregate_descriptions),
     *  as well as a set of columns obtained after the aggregation, if any,
     *  or after all the actions that are usually performed before aggregation (aggregated_columns).
     *
     * Everything below (compiling temporary ExpressionActions) - only for the purpose of query analysis (type output).
     */

    if (select_query && (select_query->group_expression_list || select_query->having_expression))
        has_aggregation = true;

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(source_columns);

    if (select_query)
    {
        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            if (static_cast<const ASTTableJoin &>(*join->table_join).using_expression_list)
                getRootActions(
                    static_cast<const ASTTableJoin &>(*join->table_join).using_expression_list,
                    true,
                    false,
                    temp_actions);

            addJoinAction(temp_actions, true);
        }
    }

    getAggregates(ast, temp_actions);

    if (has_aggregation)
    {
        assertSelect();

        /// Find out aggregation keys.
        if (select_query->group_expression_list)
        {
            NameSet unique_keys;
            ASTs & group_asts = select_query->group_expression_list->children;
            for (ssize_t i = 0; i < static_cast<ssize_t>(group_asts.size()); ++i)
            {
                ssize_t size = group_asts.size();
                getRootActions(group_asts[i], true, false, temp_actions);

                const auto & column_name = group_asts[i]->getColumnName();
                const auto & block = temp_actions->getSampleBlock();

                if (!block.has(column_name))
                    throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                const auto & col = block.getByName(column_name);

                /// Constant expressions have non-null column pointer at this stage.
                if (const auto is_constexpr = col.column)
                {
                    /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                    if (!aggregate_descriptions.empty() || size > 1)
                    {
                        if (i + 1 < static_cast<ssize_t>(size))
                            group_asts[i] = std::move(group_asts.back());

                        group_asts.pop_back();

                        --i;
                        continue;
                    }
                }

                NameAndTypePair key{column_name, col.type};

                /// Aggregation keys are uniqued.
                if (!unique_keys.count(key.name))
                {
                    unique_keys.insert(key.name);
                    aggregation_keys.push_back(key);

                    /// Key is no longer needed, therefore we can save a little by moving it.
                    aggregated_columns.push_back(std::move(key));
                }
            }

            if (group_asts.empty())
            {
                select_query->group_expression_list = nullptr;
                has_aggregation = select_query->having_expression || !aggregate_descriptions.empty();
            }
        }

        for (const auto & desc : aggregate_descriptions)
            aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
    }
    else
    {
        aggregated_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
    }
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables()
{
    /// Adds existing external tables (not subqueries) to the external_tables dictionary.
    findExternalTables(ast);

    /// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
    initGlobalSubqueries(ast);
}


void ExpressionAnalyzer::initGlobalSubqueries(ASTPtr & ast)
{
    /// Recursive calls. We do not go into subqueries.

    for (auto & child : ast->children)
        if (!typeid_cast<ASTSelectQuery *>(child.get()))
            initGlobalSubqueries(child);

    /// Bottom-up actions.

    if (auto * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        /// For GLOBAL IN.
        if (do_global && (node->name == "globalIn" || node->name == "globalNotIn"))
            addExternalStorage(node->arguments->children.at(1));
    }
    else if (auto * node = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        /// For GLOBAL JOIN.
        if (do_global && node->table_join
            && static_cast<const ASTTableJoin &>(*node->table_join).locality == ASTTableJoin::Locality::Global)
            addExternalStorage(node->table_expression);
    }
}


void ExpressionAnalyzer::findExternalTables(ASTPtr & ast)
{
    /// Traverse from the bottom. Intentionally go into subqueries.
    for (auto & child : ast->children)
        findExternalTables(child);

    /// If table type identifier
    StoragePtr external_storage;

    if (auto * node = typeid_cast<ASTIdentifier *>(ast.get()))
        if (node->kind == ASTIdentifier::Table)
            if ((external_storage = context.tryGetExternalTable(node->name)))
                external_tables[node->name] = external_storage;
}


static std::pair<String, String> getDatabaseAndTableNameFromIdentifier(const ASTIdentifier & identifier)
{
    std::pair<String, String> res;
    res.second = identifier.name;
    if (!identifier.children.empty())
    {
        if (identifier.children.size() != 2)
            throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

        res.first = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
        res.second = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
    }
    return res;
}


static std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & subquery_or_table_name,
    const Context & context,
    size_t subquery_depth,
    const Names & required_source_columns)
{
    /// Subquery or table name. The name of the table is similar to the subquery `SELECT * FROM t`.
    const auto * subquery = typeid_cast<const ASTSubquery *>(subquery_or_table_name.get());
    const auto * table = typeid_cast<const ASTIdentifier *>(subquery_or_table_name.get());

    if (!subquery && !table)
        throw Exception("IN/JOIN supports only SELECT subqueries.", ErrorCodes::BAD_ARGUMENTS);

    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    Context subquery_context = context;
    Settings subquery_settings = context.getSettings();
    /// The calculation of `extremes` does not make sense and is not necessary (if you do it, then the `extremes` of the subquery can be taken instead of the whole query).
    subquery_settings.extremes = false;
    subquery_context.setSettings(subquery_settings);

    ASTPtr query;
    if (table)
    {
        /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
        const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        query = select_with_union_query;

        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

        const auto select_query = std::make_shared<ASTSelectQuery>();
        select_with_union_query->list_of_selects->children.push_back(select_query);

        const auto select_expression_list = std::make_shared<ASTExpressionList>();
        select_query->select_expression_list = select_expression_list;
        select_query->children.emplace_back(select_query->select_expression_list);

        /// get columns list for target table
        auto database_table = getDatabaseAndTableNameFromIdentifier(*table);
        const auto & storage = context.getTable(database_table.first, database_table.second);
        const auto & columns = storage->getColumns().ordinary;
        select_expression_list->children.reserve(columns.size());

        /// manually substitute column names in place of asterisk
        for (const auto & column : columns)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

        select_query->replaceDatabaseAndTable(database_table.first, database_table.second);
    }
    else
    {
        query = subquery->children.at(0);

        /** Columns with the same name can be specified in a subquery. For example, SELECT x, x FROM t
          * This is bad, because the result of such a query can not be saved to the table, because the table can not have the same name columns.
          * Saving to the table is required for GLOBAL subqueries.
          *
          * To avoid this situation, we will rename the same columns.
          */

        std::set<std::string> all_column_names;
        std::set<std::string> assigned_column_names;

        if (auto * select_with_union = typeid_cast<ASTSelectWithUnionQuery *>(query.get()))
        {
            if (auto * select = typeid_cast<ASTSelectQuery *>(select_with_union->list_of_selects->children.at(0).get()))
            {
                for (auto & expr : select->select_expression_list->children)
                    all_column_names.insert(expr->getAliasOrColumnName());

                for (auto & expr : select->select_expression_list->children)
                {
                    auto name = expr->getAliasOrColumnName();

                    if (!assigned_column_names.insert(name).second)
                    {
                        size_t i = 1;
                        while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
                            ++i;

                        name = name + "_" + toString(i);
                        expr = expr->clone(); /// Cancels fuse of the same expressions in the tree.
                        expr->setAlias(name);

                        all_column_names.insert(name);
                        assigned_column_names.insert(name);
                    }
                }
            }
        }
    }

    return std::make_shared<InterpreterSelectWithUnionQuery>(
        query,
        subquery_context,
        required_source_columns,
        QueryProcessingStage::Complete,
        subquery_depth + 1);
}


void ExpressionAnalyzer::addExternalStorage(ASTPtr & subquery_or_table_name_or_table_expression)
{
    /// With nondistributed queries, creating temporary tables does not make sense.
    if (!(storage && storage->isRemote()))
        return;

    ASTPtr subquery;
    ASTPtr table_name;
    ASTPtr subquery_or_table_name;

    if (typeid_cast<const ASTIdentifier *>(subquery_or_table_name_or_table_expression.get()))
    {
        table_name = subquery_or_table_name_or_table_expression;
        subquery_or_table_name = table_name;
    }
    else if (
        const auto * ast_table_expr
        = typeid_cast<const ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
    {
        if (ast_table_expr->database_and_table_name)
        {
            table_name = ast_table_expr->database_and_table_name;
            subquery_or_table_name = table_name;
        }
        else if (ast_table_expr->subquery)
        {
            subquery = ast_table_expr->subquery;
            subquery_or_table_name = subquery;
        }
    }
    else if (typeid_cast<const ASTSubquery *>(subquery_or_table_name_or_table_expression.get()))
    {
        subquery = subquery_or_table_name_or_table_expression;
        subquery_or_table_name = subquery;
    }

    if (!subquery_or_table_name)
        throw Exception(
            "Logical error: unknown AST element passed to ExpressionAnalyzer::addExternalStorage method",
            ErrorCodes::LOGICAL_ERROR);

    if (table_name)
    {
        /// If this is already an external table, you do not need to add anything. Just remember its presence.
        if (external_tables.end() != external_tables.find(static_cast<const ASTIdentifier &>(*table_name).name))
            return;
    }

    /// Generate the name for the external table.
    String external_table_name = "_data" + toString(external_table_id);
    while (external_tables.count(external_table_name))
    {
        ++external_table_id;
        external_table_name = "_data" + toString(external_table_id);
    }

    auto interpreter = interpretSubquery(subquery_or_table_name, context, subquery_depth, {});

    Block sample = interpreter->getSampleBlock();
    NamesAndTypesList columns = sample.getNamesAndTypesList();

    StoragePtr external_storage = StorageMemory::create(external_table_name, ColumnsDescription{columns});
    external_storage->startup();

    /** We replace the subquery with the name of the temporary table.
        * It is in this form, the request will go to the remote server.
        * This temporary table will go to the remote server, and on its side,
        *  instead of doing a subquery, you just need to read it.
        */

    auto database_and_table_name = std::make_shared<ASTIdentifier>(external_table_name, ASTIdentifier::Table);

    if (auto * ast_table_expr = typeid_cast<ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
    {
        ast_table_expr->subquery.reset();
        ast_table_expr->database_and_table_name = database_and_table_name;

        ast_table_expr->children.clear();
        ast_table_expr->children.emplace_back(database_and_table_name);
    }
    else
        subquery_or_table_name_or_table_expression = database_and_table_name;

    external_tables[external_table_name] = external_storage;
    subqueries_for_sets[external_table_name].source = interpreter->execute().in;
    subqueries_for_sets[external_table_name].table = external_storage;

    /** NOTE If it was written IN tmp_table - the existing temporary (but not external) table,
      *  then a new temporary table will be created (for example, _data1),
      *  and the data will then be copied to it.
      * Maybe this can be avoided.
      */
}


static NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(), [&](const NamesAndTypesList::value_type & val) {
        return val.name == name;
    });
}


/// ignore_levels - aliases in how many upper levels of the subtree should be ignored.
/// For example, with ignore_levels=1 ast can not be put in the dictionary, but its children can.
void ExpressionAnalyzer::addASTAliases(ASTPtr & ast, int ignore_levels)
{
    /// Bottom-up traversal. We do not go into subqueries.
    for (auto & child : ast->children)
    {
        int new_ignore_levels = std::max(0, ignore_levels - 1);

        /// Don't descent into table functions and subqueries.
        if (!typeid_cast<ASTTableExpression *>(child.get()) && !typeid_cast<ASTSelectWithUnionQuery *>(child.get()))
            addASTAliases(child, new_ignore_levels);
    }

    if (ignore_levels > 0)
        return;

    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
        {
            std::stringstream message;
            message << "Different expressions with the same alias " << backQuoteIfNeed(alias) << ":\n";
            formatAST(*ast, message, false, true);
            message << "\nand\n";
            formatAST(*aliases[alias], message, false, true);
            message << "\n";

            throw Exception(message.str(), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);
        }

        aliases[alias] = ast;
    }
    else if (auto * subquery = typeid_cast<ASTSubquery *>(ast.get()))
    {
        /// Set unique aliases for all subqueries. This is needed, because content of subqueries could change after recursive analysis,
        ///  and auto-generated column names could become incorrect.

        size_t subquery_index = 1;
        while (true)
        {
            alias = "_subquery" + toString(subquery_index);
            if (!aliases.count("_subquery" + toString(subquery_index)))
                break;
            ++subquery_index;
        }

        subquery->setAlias(alias);
        subquery->prefer_alias_to_column_name = true;
        aliases[alias] = ast;
    }
}


void ExpressionAnalyzer::normalizeTree()
{
    SetOfASTs tmp_set;
    MapOfASTs tmp_map;
    normalizeTreeImpl(ast, tmp_map, tmp_set, "", 0);

    try
    {
        ast->checkSize(settings.max_expanded_ast_elements);
    }
    catch (Exception & e)
    {
        e.addMessage("(after expansion of aliases)");
        throw;
    }
}


/// finished_asts - already processed vertices (and by what they replaced)
/// current_asts - vertices in the current call stack of this method
/// current_alias - the alias referencing to the ancestor of ast (the deepest ancestor with aliases)
void ExpressionAnalyzer::normalizeTreeImpl(
    ASTPtr & ast,
    MapOfASTs & finished_asts,
    SetOfASTs & current_asts,
    std::string current_alias,
    size_t level)
{
    if (level > settings.max_ast_depth)
        throw Exception(
            "Normalized AST is too deep. Maximum: " + settings.max_ast_depth.toString(),
            ErrorCodes::TOO_DEEP_AST);

    if (finished_asts.count(ast))
    {
        ast = finished_asts[ast];
        return;
    }

    ASTPtr initial_ast = ast;
    current_asts.insert(initial_ast.get());

    String my_alias = ast->tryGetAlias();
    if (!my_alias.empty())
        current_alias = my_alias;

    /// rewrite rules that act when you go from top to bottom.
    bool replaced = false;

    ASTIdentifier * identifier_node = nullptr;
    ASTFunction * func_node = nullptr;

    if ((func_node = typeid_cast<ASTFunction *>(ast.get())))
    {
        /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
        if (functionIsInOrGlobalInOperator(func_node->name))
            if (auto * right = typeid_cast<ASTIdentifier *>(func_node->arguments->children.at(1).get()))
                if (!aliases.count(right->name))
                    right->kind = ASTIdentifier::Table;

        /// Special cases for count function.
        String func_name_lowercase = Poco::toLower(func_node->name);
        if (startsWith(func_name_lowercase, "count"))
        {
            /// Select implementation of countDistinct based on settings.
            /// Important that it is done as query rewrite. It means rewritten query
            ///  will be sent to remote servers during distributed query execution,
            ///  and on all remote servers, function implementation will be same.
            if (endsWith(func_node->name, "Distinct") && func_name_lowercase == "countdistinct")
                func_node->name = settings.count_distinct_implementation;

            /// As special case, treat count(*) as count(), not as count(list of all columns).
            if (func_name_lowercase == "count" && func_node->arguments->children.size() == 1
                && typeid_cast<const ASTAsterisk *>(func_node->arguments->children[0].get()))
            {
                func_node->arguments->children.clear();
            }
        }
    }
    else if ((identifier_node = typeid_cast<ASTIdentifier *>(ast.get())))
    {
        if (identifier_node->kind == ASTIdentifier::Column && identifier_node->can_be_alias)
        {
            /// If it is an alias, but not a parent alias (for constructs like "SELECT column + 1 AS column").
            auto it_alias = aliases.find(identifier_node->name);
            if (it_alias != aliases.end() && current_alias != identifier_node->name)
            {
                /// Let's replace it with the corresponding tree node.
                if (current_asts.count(it_alias->second.get()))
                    throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);

                if (!my_alias.empty() && my_alias != it_alias->second->getAliasOrColumnName())
                {
                    /// Avoid infinite recursion here
                    auto * replace_to_identifier = typeid_cast<ASTIdentifier *>(it_alias->second.get());
                    bool is_cycle = replace_to_identifier && replace_to_identifier->kind == ASTIdentifier::Column
                        && replace_to_identifier->name == identifier_node->name;

                    if (!is_cycle)
                    {
                        /// In a construct like "a AS b", where a is an alias, you must set alias b to the result of substituting alias a.
                        ast = it_alias->second->clone();
                        ast->setAlias(my_alias);
                        replaced = true;
                    }
                }
                else
                {
                    ast = it_alias->second;
                    replaced = true;
                }
            }
        }
    }
    else if (auto * node = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        // Get hidden column names of mutable storage
        OrderedNameSet filtered_names;
        {
            if (storage && select_query && !select_query->raw_for_mutable)
            {
                // LOG_DEBUG(&Poco::Logger::get("ExpressionAnalyzer"), "Filter hidden columns for mutable table.");
                filtered_names = MutSup::instance().hiddenColumns(storage->getName());
            }
        }

        /// Replace * with a list of columns.
        ASTs & asts = node->children;
        for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
        {
            if (typeid_cast<ASTAsterisk *>(asts[i].get()))
            {
                ASTs all_columns;

                if (storage)
                {
                    /// If we select from a table, get only not MATERIALIZED, not ALIAS columns.
                    for (const auto & name_type : storage->getColumns().ordinary)
                        if (!filtered_names.has(name_type.name))
                            all_columns.emplace_back(std::make_shared<ASTIdentifier>(name_type.name));
                }
                else
                {
                    for (const auto & name_type : source_columns)
                        all_columns.emplace_back(std::make_shared<ASTIdentifier>(name_type.name));
                }

                asts.erase(asts.begin() + i);
                asts.insert(asts.begin() + i, all_columns.begin(), all_columns.end());
            }
        }
    }
    else if (auto * node = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        if (node->table_expression)
        {
            auto & database_and_table_name
                = static_cast<ASTTableExpression &>(*node->table_expression).database_and_table_name;
            if (database_and_table_name)
            {
                if (auto * right = typeid_cast<ASTIdentifier *>(database_and_table_name.get()))
                {
                    right->kind = ASTIdentifier::Table;
                }
            }
        }
    }

    /// If we replace the root of the subtree, we will be called again for the new root, in case the alias is replaced by an alias.
    if (replaced)
    {
        normalizeTreeImpl(ast, finished_asts, current_asts, current_alias, level + 1);
        current_asts.erase(initial_ast.get());
        current_asts.erase(ast.get());
        finished_asts[initial_ast] = ast;
        return;
    }

    /// Recurring calls. Don't go into subqueries. Don't go into components of compound identifiers.
    /// We also do not go to the left argument of lambda expressions, so as not to replace the formal parameters
    ///  on aliases in expressions of the form 123 AS x, arrayMap(x -> 1, [2]).

    if (func_node && func_node->name == "lambda")
    {
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        for (size_t i = 1, size = func_node->arguments->children.size(); i < size; ++i)
        {
            auto & child = func_node->arguments->children[i];

            if (typeid_cast<const ASTSelectQuery *>(child.get())
                || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            normalizeTreeImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }
    else if (identifier_node) {}
    else
    {
        for (auto & child : ast->children)
        {
            if (typeid_cast<const ASTSelectQuery *>(child.get())
                || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            normalizeTreeImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children, but also in where_expression and having_expression.
    if (auto * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        if (select->prewhere_expression)
            normalizeTreeImpl(select->prewhere_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->where_expression)
            normalizeTreeImpl(select->where_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->having_expression)
            normalizeTreeImpl(select->having_expression, finished_asts, current_asts, current_alias, level + 1);
    }

    current_asts.erase(initial_ast.get());
    current_asts.erase(ast.get());
    finished_asts[initial_ast] = ast;
}


void ExpressionAnalyzer::addAliasColumns()
{
    if (!select_query)
        return;

    if (!storage)
        return;

    const auto & aliases = storage->getColumns().aliases;
    source_columns.insert(std::end(source_columns), std::begin(aliases), std::end(aliases));
}


void ExpressionAnalyzer::executeScalarSubqueries()
{
    if (!select_query)
        executeScalarSubqueriesImpl(ast);
    else
    {
        for (auto & child : ast->children)
        {
            /// Do not go to FROM, JOIN, UNION.
            if (!typeid_cast<const ASTTableExpression *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
            {
                executeScalarSubqueriesImpl(child);
            }
        }
    }
}


static ASTPtr addTypeConversion(std::unique_ptr<ASTLiteral> && ast, const String & type_name)
{
    auto func = std::make_shared<ASTFunction>();
    ASTPtr res = func;
    func->alias = ast->alias;
    func->prefer_alias_to_column_name = ast->prefer_alias_to_column_name;
    ast->alias.clear();
    func->name = "CAST";
    auto exp_list = std::make_shared<ASTExpressionList>();
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    exp_list->children.emplace_back(ast.release());
    exp_list->children.emplace_back(std::make_shared<ASTLiteral>(type_name));
    return res;
}


void ExpressionAnalyzer::executeScalarSubqueriesImpl(ASTPtr & ast)
{
    /** Replace subqueries that return exactly one row
      * ("scalar" subqueries) to the corresponding constants.
      *
      * If the subquery returns more than one column, it is replaced by a tuple of constants.
      *
      * Features
      *
      * A replacement occurs during query analysis, and not during the main runtime.
      * This means that the progress indicator will not work during the execution of these requests,
      *  and also such queries can not be aborted.
      *
      * But the query result can be used for the index in the table.
      *
      * Scalar subqueries are executed on the request-initializer server.
      * The request is sent to remote servers with already substituted constants.
      */

    if (auto * subquery = typeid_cast<ASTSubquery *>(ast.get()))
    {
        Context subquery_context = context;
        Settings subquery_settings = context.getSettings();
        subquery_settings.extremes = false;
        subquery_context.setSettings(subquery_settings);

        ASTPtr query = subquery->children.at(0);
        BlockIO res = InterpreterSelectWithUnionQuery(
                          query,
                          subquery_context,
                          {},
                          QueryProcessingStage::Complete,
                          subquery_depth + 1)
                          .execute();

        Block block;
        try
        {
            block = res.in->read();

            if (!block)
            {
                /// Interpret subquery with empty result as Null literal
                auto ast_new = std::make_unique<ASTLiteral>(Null());
                ast_new->setAlias(ast->tryGetAlias());
                ast = std::move(ast_new);
                return;
            }

            if (block.rows() != 1 || res.in->read())
                throw Exception(
                    "Scalar subquery returned more than one row",
                    ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::TOO_MANY_ROWS)
                throw Exception(
                    "Scalar subquery returned more than one row",
                    ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
            else
                throw;
        }

        size_t columns = block.columns();
        if (columns == 1)
        {
            auto lit = std::make_unique<ASTLiteral>((*block.safeGetByPosition(0).column)[0]);
            lit->alias = subquery->alias;
            lit->prefer_alias_to_column_name = subquery->prefer_alias_to_column_name;
            ast = addTypeConversion(std::move(lit), block.safeGetByPosition(0).type->getName());
        }
        else
        {
            auto tuple = std::make_shared<ASTFunction>();
            tuple->alias = subquery->alias;
            ast = tuple;
            tuple->name = "tuple";
            auto exp_list = std::make_shared<ASTExpressionList>();
            tuple->arguments = exp_list;
            tuple->children.push_back(tuple->arguments);

            exp_list->children.resize(columns);
            for (size_t i = 0; i < columns; ++i)
            {
                exp_list->children[i] = addTypeConversion(
                    std::make_unique<ASTLiteral>((*block.safeGetByPosition(i).column)[0]),
                    block.safeGetByPosition(i).type->getName());
            }
        }
    }
    else
    {
        /** Don't descend into subqueries in FROM section.
          */
        if (!typeid_cast<ASTTableExpression *>(ast.get()))
        {
            /** Don't descend into subqueries in arguments of IN operator.
              * But if an argument is not subquery, than deeper may be scalar subqueries and we need to descend in them.
              */
            auto * func = typeid_cast<ASTFunction *>(ast.get());

            if (func && functionIsInOrGlobalInOperator(func->name))
            {
                for (auto & child : ast->children)
                {
                    if (child != func->arguments)
                        executeScalarSubqueriesImpl(child);
                    else
                        for (size_t i = 0, size = func->arguments->children.size(); i < size; ++i)
                            if (i != 1 || !typeid_cast<ASTSubquery *>(func->arguments->children[i].get()))
                                executeScalarSubqueriesImpl(func->arguments->children[i]);
                }
            }
            else
                for (auto & child : ast->children)
                    executeScalarSubqueriesImpl(child);
        }
    }
}


void ExpressionAnalyzer::optimizeGroupBy()
{
    if (!(select_query && select_query->group_expression_list))
        return;

    const auto is_literal = [](const ASTPtr & ast) {
        return typeid_cast<const ASTLiteral *>(ast.get());
    };

    auto & group_exprs = select_query->group_expression_list->children;

    /// removes expression at index idx by making it last one and calling .pop_back()
    const auto remove_expr_at_index = [&group_exprs](const size_t idx) {
        if (idx < group_exprs.size() - 1)
            std::swap(group_exprs[idx], group_exprs.back());

        group_exprs.pop_back();
    };

    /// iterate over each GROUP BY expression, eliminate injective function calls and literals
    for (size_t i = 0; i < group_exprs.size();)
    {
        if (const auto * function = typeid_cast<ASTFunction *>(group_exprs[i].get()))
        {
            if (!injective_function_names.count(function->name))
            {
                ++i;
                continue;
            }

            /// copy shared pointer to args in order to ensure lifetime
            auto args_ast = function->arguments;

            /** remove function call and take a step back to ensure
              * next iteration does not skip not yet processed data
              */
            remove_expr_at_index(i);

            /// copy non-literal arguments
            std::remove_copy_if(
                std::begin(args_ast->children),
                std::end(args_ast->children),
                std::back_inserter(group_exprs),
                is_literal);
        }
        else if (is_literal(group_exprs[i]))
        {
            remove_expr_at_index(i);
        }
        else
        {
            /// if neither a function nor literal - advance to next expression
            ++i;
        }
    }

    if (group_exprs.empty())
    {
        /** You can not completely remove GROUP BY. Because if there were no aggregate functions, then it turns out that there will be no aggregation.
          * Instead, leave `GROUP BY const`.
          * Next, see deleting the constants in the analyzeAggregation method.
          */

        /// You must insert a constant that is not the name of the column in the table. Such a case is rare, but it happens.
        UInt64 unused_column = 0;
        String unused_column_name = toString(unused_column);

        while (source_columns.end()
               != std::find_if(
                   source_columns.begin(),
                   source_columns.end(),
                   [&unused_column_name](const NameAndTypePair & name_type) {
                       return name_type.name == unused_column_name;
                   }))
        {
            ++unused_column;
            unused_column_name = toString(unused_column);
        }

        select_query->group_expression_list = std::make_shared<ASTExpressionList>();
        select_query->group_expression_list->children.emplace_back(std::make_shared<ASTLiteral>(unused_column));
    }
}


void ExpressionAnalyzer::optimizeOrderBy()
{
    if (!(select_query && select_query->order_expression_list))
        return;

    /// Make unique sorting conditions.
    using NameAndLocale = std::pair<String, String>;
    std::set<NameAndLocale> elems_set;

    ASTs & elems = select_query->order_expression_list->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        String name = elem->children.front()->getColumnName();
        const auto & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

        if (elems_set.emplace(name, order_by_elem.collation ? order_by_elem.collation->getColumnName() : "").second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = unique_elems;
}


void ExpressionAnalyzer::optimizeLimitBy()
{
    if (!(select_query && select_query->limit_by_expression_list))
        return;

    std::set<String> elems_set;

    ASTs & elems = select_query->limit_by_expression_list->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        if (elems_set.emplace(elem->getColumnName()).second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = unique_elems;
}


void ExpressionAnalyzer::makeSetsForIndex()
{
    if (storage && select_query && storage->supportsIndexForIn())
    {
        if (select_query->where_expression)
            makeSetsForIndexImpl(select_query->where_expression, storage->getSampleBlock());
        if (select_query->prewhere_expression)
            makeSetsForIndexImpl(select_query->prewhere_expression, storage->getSampleBlock());
    }
}


void ExpressionAnalyzer::tryMakeSetFromSubquery(const ASTPtr & subquery_or_table_name)
{
    BlockIO res = interpretSubquery(subquery_or_table_name, context, subquery_depth + 1, {})->execute();

    SetPtr set = std::make_shared<Set>(
        SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode));

    set->setHeader(res.in->getHeader());
    while (Block block = res.in->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block, true))
            return;
    }

    prepared_sets[subquery_or_table_name.get()] = std::move(set);
}


void ExpressionAnalyzer::makeSetsForIndexImpl(const ASTPtr & node, const Block & sample_block)
{
    for (auto & child : node->children)
    {
        /// Don't descent into subqueries.
        if (typeid_cast<ASTSubquery *>(child.get()))
            continue;

        /// Don't dive into lambda functions
        const auto * func = typeid_cast<const ASTFunction *>(child.get());
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndexImpl(child, sample_block);
    }

    const auto * func = typeid_cast<const ASTFunction *>(node.get());
    if (func && functionIsInOperator(func->name))
    {
        const IAST & args = *func->arguments;

        if (storage && storage->mayBenefitFromIndexForIn(args.children.at(0)))
        {
            const ASTPtr & arg = args.children.at(1);

            if (!prepared_sets.count(arg.get())) /// Not already prepared.
            {
                if (typeid_cast<ASTSubquery *>(arg.get()) || typeid_cast<ASTIdentifier *>(arg.get()))
                {
                    if (settings.use_index_for_in_with_subqueries)
                        tryMakeSetFromSubquery(arg);
                }
                else
                {
                    NamesAndTypesList temp_columns = source_columns;
                    temp_columns.insert(temp_columns.end(), columns_added_by_join.begin(), columns_added_by_join.end());
                    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(temp_columns);
                    getRootActions(func->arguments->children.at(0), true, false, temp_actions);

                    Block sample_block_with_calculated_columns = temp_actions->getSampleBlock();
                    if (sample_block_with_calculated_columns.has(args.children.at(0)->getColumnName()))
                        makeExplicitSet(func, sample_block_with_calculated_columns, true);
                }
            }
        }
    }
}


void ExpressionAnalyzer::makeSet(const ASTFunction * node, const Block & sample_block)
{
    /** You need to convert the right argument to a set.
      * This can be a table name, a value, a value enumeration, or a subquery.
      * The enumeration of values is parsed as a function `tuple`.
      */
    const IAST & args = *node->arguments;
    const ASTPtr & arg = args.children.at(1);

    /// Already converted.
    if (prepared_sets.count(arg.get()))
        return;

    /// If the subquery or table name for SELECT.
    const auto * identifier = typeid_cast<const ASTIdentifier *>(arg.get());
    if (typeid_cast<const ASTSubquery *>(arg.get()) || identifier)
    {
        /// We get the stream of blocks for the subquery. Create Set and put it in place of the subquery.
        String set_id = arg->getColumnName();

        /// A special case is if the name of the table is specified on the right side of the IN statement,
        ///  and the table has the type Set (a previously prepared set).
        if (identifier)
        {
            auto database_table = getDatabaseAndTableNameFromIdentifier(*identifier);
            StoragePtr table = context.tryGetTable(database_table.first, database_table.second);

            if (table)
            {
                auto * storage_set = dynamic_cast<StorageSet *>(table.get());

                if (storage_set)
                {
                    prepared_sets[arg.get()] = storage_set->getSet();
                    return;
                }
            }
        }

        SubqueryForSet & subquery_for_set = subqueries_for_sets[set_id];

        /// If you already created a Set with the same subquery / table.
        if (subquery_for_set.set)
        {
            prepared_sets[arg.get()] = subquery_for_set.set;
            return;
        }

        SetPtr set = std::make_shared<Set>(
            SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode));

        /** The following happens for GLOBAL INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          */
        if (!subquery_for_set.source && (!storage || !storage->isRemote()))
        {
            auto interpreter = interpretSubquery(arg, context, subquery_depth, {});
            subquery_for_set.source
                = std::make_shared<LazyBlockInputStream>(interpreter->getSampleBlock(), [interpreter]() mutable {
                      return interpreter->execute().in;
                  });

            /** Why is LazyBlockInputStream used?
              *
              * The fact is that when processing a query of the form
              *  SELECT ... FROM remote_test WHERE column GLOBAL IN (subquery),
              *  if the distributed remote_test table contains localhost as one of the servers,
              *  the query will be interpreted locally again (and not sent over TCP, as in the case of a remote server).
              *
              * The query execution pipeline will be:
              * CreatingSets
              *  subquery execution, filling the temporary table with _data1 (1)
              *  CreatingSets
              *   reading from the table _data1, creating the set (2)
              *   read from the table subordinate to remote_test.
              *
              * (The second part of the pipeline under CreateSets is a reinterpretation of the query inside StorageDistributed,
              *  the query differs in that the database name and tables are replaced with subordinates, and the subquery is replaced with _data1.)
              *
              * But when creating the pipeline, when creating the source (2), it will be found that the _data1 table is empty
              *  (because the query has not started yet), and empty source will be returned as the source.
              * And then, when the query is executed, an empty set will be created in step (2).
              *
              * Therefore, we make the initialization of step (2) lazy
              *  - so that it does not occur until step (1) is completed, on which the table will be populated.
              *
              * Note: this solution is not very good, you need to think better.
              */
        }

        subquery_for_set.set = set;
        prepared_sets[arg.get()] = set;
    }
    else
    {
        /// An explicit enumeration of values in parentheses.
        makeExplicitSet(node, sample_block, false);
    }
}

/// The case of an explicit enumeration of values.
void ExpressionAnalyzer::makeExplicitSet(const ASTFunction * node, const Block & sample_block, bool create_ordered_set)
{
    const IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception(
            "Wrong number of arguments passed to function in",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTPtr & arg = args.children.at(1);

    DataTypes set_element_types;
    const ASTPtr & left_arg = args.children.at(0);

    const auto * left_arg_tuple = typeid_cast<const ASTFunction *>(left_arg.get());

    /** NOTE If tuple in left hand side specified non-explicitly
      * Example: identity((a, b)) IN ((1, 2), (3, 4))
      *  instead of       (a, b)) IN ((1, 2), (3, 4))
      * then set creation doesn't work correctly.
      */
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        for (const auto & arg : left_arg_tuple->arguments->children)
            set_element_types.push_back(sample_block.getByName(arg->getColumnName()).type);
    }
    else
    {
        DataTypePtr left_type = sample_block.getByName(left_arg->getColumnName()).type;
        set_element_types.push_back(left_type);
    }

    /// The case `x in (1, 2)` distinguishes from the case `x in 1` (also `x in (1)`).
    bool single_value = false;
    ASTPtr elements_ast = arg;

    if (auto * set_func = typeid_cast<ASTFunction *>(arg.get()))
    {
        if (set_func->name == "tuple")
        {
            if (set_func->arguments->children.empty())
            {
                /// Empty set.
                elements_ast = set_func->arguments;
            }
            else
            {
                /// Distinguish the case `(x, y) in ((1, 2), (3, 4))` from the case `(x, y) in (1, 2)`.
                auto * any_element = typeid_cast<ASTFunction *>(set_func->arguments->children.at(0).get());
                if (set_element_types.size() >= 2 && (!any_element || any_element->name != "tuple"))
                    single_value = true;
                else
                    elements_ast = set_func->arguments;
            }
        }
        else
        {
            if (set_element_types.size() >= 2)
                throw Exception(
                    "Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of "
                        + toString(set_element_types.size()) + "-element tuples.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            single_value = true;
        }
    }
    else if (typeid_cast<ASTLiteral *>(arg.get()))
    {
        single_value = true;
    }
    else
    {
        throw Exception(
            "Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of values.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    if (single_value)
    {
        ASTPtr exp_list = std::make_shared<ASTExpressionList>();
        exp_list->children.push_back(elements_ast);
        elements_ast = exp_list;
    }

    SetPtr set = std::make_shared<Set>(
        SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode));
    set->createFromAST(set_element_types, elements_ast, context, create_ordered_set);
    prepared_sets[arg.get()] = std::move(set);
}


static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}


/** For getActionsImpl.
  * A stack of ExpressionActions corresponding to nested lambda expressions.
  * The new action should be added to the highest possible level.
  * For example, in the expression "select arrayMap(x -> x + column1 * column2, array1)"
  *  calculation of the product must be done outside the lambda expression (it does not depend on x), and the calculation of the sum is inside (depends on x).
  */
struct ExpressionAnalyzer::ScopeStack
{
    struct Level
    {
        ExpressionActionsPtr actions;
        NameSet new_columns;
    };

    using Levels = std::vector<Level>;

    Levels stack;
    Settings settings;

    ScopeStack(const ExpressionActionsPtr & actions, const Settings & settings_)
        : settings(settings_)
    {
        stack.emplace_back();
        stack.back().actions = actions;

        const Block & sample_block = actions->getSampleBlock();
        for (size_t i = 0, size = sample_block.columns(); i < size; ++i)
            stack.back().new_columns.insert(sample_block.getByPosition(i).name);
    }

    void pushLevel(const NamesAndTypesList & input_columns)
    {
        stack.emplace_back();
        Level & prev = stack[stack.size() - 2];

        ColumnsWithTypeAndName all_columns;
        NameSet new_names;

        for (const auto & input_column : input_columns)
        {
            all_columns.emplace_back(nullptr, input_column.type, input_column.name);
            new_names.insert(input_column.name);
            stack.back().new_columns.insert(input_column.name);
        }

        const Block & prev_sample_block = prev.actions->getSampleBlock();
        for (size_t i = 0, size = prev_sample_block.columns(); i < size; ++i)
        {
            const ColumnWithTypeAndName & col = prev_sample_block.getByPosition(i);
            if (!new_names.count(col.name))
                all_columns.push_back(col);
        }

        stack.back().actions = std::make_shared<ExpressionActions>(all_columns);
    }

    size_t getColumnLevel(const std::string & name)
    {
        for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
            if (stack[i].new_columns.count(name))
                return i;

        throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    void addAction(const ExpressionAction & action)
    {
        size_t level = 0;
        Names required = action.getNeededColumns();
        for (const auto & name : required)
            level = std::max(level, getColumnLevel(name));

        Names added;
        stack[level].actions->add(action, added);

        stack[level].new_columns.insert(added.begin(), added.end());

        for (const auto & name : added)
        {
            const ColumnWithTypeAndName & col = stack[level].actions->getSampleBlock().getByName(name);
            for (size_t j = level + 1; j < stack.size(); ++j)
                stack[j].actions->addInput(col);
        }
    }

    ExpressionActionsPtr popLevel()
    {
        ExpressionActionsPtr res = stack.back().actions;
        stack.pop_back();
        return res;
    }

    const Block & getSampleBlock() const { return stack.back().actions->getSampleBlock(); }
};


void ExpressionAnalyzer::getRootActions(
    const ASTPtr & ast,
    bool no_subqueries,
    bool only_consts,
    ExpressionActionsPtr & actions)
{
    ScopeStack scopes(actions, settings);
    getActionsImpl(ast, no_subqueries, only_consts, scopes);
    actions = scopes.popLevel();
}

void ExpressionAnalyzer::getActionsImpl(
    const ASTPtr & ast,
    bool no_subqueries,
    bool only_consts,
    ScopeStack & actions_stack)
{
    /// If the result of the calculation already exists in the block.
    if ((typeid_cast<ASTFunction *>(ast.get()) || typeid_cast<ASTLiteral *>(ast.get()))
        && actions_stack.getSampleBlock().has(ast->getColumnName()))
        return;

    if (auto * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        std::string name = node->getColumnName();
        if (!only_consts && !actions_stack.getSampleBlock().has(name))
        {
            /// The requested column is not in the block.
            /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

            bool found = false;
            for (const auto & column_name_type : source_columns)
                if (column_name_type.name == name)
                    found = true;

            if (found)
                throw Exception(
                    "Column " + name + " is not under aggregate function and not in GROUP BY.",
                    ErrorCodes::NOT_AN_AGGREGATE);
        }
    }
    else if (auto * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->name == "lambda")
            throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

        if (functionIsInOrGlobalInOperator(node->name))
        {
            if (!no_subqueries)
            {
                /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
                getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);

                /// Transform tuple or subquery into a set.
                makeSet(node, actions_stack.getSampleBlock());
            }
            else
            {
                if (!only_consts)
                {
                    /// We are in the part of the tree that we are not going to compute. You just need to define types.
                    /// Do not subquery and create sets. We insert an arbitrary column of the correct type.
                    ColumnWithTypeAndName fake_column;
                    fake_column.name = node->getColumnName();
                    fake_column.type = std::make_shared<DataTypeUInt8>();
                    actions_stack.addAction(ExpressionAction::addColumn(fake_column));
                    getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);
                }
                return;
            }
        }

        /// A special function `indexHint`. Everything that is inside it is not calculated
        /// (and is used only for index analysis, see KeyCondition).
        if (node->name == "indexHint")
        {
            actions_stack.addAction(ExpressionAction::addColumn(ColumnWithTypeAndName(
                ColumnConst::create(ColumnUInt8::create(1, 1), 1),
                std::make_shared<DataTypeUInt8>(),
                node->getColumnName())));
            return;
        }

        if (AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
            return;

        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(node->name, context);

        Names argument_names;
        DataTypes argument_types;
        bool arguments_present = true;

        /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
        bool has_lambda_arguments = false;

        for (auto & child : node->arguments->children)
        {
            auto * lambda = typeid_cast<ASTFunction *>(child.get());
            if (lambda && lambda->name == "lambda")
            {
                /// If the argument is a lambda expression, just remember its approximate type.
                if (lambda->arguments->children.size() != 2)
                    throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                auto * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());

                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                    throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

                has_lambda_arguments = true;
                argument_types.emplace_back(
                    std::make_shared<DataTypeFunction>(DataTypes(lambda_args_tuple->arguments->children.size())));
                /// Select the name in the next cycle.
                argument_names.emplace_back();
            }
            else if (prepared_sets.count(child.get()))
            {
                ColumnWithTypeAndName column;
                column.type = std::make_shared<DataTypeSet>();

                const SetPtr & set = prepared_sets[child.get()];

                /// If the argument is a set given by an enumeration of values (so, the set was already built), give it a unique name,
                ///  so that sets with the same literal representation do not fuse together (they can have different types).
                if (!set->empty())
                    column.name = getUniqueName(actions_stack.getSampleBlock(), "__set");
                else
                    column.name = child->getColumnName();

                if (!actions_stack.getSampleBlock().has(column.name))
                {
                    column.column = ColumnSet::create(1, set);

                    actions_stack.addAction(ExpressionAction::addColumn(column));
                }

                argument_types.push_back(column.type);
                argument_names.push_back(column.name);
            }
            else
            {
                /// If the argument is not a lambda expression, call it recursively and find out its type.
                getActionsImpl(child, no_subqueries, only_consts, actions_stack);
                std::string name = child->getColumnName();
                if (actions_stack.getSampleBlock().has(name))
                {
                    argument_types.push_back(actions_stack.getSampleBlock().getByName(name).type);
                    argument_names.push_back(name);
                }
                else
                {
                    if (only_consts)
                    {
                        arguments_present = false;
                    }
                    else
                    {
                        throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
                    }
                }
            }
        }

        if (only_consts && !arguments_present)
            return;

        if (has_lambda_arguments && !only_consts)
        {
            function_builder->getLambdaArgumentTypes(argument_types);

            /// Call recursively for lambda expressions.
            for (size_t i = 0; i < node->arguments->children.size(); ++i)
            {
                ASTPtr child = node->arguments->children[i];

                auto * lambda = typeid_cast<ASTFunction *>(child.get());
                if (lambda && lambda->name == "lambda")
                {
                    const auto * lambda_type = typeid_cast<const DataTypeFunction *>(argument_types[i].get());
                    auto * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());
                    ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
                    NamesAndTypesList lambda_arguments;

                    for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
                    {
                        auto * identifier = typeid_cast<ASTIdentifier *>(lambda_arg_asts[j].get());
                        if (!identifier)
                            throw Exception(
                                "lambda argument declarations must be identifiers",
                                ErrorCodes::TYPE_MISMATCH);

                        String arg_name = identifier->name;

                        lambda_arguments.emplace_back(arg_name, lambda_type->getArgumentTypes()[j]);
                    }

                    actions_stack.pushLevel(lambda_arguments);
                    getActionsImpl(lambda->arguments->children.at(1), no_subqueries, only_consts, actions_stack);
                    ExpressionActionsPtr lambda_actions = actions_stack.popLevel();

                    String result_name = lambda->arguments->children.at(1)->getColumnName();
                    lambda_actions->finalize(Names(1, result_name));
                    DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;

                    Names captured;
                    Names required = lambda_actions->getRequiredColumns();
                    for (const auto & name : required)
                        if (findColumn(name, lambda_arguments) == lambda_arguments.end())
                            captured.push_back(name);

                    /// We can not name `getColumnName()`,
                    ///  because it does not uniquely define the expression (the types of arguments can be different).
                    String lambda_name = getUniqueName(actions_stack.getSampleBlock(), "__lambda");

                    auto function_capture = std::make_shared<FunctionCapture>(
                        lambda_actions,
                        captured,
                        lambda_arguments,
                        result_type,
                        result_name);
                    actions_stack.addAction(ExpressionAction::applyFunction(function_capture, captured, lambda_name));

                    argument_types[i]
                        = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), result_type);
                    argument_names[i] = lambda_name;
                }
            }
        }

        if (only_consts)
        {
            for (const auto & argument_name : argument_names)
            {
                if (!actions_stack.getSampleBlock().has(argument_name))
                {
                    arguments_present = false;
                    break;
                }
            }
        }

        if (arguments_present)
            actions_stack.addAction(
                ExpressionAction::applyFunction(function_builder, argument_names, node->getColumnName()));
    }
    else if (auto * node = typeid_cast<ASTLiteral *>(ast.get()))
    {
        DataTypePtr type = applyVisitor(FieldToDataType(), node->value);

        ColumnWithTypeAndName column;
        column.column = type->createColumnConst(1, convertFieldToType(node->value, *type));
        column.type = type;
        column.name = node->getColumnName();

        actions_stack.addAction(ExpressionAction::addColumn(column));
    }
    else
    {
        for (auto & child : ast->children)
        {
            /// Do not go to FROM, JOIN, UNION.
            if (!typeid_cast<const ASTTableExpression *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
                getActionsImpl(child, no_subqueries, only_consts, actions_stack);
        }
    }
}


void ExpressionAnalyzer::getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query
        && (ast.get() == select_query->where_expression.get() || ast.get() == select_query->prewhere_expression.get()))
    {
        assertNoAggregates(ast, "in WHERE or PREWHERE");
        return;
    }

    /// If we are not analyzing a SELECT query, but a separate expression, then there can not be aggregate functions in it.
    if (!select_query)
    {
        assertNoAggregates(ast, "in wrong place");
        return;
    }

    const auto * node = typeid_cast<const ASTFunction *>(ast.get());
    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
    {
        has_aggregation = true;
        AggregateDescription aggregate;
        aggregate.column_name = node->getColumnName();

        /// Make unique aggregate functions.
        for (const auto & aggregate_description : aggregate_descriptions)
            if (aggregate_description.column_name == aggregate.column_name)
                return;

        const ASTs & arguments = node->arguments->children;
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            /// There can not be other aggregate functions within the aggregate functions.
            assertNoAggregates(arguments[i], "inside another aggregate function");

            getRootActions(arguments[i], true, false, actions);
            const std::string & name = arguments[i]->getColumnName();
            types[i] = actions->getSampleBlock().getByName(name).type;
            aggregate.argument_names[i] = name;
        }

        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(context, node->name, types, aggregate.parameters);

        aggregate_descriptions.push_back(aggregate);
    }
    else
    {
        for (const auto & child : ast->children)
            if (!typeid_cast<const ASTSubquery *>(child.get()) && !typeid_cast<const ASTSelectQuery *>(child.get()))
                getAggregates(child, actions);
    }
}


void ExpressionAnalyzer::assertNoAggregates(const ASTPtr & ast, const char * description)
{
    const auto * node = typeid_cast<const ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        throw Exception(
            "Aggregate function " + node->getColumnName() + " is found " + String(description) + " in query",
            ErrorCodes::ILLEGAL_AGGREGATION);

    for (const auto & child : ast->children)
        if (!typeid_cast<const ASTSubquery *>(child.get()) && !typeid_cast<const ASTSelectQuery *>(child.get()))
            assertNoAggregates(child, description);
}


void ExpressionAnalyzer::assertSelect() const
{
    if (!select_query)
        throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::assertAggregation() const
{
    if (!has_aggregation)
        throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns)
{
    if (chain.steps.empty())
    {
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns));
    }
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
    if (only_types)
        actions->add(ExpressionAction::ordinaryJoin(nullptr, columns_added_by_join));
    else
        for (const auto & subquery_for_set : subqueries_for_sets)
            if (subquery_for_set.second.join)
                actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, columns_added_by_join));
}

bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain &, bool)
{
    assertSelect();

    if (!select_query->join())
        return false;

    /// after https://github.com/pingcap/tiflash/pull/6650, join from TiFlash client
    /// is no longer supported because the "waiting build finish" step has been moved
    /// from Join::joinBlock() to HashJoinProbeInputStream::readImpl, so before support
    /// HashJoinProbeInputStream in `ExpressionAnalyzer::appendJoin`, join is disabled.
    throw Exception("Join is not supported");
}


bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->where_expression)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where_expression->getColumnName());
    getRootActions(select_query->where_expression, only_types, false, step.actions);

    return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->group_expression_list)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->group_expression_list->children;
    for (const auto & ast : asts)
    {
        step.required_output.push_back(ast->getColumnName());
        getRootActions(ast, only_types, false, step.actions);
    }

    return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    for (const auto & aggregate_description : aggregate_descriptions)
        for (const auto & name : aggregate_description.argument_names)
            step.required_output.push_back(name);

    getActionsBeforeAggregation(select_query->select_expression_list, step.actions, only_types);

    if (select_query->having_expression)
        getActionsBeforeAggregation(select_query->having_expression, step.actions, only_types);

    if (select_query->order_expression_list)
        getActionsBeforeAggregation(select_query->order_expression_list, step.actions, only_types);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->having_expression)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->having_expression->getColumnName());
    getRootActions(select_query->having_expression, only_types, false, step.actions);

    return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select_expression_list, only_types, false, step.actions);

    for (const auto & child : select_query->select_expression_list->children)
        step.required_output.push_back(child->getColumnName());
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->order_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->order_expression_list, only_types, false, step.actions);

    ASTs asts = select_query->order_expression_list->children;
    for (const auto & i : asts)
    {
        auto * ast = typeid_cast<ASTOrderByElement *>(i.get());
        if (!ast || ast->children.empty())
            throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
        ASTPtr order_expression = ast->children.at(0);
        step.required_output.push_back(order_expression->getColumnName());
    }

    return true;
}

bool ExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->limit_by_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->limit_by_expression_list, only_types, false, step.actions);

    for (const auto & child : select_query->limit_by_expression_list->children)
        step.required_output.push_back(child->getColumnName());

    return true;
}

void ExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    NamesWithAliases result_columns;

    ASTs asts = select_query->select_expression_list->children;
    for (const auto & ast : asts)
    {
        String result_name = ast->getAliasOrColumnName();
        if (required_result_columns.empty() || required_result_columns.count(result_name))
        {
            result_columns.emplace_back(ast->getColumnName(), result_name);
            step.required_output.push_back(result_columns.back().second);
        }
    }

    step.actions->add(ExpressionAction::project(result_columns));
}


void ExpressionAnalyzer::getActionsBeforeAggregation(
    const ASTPtr & ast,
    ExpressionActionsPtr & actions,
    bool no_subqueries)
{
    auto * node = typeid_cast<ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        for (auto & argument : node->arguments->children)
            getRootActions(argument, no_subqueries, false, actions);
    else
        for (auto & child : ast->children)
            getActionsBeforeAggregation(child, actions, no_subqueries);
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool project_result)
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(source_columns);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (const auto * node = typeid_cast<const ASTExpressionList *>(ast.get()))
        asts = node->children;
    else
        asts = ASTs(1, ast);

    for (const auto & ast : asts)
    {
        std::string name = ast->getColumnName();
        std::string alias;
        if (project_result)
            alias = ast->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(ast, false, false, actions);
    }

    if (project_result)
    {
        actions->add(ExpressionAction::project(result_columns));
    }
    else
    {
        /// We will not delete the original columns.
        for (const auto & column_name_type : source_columns)
            result_names.push_back(column_name_type.name);
    }

    actions->finalize(result_names);

    return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(NamesAndTypesList());

    getRootActions(ast, true, true, actions);

    return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const
{
    for (const auto & name_and_type : aggregation_keys)
        key_names.emplace_back(name_and_type.name);

    aggregates = aggregate_descriptions;
}

void ExpressionAnalyzer::collectUsedColumns()
{
    /** Calculate which columns are required to execute the expression.
      * Then, delete all other columns from the list of available columns.
      * After execution, columns will only contain the list of columns needed to read from the table.
      */

    NameSet required;
    NameSet ignored;

    NameSet available_columns;
    for (const auto & column : source_columns)
        available_columns.insert(column.name);

    /** You also need to ignore the identifiers of the columns that are obtained by JOIN.
      * (Do not assume that they are required for reading from the "left" table).
      */
    NameSet available_joined_columns;
    collectJoinedColumns(available_joined_columns, columns_added_by_join);

    NameSet required_joined_columns;
    getRequiredSourceColumnsImpl(
        ast,
        available_columns,
        required,
        ignored,
        available_joined_columns,
        required_joined_columns);

    for (auto it = columns_added_by_join.begin(); it != columns_added_by_join.end();)
    {
        if (required_joined_columns.count(it->name))
            ++it;
        else
            columns_added_by_join.erase(it++);
    }

    /// You need to read at least one column to find the number of rows.
    if (select_query && required.empty())
        required.insert(ExpressionActions::getSmallestColumn(source_columns));

    NameSet unknown_required_source_columns = required;

    for (auto it = source_columns.begin(); it != source_columns.end();)
    {
        unknown_required_source_columns.erase(it->name);

        if (!required.count(it->name))
            source_columns.erase(it++);
        else
            ++it;
    }

    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage)
    {
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            if (storage->hasColumn(*it))
            {
                source_columns.push_back(storage->getColumn(*it));
                unknown_required_source_columns.erase(it++);
            }
            else
                ++it;
        }
    }

    if (!unknown_required_source_columns.empty())
        throw Exception(
            "Unknown identifier: " + *unknown_required_source_columns.begin(),
            ErrorCodes::UNKNOWN_IDENTIFIER);
}

void ExpressionAnalyzer::collectJoinedColumns(NameSet & joined_columns, NamesAndTypesList & joined_columns_name_type)
{
    if (!select_query)
        return;

    const ASTTablesInSelectQueryElement * node = select_query->join();

    if (!node)
        return;

    const auto & table_join = static_cast<const ASTTableJoin &>(*node->table_join);
    const auto & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);

    Block nested_result_sample;
    if (table_expression.database_and_table_name)
    {
        auto database_table = getDatabaseAndTableNameFromIdentifier(
            static_cast<const ASTIdentifier &>(*table_expression.database_and_table_name));
        const auto & table = context.getTable(database_table.first, database_table.second);
        nested_result_sample = table->getSampleBlockNonMaterialized();
    }
    else if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        nested_result_sample = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context);
    }

    if (table_join.using_expression_list)
    {
        auto & keys = typeid_cast<ASTExpressionList &>(*table_join.using_expression_list);
        for (const auto & key : keys.children)
        {
            if (join_key_names_left.end()
                == std::find(join_key_names_left.begin(), join_key_names_left.end(), key->getColumnName()))
                join_key_names_left.push_back(key->getColumnName());
            else
                throw Exception(
                    "Duplicate column " + key->getColumnName() + " in USING list",
                    ErrorCodes::DUPLICATE_COLUMN);

            if (join_key_names_right.end()
                == std::find(join_key_names_right.begin(), join_key_names_right.end(), key->getAliasOrColumnName()))
                join_key_names_right.push_back(key->getAliasOrColumnName());
            else
                throw Exception(
                    "Duplicate column " + key->getAliasOrColumnName() + " in USING list",
                    ErrorCodes::DUPLICATE_COLUMN);
        }
    }

    for (const auto i : ext::range(0, nested_result_sample.columns()))
    {
        const auto & col = nested_result_sample.safeGetByPosition(i);
        if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), col.name)
            && !joined_columns.count(col.name)) /// Duplicate columns in the subquery for JOIN do not make sense.
        {
            joined_columns.insert(col.name);

            bool make_nullable = table_join.kind == ASTTableJoin::Kind::LeftOuter
                || table_join.kind == ASTTableJoin::Kind::Cross_LeftOuter
                || table_join.kind == ASTTableJoin::Kind::Full;
            joined_columns_name_type.emplace_back(col.name, make_nullable ? makeNullable(col.type) : col.type);
        }
    }
}


Names ExpressionAnalyzer::getRequiredSourceColumns() const
{
    return source_columns.getNames();
}


void ExpressionAnalyzer::getRequiredSourceColumnsImpl(
    const ASTPtr & ast,
    const NameSet & available_columns,
    NameSet & required_source_columns,
    NameSet & ignored_names,
    const NameSet & available_joined_columns,
    NameSet & required_joined_columns)
{
    /** Find all the identifiers in the query.
      * We will use depth first search in AST.
      * In this case
      * - for lambda functions we will not take formal parameters;
      * - do not go into subqueries (they have their own identifiers);
      */

    if (auto * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        if (node->kind == ASTIdentifier::Column && !ignored_names.count(node->name)
            && !ignored_names.count(Nested::extractTableName(node->name)))
        {
            if (!available_joined_columns.count(node->name)
                || available_columns.count(node->name)) /// Read column from left table if has.
                required_source_columns.insert(node->name);
            else
                required_joined_columns.insert(node->name);
        }

        return;
    }

    if (auto * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->name == "lambda")
        {
            if (node->arguments->children.size() != 2)
                throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            auto * lambda_args_tuple = typeid_cast<ASTFunction *>(node->arguments->children.at(0).get());

            if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

            /// You do not need to add formal parameters of the lambda expression in required_source_columns.
            Names added_ignored;
            for (auto & child : lambda_args_tuple->arguments->children)
            {
                auto * identifier = typeid_cast<ASTIdentifier *>(child.get());
                if (!identifier)
                    throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                String & name = identifier->name;
                if (!ignored_names.count(name))
                {
                    ignored_names.insert(name);
                    added_ignored.push_back(name);
                }
            }

            getRequiredSourceColumnsImpl(
                node->arguments->children.at(1),
                available_columns,
                required_source_columns,
                ignored_names,
                available_joined_columns,
                required_joined_columns);

            for (const auto & ignored : added_ignored)
                ignored_names.erase(ignored);

            return;
        }

        /// A special function `indexHint`. Everything that is inside it is not calculated
        /// (and is used only for index analysis, see KeyCondition).
        if (node->name == "indexHint")
            return;
    }

    /// Recursively traverses an expression.
    for (auto & child : ast->children)
    {
        if (!typeid_cast<const ASTSelectQuery *>(child.get()) && !typeid_cast<const ASTTableExpression *>(child.get()))
            getRequiredSourceColumnsImpl(
                child,
                available_columns,
                required_source_columns,
                ignored_names,
                available_joined_columns,
                required_joined_columns);
    }
}


void ExpressionAnalyzer::removeUnneededColumnsFromSelectClause()
{
    if (!select_query)
        return;

    if (required_result_columns.empty() || select_query->distinct)
        return;

    ASTs & elements = select_query->select_expression_list->children;

    elements.erase(
        std::remove_if(
            elements.begin(),
            elements.end(),
            [this](const auto & node) { return !required_result_columns.count(node->getAliasOrColumnName()); }),
        elements.end());
}

} // namespace DB
