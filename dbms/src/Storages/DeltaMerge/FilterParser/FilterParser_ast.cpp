#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <common/logger_useful.h>
#include <tipb/expression.pb.h>

#include <cassert>


namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

namespace DM
{
namespace ast
{

String astToDebugString(const IAST * const ast)
{
    std::stringstream ss;
    ast->dumpTree(ss);
    return ss.str();
}

RSOperatorPtr
parseASTCompareFunction(const ASTFunction * const func, const FilterParser::AttrCreatorByColumnName & creator, Poco::Logger * /*log*/)
{
    if (unlikely(func->arguments->children.size() != 2))
        return createUnsupported(astToDebugString(func),
                                 func->name + " with " + DB::toString(func->arguments->children.size()) + " children is not supported",
                                 false);

    /// Only support `column` `op` `constant` now.

    Attr             attr;
    Field            value;
    UInt32           state             = 0x0;
    constexpr UInt32 state_has_column  = 0x1;
    constexpr UInt32 state_has_literal = 0x2;
    constexpr UInt32 state_finish      = state_has_column | state_has_literal;
    for (auto & child : func->arguments->children)
    {
        if (auto * id = dynamic_cast<ASTIdentifier *>(child.get()); id != nullptr && id->kind == ASTIdentifier::Column)
        {
            state |= state_has_column;
            const String & col_name = id->name;
            attr                    = creator(col_name);
        }
        else if (auto * liter = dynamic_cast<ASTLiteral *>(child.get()); liter != nullptr)
        {
            state |= state_has_literal;
            value = liter->value;
        }
    }

    // TODO: null_direction
    if (unlikely(state != state_finish))
        return createUnsupported(astToDebugString(func), func->name + " with state " + DB::toString(state) + " is not supported", false);
    else if (func->name == "equals")
        return createEqual(attr, value);
    else if (func->name == "notEquals")
        return createNotEqual(attr, value);
    else if (func->name == "greater")
        return createGreater(attr, value, -1);
    else if (func->name == "greaterOrEquals")
        return createGreaterEqual(attr, value, -1);
    else if (func->name == "less")
        return createLess(attr, value, -1);
    else if (func->name == "lessOrEquals")
        return createLessEqual(attr, value, -1);
    return createUnsupported(astToDebugString(func), "Unknown compare func: " + func->name, false);
}

RSOperatorPtr parseASTFunction(const ASTFunction * const func, const FilterParser::AttrCreatorByColumnName & creator, Poco::Logger * log)
{
    assert(func != nullptr);
    RSOperatorPtr op = EMPTY_FILTER;

    if (func->name == "equals" || func->name == "notEquals"           //
        || func->name == "greater" || func->name == "greaterOrEquals" //
        || func->name == "less" || func->name == "lessOrEquals")
    {
        op = parseASTCompareFunction(func, creator, log);
    }
    else if (func->name == "or" || func->name == "and")
    {
        RSOperators children;
        for (const auto & child : func->arguments->children)
        {
            ASTFunction * sub_func = dynamic_cast<ASTFunction *>(child.get());
            if (sub_func != nullptr)
            {
                children.emplace_back(parseASTFunction(sub_func, creator, log));
            }
            else
            {
                children.emplace_back(createUnsupported(astToDebugString(func), "child of logical operator is not function", false));
            }
        }
        if (func->name == "or")
            op = createOr(children);
        else
            op = createAnd(children);
    }
    else if (func->name == "not")
    {
        if (unlikely(func->arguments->children.size() != 1))
            op = createUnsupported(
                astToDebugString(func), "logical not with " + DB::toString(func->arguments->children.size()) + " children", false);
        else
        {
            if (ASTFunction * sub_func = dynamic_cast<ASTFunction *>(func->arguments->children[0].get()); sub_func != nullptr)
                op = createNot(parseASTFunction(sub_func, creator, log));
            else
                op = createUnsupported(astToDebugString(func), "child of logical not is not function", false);
        }
    }
#if 0
    else if (func->name == "in")
    {
    }
    else if (func->name == "notIn")
    {
    }
    else if (func->name == "like")
    {
    }
    else if (func->name == "notLike")
    {
    }
#endif
    else
    {
        op = createUnsupported(astToDebugString(func), "Function " + func->name + " is not supported", false);
    }

    return op;
}

} // namespace ast

RSOperatorPtr FilterParser::parseSelectQuery(const ASTSelectQuery & query, AttrCreatorByColumnName && creator, Poco::Logger * log)
{
    RSOperatorPtr op = EMPTY_FILTER;
    if (!query.where_expression)
        return op;

    const ASTFunction * where = dynamic_cast<ASTFunction *>(query.where_expression.get());
    if (!where)
    {
        const String debug_string = ast::astToDebugString(query.where_expression.get());
        LOG_WARNING(log, "Where expression is not ASTFunction, can not parse to rough set index. Expr: " << debug_string);
        return op;
    }

    if (log->trace())
    {
        std::string expr_tree = ast::astToDebugString(where);
        LOG_TRACE(log, " where expr: " << expr_tree);
    }

    op = ast::parseASTFunction(where, creator, log);

    return op;
}

} // namespace DM

} // namespace DB
