#include <Storages/DeltaMerge/FilterParser/FilterParser.h>

#include <cassert>

#include <common/logger_useful.h>
#include <tipb/expression.pb.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>


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

RSOperatorPtr parseRSOperator(const ASTFunction * const func, Poco::Logger * log)
{
    assert(func != nullptr);
    RSOperatorPtr op = EMPTY_FILTER;


    if (func->name == "equals")
    {
        auto * lhs = static_cast<ASTIdentifier *>(func->arguments->children[0].get());
        assert(lhs != nullptr);
        assert(lhs->kind == ASTIdentifier::Kind::Column);
        Attr   attr{lhs->name, 0, DataTypeFactory::instance().get("Int32")};
        auto * rhs = static_cast<ASTLiteral *>(func->arguments->children[1].get());
        assert(rhs != nullptr);
        op = createEqual(attr, rhs->value);
    }
    else if (func->name == "or" || func->name == "and")
    {
        RSOperators children;
        for (const auto & child : func->arguments->children)
        {
            ASTFunction * sub_func = static_cast<ASTFunction *>(child.get());
            assert(sub_func != nullptr);
            children.emplace_back(parseRSOperator(sub_func, log));
        }
        op = createOr(children);
    }
    else if (func->name == "not")
    {
        assert(func->arguments->children.size() == 1);
        ASTFunction * sub_func = static_cast<ASTFunction *>(func->arguments->children[0].get());
        assert(sub_func != nullptr);
        RSOperatorPtr sub_op = parseRSOperator(sub_func, log);
        op                   = createNot(sub_op);
    }
#if 0
    else if (func->name == "notEquals")
    {
        op = createEqual();
    }
    else if (func->name == "greater")
    {
        op = createGreater();
    }
    else if (func->name == "greaterOrEquals")
    {
        op = createGreaterEqual();
    }
    else if (func->name == "less")
    {
        op = createLess();
    }
    else if (func->name == "lessOrEquals")
    {
        op = createLessEqual();
    }
    else if (func->name == "in")
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
        std::stringstream ss;
        func->dumpTree(ss);
        op = createUnsupported(ss.str(), "Function " + func->name + " is not supported", false);
    }

    return op;
}

} // namespace ast

RSOperatorPtr FilterParser::parseSelectQuery(const ASTSelectQuery & query, Poco::Logger * log)
{
    RSOperatorPtr op = EMPTY_FILTER;
    if (!query.where_expression)
        return op;

    const ASTFunction * where = static_cast<ASTFunction *>(query.where_expression.get());
    if (!where)
    {
        std::stringstream ss;
        query.where_expression->dumpTree(ss);
        LOG_WARNING(log, String("Where expression is not ASTFunction, can not parse to rough set index. Expr: ") + ss.str());
        return op;
    }

    std::stringstream ss;
    where->dumpTree(ss);
    std::string expr_tree = ss.str();
    LOG_TRACE(log, " where expr: " << expr_tree);

    op = ast::parseRSOperator(where, log);

    return op;
}

} // namespace DM

} // namespace DB
