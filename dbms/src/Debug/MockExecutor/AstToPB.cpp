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
#include <DataTypes/FieldToDataType.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/FuncSigMap.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Decode/TypeMapping.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
void literalFieldToTiPBExpr(const TiDB::ColumnInfo & ci, const Field & val_field, tipb::Expr * expr, Int32 collator_id)
{
    *(expr->mutable_field_type()) = columnInfoToFieldType(ci);
    expr->mutable_field_type()->set_collate(collator_id);
    if (!val_field.isNull())
    {
        WriteBufferFromOwnString ss;
        switch (ci.tp)
        {
        case TiDB::TypeLongLong:
        case TiDB::TypeLong:
        case TiDB::TypeShort:
        case TiDB::TypeTiny:
        case TiDB::TypeInt24:
            if (ci.hasUnsignedFlag())
            {
                expr->set_tp(tipb::ExprType::Uint64);
                UInt64 val = val_field.safeGet<UInt64>();
                encodeDAGUInt64(val, ss);
            }
            else
            {
                expr->set_tp(tipb::ExprType::Int64);
                Int64 val = val_field.safeGet<Int64>();
                encodeDAGInt64(val, ss);
            }
            break;
        case TiDB::TypeFloat:
        {
            expr->set_tp(tipb::ExprType::Float32);
            auto val = static_cast<Float32>(val_field.safeGet<Float64>());
            encodeDAGFloat32(val, ss);
            break;
        }
        case TiDB::TypeDouble:
        {
            expr->set_tp(tipb::ExprType::Float64);
            Float64 val = val_field.safeGet<Float64>();
            encodeDAGFloat64(val, ss);
            break;
        }
        case TiDB::TypeString:
        {
            expr->set_tp(tipb::ExprType::String);
            const auto & val = val_field.safeGet<String>();
            encodeDAGString(val, ss);
            break;
        }
        case TiDB::TypeNewDecimal:
        {
            expr->set_tp(tipb::ExprType::MysqlDecimal);
            encodeDAGDecimal(val_field, ss);
            break;
        }
        case TiDB::TypeDate:
        {
            expr->set_tp(tipb::ExprType::MysqlTime);
            UInt64 val = val_field.safeGet<UInt64>();
            encodeDAGUInt64(MyDate(val).toPackedUInt(), ss);
            break;
        }
        case TiDB::TypeDatetime:
        case TiDB::TypeTimestamp:
        {
            expr->set_tp(tipb::ExprType::MysqlTime);
            UInt64 val = val_field.safeGet<UInt64>();
            encodeDAGUInt64(MyDateTime(val).toPackedUInt(), ss);
            break;
        }
        case TiDB::TypeTime:
        {
            expr->set_tp(tipb::ExprType::MysqlDuration);
            Int64 val = val_field.safeGet<Int64>();
            encodeDAGInt64(val, ss);
            break;
        }
        default:
            throw Exception(fmt::format(
                "Type {} does not support literal in function unit test",
                getDataTypeByColumnInfo(ci)->getName()));
        }
        expr->set_val(ss.releaseStr());
    }
    else
    {
        expr->set_tp(tipb::ExprType::Null);
    }
}

void literalToPB(tipb::Expr * expr, const Field & value, int32_t collator_id)
{
    DataTypePtr type = applyVisitor(FieldToDataType(), value);
    TiDB::ColumnInfo ci = reverseGetColumnInfo({"", type}, 0, Field(), true);
    literalFieldToTiPBExpr(ci, value, expr, collator_id);
}

String getFunctionNameForConstantFolding(tipb::Expr * expr)
{
    // todo support more function for constant folding
    switch (expr->sig())
    {
    case tipb::ScalarFuncSig::CastStringAsTime:
        return "toMyDateTimeOrNull";
    default:
        return "";
    }
}

void foldConstant(tipb::Expr * expr, int32_t collator_id, const Context & context)
{
    if (expr->tp() == tipb::ScalarFunc)
    {
        bool all_const = true;
        for (const auto & c : expr->children())
        {
            if (!isLiteralExpr(c))
            {
                all_const = false;
                break;
            }
        }
        if (!all_const)
            return;
        DataTypes arguments_types;
        ColumnsWithTypeAndName argument_columns;
        for (const auto & c : expr->children())
        {
            Field value = decodeLiteral(c);
            DataTypePtr flash_type = applyVisitor(FieldToDataType(), value);
            DataTypePtr target_type = inferDataType4Literal(c);
            ColumnWithTypeAndName column;
            column.column
                = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
            column.name = exprToString(c, {}) + "_" + target_type->getName();
            column.type = target_type;
            arguments_types.emplace_back(target_type);
            argument_columns.emplace_back(column);
        }
        auto func_name = getFunctionNameForConstantFolding(expr);
        if (func_name.empty())
            return;
        const auto & function_builder_ptr = FunctionFactory::instance().get(func_name, context);
        auto function_ptr = function_builder_ptr->build(argument_columns);
        if (function_ptr->isSuitableForConstantFolding())
        {
            Block block_with_constants(argument_columns);
            ColumnNumbers argument_numbers(arguments_types.size());
            for (size_t i = 0, size = arguments_types.size(); i < size; i++)
                argument_numbers[i] = i;
            size_t result_pos = argument_numbers.size();
            block_with_constants.insert({nullptr, function_ptr->getReturnType(), "result"});
            function_ptr->execute(block_with_constants, argument_numbers, result_pos);
            const auto & result_column = block_with_constants.getByPosition(result_pos).column;
            if (result_column->isColumnConst())
            {
                auto updated_value = (*result_column)[0];
                tipb::FieldType orig_field_type = expr->field_type();
                expr->Clear();
                literalToPB(expr, updated_value, collator_id);
                expr->clear_field_type();
                auto * field_type = expr->mutable_field_type();
                (*field_type) = orig_field_type;
            }
        }
    }
}

void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, int32_t collator_id, const Context & context)
{
    if (auto * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        identifierToPB(input, id, expr, collator_id);
    }
    else if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        functionToPB(input, func, expr, collator_id, context);
    }
    else if (auto * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        literalToPB(expr, lit->value, collator_id);
    }
    else
    {
        throw Exception("Unsupported expression: " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
}

void functionToPB(
    const DAGSchema & input,
    ASTFunction * func,
    tipb::Expr * expr,
    int32_t collator_id,
    const Context & context)
{
    /// aggregation function is handled in AggregationBinder, so just treated as a column
    auto ft = checkSchema(input, func->getColumnName());
    if (ft != input.end())
    {
        expr->set_tp(tipb::ColumnRef);
        *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(ft - input.begin(), ss);
        expr->set_val(ss.releaseStr());
        return;
    }
    if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
    {
        throw Exception("No such column: " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }
    String func_name_lowercase = Poco::toLower(func->name);
    // TODO: Support more functions.
    // TODO: Support type inference.

    const auto it_sig = tests::func_name_to_sig.find(func_name_lowercase);
    if (it_sig == tests::func_name_to_sig.end())
    {
        throw Exception("Unsupported function: " + func_name_lowercase, ErrorCodes::LOGICAL_ERROR);
    }
    switch (it_sig->second)
    {
    case tipb::ScalarFuncSig::InInt:
    {
        tipb::Expr * in_expr = expr;
        if (func_name_lowercase == "notin")
        {
            // notin is transformed into not(in()) by tidb
            expr->set_sig(tipb::ScalarFuncSig::UnaryNotInt);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeLongLong);
            ft->set_flag(TiDB::ColumnFlagUnsigned);
            expr->set_tp(tipb::ExprType::ScalarFunc);
            in_expr = expr->add_children();
        }
        in_expr->set_sig(tipb::ScalarFuncSig::InInt);
        auto * ft = in_expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagUnsigned);
        ft->set_collate(collator_id);
        in_expr->set_tp(tipb::ExprType::ScalarFunc);
        for (const auto & child_ast : func->arguments->children)
        {
            auto * tuple_func = typeid_cast<ASTFunction *>(child_ast.get());
            if (tuple_func != nullptr && tuple_func->name == "tuple")
            {
                // flatten tuple elements
                for (const auto & c : tuple_func->arguments->children)
                {
                    tipb::Expr * child = in_expr->add_children();
                    astToPB(input, c, child, collator_id, context);
                }
            }
            else
            {
                tipb::Expr * child = in_expr->add_children();
                astToPB(input, child_ast, child, collator_id, context);
            }
        }
        return;
    }
    case tipb::ScalarFuncSig::IfInt:
    case tipb::ScalarFuncSig::BitAndSig:
    case tipb::ScalarFuncSig::BitOrSig:
    case tipb::ScalarFuncSig::BitXorSig:
    case tipb::ScalarFuncSig::BitNegSig:
        expr->set_sig(it_sig->second);
        expr->set_tp(tipb::ExprType::ScalarFunc);
        for (size_t i = 0; i < func->arguments->children.size(); i++)
        {
            const auto & child_ast = func->arguments->children[i];
            tipb::Expr * child = expr->add_children();
            astToPB(input, child_ast, child, collator_id, context);
            // todo should infer the return type based on all input types
            if ((it_sig->second == tipb::ScalarFuncSig::IfInt && i == 1)
                || (it_sig->second != tipb::ScalarFuncSig::IfInt && i == 0))
                *(expr->mutable_field_type()) = child->field_type();
        }
        return;
    case tipb::ScalarFuncSig::PlusInt:
    case tipb::ScalarFuncSig::PlusReal:
    case tipb::ScalarFuncSig::PlusDecimal:
    case tipb::ScalarFuncSig::MinusInt:
    case tipb::ScalarFuncSig::MinusReal:
    case tipb::ScalarFuncSig::MinusDecimal:
    {
        for (const auto & child_ast : func->arguments->children)
        {
            tipb::Expr * child = expr->add_children();
            astToPB(input, child_ast, child, collator_id, context);
        }
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(expr->children(0).field_type().tp());
        ft->set_flag(expr->children(0).field_type().flag());
        ft->set_collate(collator_id);
        expr->set_tp(tipb::ExprType::ScalarFunc);
        return;
    }
    case tipb::ScalarFuncSig::LikeSig:
    {
        expr->set_sig(tipb::ScalarFuncSig::LikeSig);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagUnsigned);
        ft->set_collate(collator_id);
        expr->set_tp(tipb::ExprType::ScalarFunc);
        for (const auto & child_ast : func->arguments->children)
        {
            tipb::Expr * child = expr->add_children();
            astToPB(input, child_ast, child, collator_id, context);
        }
        // for like need to add the third argument
        *expr->add_children() = constructInt64LiteralTiExpr(92);
        return;
    }
    case tipb::ScalarFuncSig::FromUnixTime2Arg:
        if (func->arguments->children.size() == 1)
        {
            expr->set_sig(tipb::ScalarFuncSig::FromUnixTime1Arg);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeDatetime);
            ft->set_decimal(6);
        }
        else
        {
            expr->set_sig(tipb::ScalarFuncSig::FromUnixTime2Arg);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeString);
        }
        break;
    case tipb::ScalarFuncSig::DateFormatSig:
        expr->set_sig(tipb::ScalarFuncSig::DateFormatSig);
        expr->mutable_field_type()->set_tp(TiDB::TypeString);
        break;
    case tipb::ScalarFuncSig::CastIntAsTime:
    case tipb::ScalarFuncSig::CastRealAsTime:
    case tipb::ScalarFuncSig::CastTimeAsTime:
    case tipb::ScalarFuncSig::CastDecimalAsTime:
    case tipb::ScalarFuncSig::CastStringAsTime:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        if (it_sig->first.find("datetime"))
        {
            ft->set_tp(TiDB::TypeDatetime);
        }
        else
        {
            ft->set_tp(TiDB::TypeDate);
        }
        break;
    }
    case tipb::ScalarFuncSig::CastIntAsReal:
    case tipb::ScalarFuncSig::CastRealAsReal:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeDouble);
        ft->set_collate(collator_id);
        break;
    }
    case tipb::ScalarFuncSig::Concat:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeString);
        ft->set_collate(collator_id);
        break;
    }
    case tipb::ScalarFuncSig::RoundInt:
    case tipb::ScalarFuncSig::RoundWithFracInt:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        if (it_sig->first.find("uint") != std::string::npos)
            ft->set_flag(TiDB::ColumnFlagUnsigned);
        ft->set_collate(collator_id);
        break;
    }
    case tipb::ScalarFuncSig::RoundDec:
    case tipb::ScalarFuncSig::RoundWithFracDec:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeNewDecimal);
        ft->set_collate(collator_id);
        break;
    }
    case tipb::ScalarFuncSig::RoundReal:
    case tipb::ScalarFuncSig::RoundWithFracReal:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeDouble);
        ft->set_collate(collator_id);
        break;
    }
    default:
    {
        expr->set_sig(it_sig->second);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagUnsigned);
        ft->set_collate(collator_id);
        break;
    }
    }
    expr->set_tp(tipb::ExprType::ScalarFunc);
    for (const auto & child_ast : func->arguments->children)
    {
        tipb::Expr * child = expr->add_children();
        astToPB(input, child_ast, child, collator_id, context);
    }
    foldConstant(expr, collator_id, context);
}

void identifierToPB(const DAGSchema & input, ASTIdentifier * id, tipb::Expr * expr, int32_t collator_id)
{
    auto ft = checkSchema(input, id->getColumnName());
    if (ft == input.end())
        throw Exception("No such column: " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    expr->set_tp(tipb::ColumnRef);
    *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);
    expr->mutable_field_type()->set_collate(collator_id);
    WriteBufferFromOwnString ss;
    encodeDAGInt64(ft - input.begin(), ss);
    expr->set_val(ss.releaseStr());
}

void collectUsedColumnsFromExpr(const DAGSchema & input, ASTPtr ast, std::unordered_set<String> & used_columns)
{
    if (auto * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto [db_name, table_name, column_name] = splitQualifiedName(id->getColumnName());
        if (!table_name.empty())
            used_columns.emplace(id->getColumnName());
        else
        {
            bool found = false;
            for (const auto & field : input)
            {
                if (splitQualifiedName(field.first).column_name == column_name)
                {
                    if (found)
                        throw Exception("ambiguous column for " + column_name);
                    found = true;
                    used_columns.emplace(field.first);
                }
            }
        }
    }
    else if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            used_columns.emplace(func->getColumnName());
        }
        else
        {
            /// check function
            auto ft = checkSchema(input, func->getColumnName());
            if (ft != input.end())
            {
                used_columns.emplace(func->getColumnName());
                return;
            }
            for (const auto & child_ast : func->arguments->children)
            {
                collectUsedColumnsFromExpr(input, child_ast, used_columns);
            }
        }
    }
}

TiDB::ColumnInfo compileIdentifier(const DAGSchema & input, ASTIdentifier * id)
{
    TiDB::ColumnInfo ci;

    /// check column
    auto ft = checkSchema(input, id->getColumnName());
    if (ft == input.end())
        throw Exception("No such column: " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    ci = ft->second;

    return ci;
}

TiDB::ColumnInfo compileFunction(const DAGSchema & input, ASTFunction * func)
{
    TiDB::ColumnInfo ci;
    /// check function
    String func_name_lowercase = Poco::toLower(func->name);
    const auto it_sig = tests::func_name_to_sig.find(func_name_lowercase);
    if (it_sig == tests::func_name_to_sig.end())
    {
        throw Exception("Unsupported function: " + func_name_lowercase, ErrorCodes::LOGICAL_ERROR);
    }
    switch (it_sig->second)
    {
    case tipb::ScalarFuncSig::InInt:
        ci.tp = TiDB::TypeLongLong;
        ci.flag = TiDB::ColumnFlagUnsigned;
        for (const auto & child_ast : func->arguments->children)
        {
            auto * tuple_func = typeid_cast<ASTFunction *>(child_ast.get());
            if (tuple_func != nullptr && tuple_func->name == "tuple")
            {
                // flatten tuple elements
                for (const auto & c : tuple_func->arguments->children)
                {
                    compileExpr(input, c);
                }
            }
            else
            {
                compileExpr(input, child_ast);
            }
        }
        return ci;
    case tipb::ScalarFuncSig::IfInt:
    case tipb::ScalarFuncSig::BitAndSig:
    case tipb::ScalarFuncSig::BitOrSig:
    case tipb::ScalarFuncSig::BitXorSig:
    case tipb::ScalarFuncSig::BitNegSig:
        for (size_t i = 0; i < func->arguments->children.size(); i++)
        {
            const auto & child_ast = func->arguments->children[i];
            auto child_ci = compileExpr(input, child_ast);
            // todo should infer the return type based on all input types
            if ((it_sig->second == tipb::ScalarFuncSig::IfInt && i == 1)
                || (it_sig->second != tipb::ScalarFuncSig::IfInt && i == 0))
                ci = child_ci;
        }
        return ci;
    case tipb::ScalarFuncSig::PlusInt:
    case tipb::ScalarFuncSig::MinusInt:
        return compileExpr(input, func->arguments->children[0]);
    case tipb::ScalarFuncSig::LikeSig:
        ci.tp = TiDB::TypeLongLong;
        ci.flag = TiDB::ColumnFlagUnsigned;
        for (const auto & child_ast : func->arguments->children)
        {
            compileExpr(input, child_ast);
        }
        return ci;
    case tipb::ScalarFuncSig::FromUnixTime2Arg:
        if (func->arguments->children.size() == 1)
        {
            ci.tp = TiDB::TypeDatetime;
            ci.decimal = 6;
        }
        else
        {
            ci.tp = TiDB::TypeString;
        }
        break;
    case tipb::ScalarFuncSig::DateFormatSig:
        ci.tp = TiDB::TypeString;
        break;
    case tipb::ScalarFuncSig::CastIntAsTime:
    case tipb::ScalarFuncSig::CastRealAsTime:
    case tipb::ScalarFuncSig::CastTimeAsTime:
    case tipb::ScalarFuncSig::CastDecimalAsTime:
    case tipb::ScalarFuncSig::CastStringAsTime:
        if (it_sig->first.find("datetime"))
        {
            ci.tp = TiDB::TypeDatetime;
        }
        else
        {
            ci.tp = TiDB::TypeDate;
        }
        break;
    case tipb::ScalarFuncSig::CastIntAsReal:
    case tipb::ScalarFuncSig::CastRealAsReal:
    {
        ci.tp = TiDB::TypeDouble;
        break;
    }
    case tipb::ScalarFuncSig::RoundInt:
    case tipb::ScalarFuncSig::RoundWithFracInt:
    {
        ci.tp = TiDB::TypeLongLong;
        if (it_sig->first.find("uint") != std::string::npos)
            ci.flag = TiDB::ColumnFlagUnsigned;
        break;
    }
    case tipb::ScalarFuncSig::RoundDec:
    case tipb::ScalarFuncSig::RoundWithFracDec:
    {
        ci.tp = TiDB::TypeNewDecimal;
        break;
    }
    case tipb::ScalarFuncSig::RoundReal:
    case tipb::ScalarFuncSig::RoundWithFracReal:
    {
        ci.tp = TiDB::TypeDouble;
        break;
    }
    default:
        ci.tp = TiDB::TypeLongLong;
        ci.flag = TiDB::ColumnFlagUnsigned;
        break;
    }
    for (const auto & child_ast : func->arguments->children)
    {
        compileExpr(input, child_ast);
    }
    return ci;
}

TiDB::ColumnInfo compileLiteral(ASTLiteral * lit)
{
    TiDB::ColumnInfo ci;
    switch (lit->value.getType())
    {
    case Field::Types::Which::Null:
        ci.tp = TiDB::TypeNull;
        // Null literal expr doesn't need value.
        break;
    case Field::Types::Which::UInt64:
        ci.tp = TiDB::TypeLongLong;
        ci.flag = TiDB::ColumnFlagUnsigned;
        break;
    case Field::Types::Which::Int64:
        ci.tp = TiDB::TypeLongLong;
        break;
    case Field::Types::Which::Float64:
        ci.tp = TiDB::TypeDouble;
        break;
    case Field::Types::Which::Decimal32:
    case Field::Types::Which::Decimal64:
    case Field::Types::Which::Decimal128:
    case Field::Types::Which::Decimal256:
        ci.tp = TiDB::TypeNewDecimal;
        break;
    case Field::Types::Which::String:
        ci.tp = TiDB::TypeString;
        break;
    default:
        throw Exception(String("Unsupported literal type: ") + lit->value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
    }
    return ci;
}

TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast)
{
    if (auto * id = typeid_cast<ASTIdentifier *>(ast.get()))
        return compileIdentifier(input, id);
    else if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
        return compileFunction(input, func);
    else if (auto * lit = typeid_cast<ASTLiteral *>(ast.get()))
        return compileLiteral(lit);
    else
    {
        /// not supported unless this is a literal
        throw Exception("Unsupported expression: " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
}

void compileFilter(const DAGSchema & input, ASTPtr ast, ASTs & conditions)
{
    if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (func->name == "and")
        {
            for (auto & child : func->arguments->children)
            {
                compileFilter(input, child, conditions);
            }
            return;
        }
    }
    conditions.push_back(ast);
    compileExpr(input, ast);
}

void fillTaskMetaWithMPPInfo(mpp::TaskMeta & meta, const MPPInfo & mpp_info)
{
    meta.set_start_ts(mpp_info.start_ts);
    meta.set_gather_id(mpp_info.gather_id);
    meta.set_query_ts(mpp_info.query_ts);
    meta.set_local_query_id(mpp_info.local_query_id);
    meta.set_server_id(mpp_info.server_id);
}

} // namespace DB
