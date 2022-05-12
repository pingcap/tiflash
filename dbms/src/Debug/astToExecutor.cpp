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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <DataTypes/FieldToDataType.h>
#include <Debug/astToExecutor.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/StringTokenizer.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
std::unordered_map<String, tipb::ScalarFuncSig> func_name_to_sig({
    {"equals", tipb::ScalarFuncSig::EQInt},
    {"notEquals", tipb::ScalarFuncSig::NEInt},
    {"and", tipb::ScalarFuncSig::LogicalAnd},
    {"or", tipb::ScalarFuncSig::LogicalOr},
    {"xor", tipb::ScalarFuncSig::LogicalXor},
    {"not", tipb::ScalarFuncSig::UnaryNotInt},
    {"greater", tipb::ScalarFuncSig::GTInt},
    {"greaterorequals", tipb::ScalarFuncSig::GEInt},
    {"less", tipb::ScalarFuncSig::LTInt},
    {"lessorequals", tipb::ScalarFuncSig::LEInt},
    {"in", tipb::ScalarFuncSig::InInt},
    {"notin", tipb::ScalarFuncSig::InInt},
    {"date_format", tipb::ScalarFuncSig::DateFormatSig},
    {"if", tipb::ScalarFuncSig::IfInt},
    {"from_unixtime", tipb::ScalarFuncSig::FromUnixTime2Arg},
    /// bit_and/bit_or/bit_xor is aggregated function in clickhouse/mysql
    {"bitand", tipb::ScalarFuncSig::BitAndSig},
    {"bitor", tipb::ScalarFuncSig::BitOrSig},
    {"bitxor", tipb::ScalarFuncSig::BitXorSig},
    {"bitnot", tipb::ScalarFuncSig::BitNegSig},
    {"notequals", tipb::ScalarFuncSig::NEInt},
    {"like", tipb::ScalarFuncSig::LikeSig},
    {"cast_int_int", tipb::ScalarFuncSig::CastIntAsInt},
    {"cast_int_real", tipb::ScalarFuncSig::CastIntAsReal},
    {"cast_real_int", tipb::ScalarFuncSig::CastRealAsInt},
    {"cast_real_real", tipb::ScalarFuncSig::CastRealAsReal},
    {"cast_decimal_int", tipb::ScalarFuncSig::CastDecimalAsInt},
    {"cast_time_int", tipb::ScalarFuncSig::CastTimeAsInt},
    {"cast_string_int", tipb::ScalarFuncSig::CastStringAsInt},
    {"cast_int_decimal", tipb::ScalarFuncSig::CastIntAsDecimal},
    {"cast_real_decimal", tipb::ScalarFuncSig::CastRealAsDecimal},
    {"cast_decimal_decimal", tipb::ScalarFuncSig::CastDecimalAsDecimal},
    {"cast_time_decimal", tipb::ScalarFuncSig::CastTimeAsDecimal},
    {"cast_string_decimal", tipb::ScalarFuncSig::CastStringAsDecimal},
    {"cast_int_string", tipb::ScalarFuncSig::CastIntAsString},
    {"cast_real_string", tipb::ScalarFuncSig::CastRealAsString},
    {"cast_decimal_string", tipb::ScalarFuncSig::CastDecimalAsString},
    {"cast_time_string", tipb::ScalarFuncSig::CastTimeAsString},
    {"cast_string_string", tipb::ScalarFuncSig::CastStringAsString},
    {"cast_int_date", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_date", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_date", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_date", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_date", tipb::ScalarFuncSig::CastStringAsTime},
    {"cast_int_datetime", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_datetime", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_datetime", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_datetime", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_datetime", tipb::ScalarFuncSig::CastStringAsTime},
    {"round_int", tipb::ScalarFuncSig::RoundInt},
    {"round_uint", tipb::ScalarFuncSig::RoundInt},
    {"round_dec", tipb::ScalarFuncSig::RoundDec},
    {"round_real", tipb::ScalarFuncSig::RoundReal},
    {"round_with_frac_int", tipb::ScalarFuncSig::RoundWithFracInt},
    {"round_with_frac_uint", tipb::ScalarFuncSig::RoundWithFracInt},
    {"round_with_frac_dec", tipb::ScalarFuncSig::RoundWithFracDec},
    {"round_with_frac_real", tipb::ScalarFuncSig::RoundWithFracReal},
});

std::unordered_map<String, tipb::ExprType> agg_func_name_to_sig({
    {"min", tipb::ExprType::Min},
    {"max", tipb::ExprType::Max},
    {"count", tipb::ExprType::Count},
    {"sum", tipb::ExprType::Sum},
    {"first_row", tipb::ExprType::First},
    {"uniqRawRes", tipb::ExprType::ApproxCountDistinct},
    {"group_concat", tipb::ExprType::GroupConcat},
});

DAGColumnInfo toNullableDAGColumnInfo(const DAGColumnInfo & input)
{
    DAGColumnInfo output = input;
    output.second.clearNotNullFlag();
    return output;
}

void literalToPB(tipb::Expr * expr, const Field & value, uint32_t collator_id)
{
    WriteBufferFromOwnString ss;
    switch (value.getType())
    {
    case Field::Types::Which::Null:
    {
        expr->set_tp(tipb::Null);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeNull);
        ft->set_collate(collator_id);
        // Null literal expr doesn't need value.
        break;
    }
    case Field::Types::Which::UInt64:
    {
        expr->set_tp(tipb::Uint64);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
        ft->set_collate(collator_id);
        encodeDAGUInt64(value.get<UInt64>(), ss);
        break;
    }
    case Field::Types::Which::Int64:
    {
        expr->set_tp(tipb::Int64);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagNotNull);
        ft->set_collate(collator_id);
        encodeDAGInt64(value.get<Int64>(), ss);
        break;
    }
    case Field::Types::Which::Float64:
    {
        expr->set_tp(tipb::Float64);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeFloat);
        ft->set_flag(TiDB::ColumnFlagNotNull);
        ft->set_collate(collator_id);
        encodeDAGFloat64(value.get<Float64>(), ss);
        break;
    }
    case Field::Types::Which::Decimal32:
    case Field::Types::Which::Decimal64:
    case Field::Types::Which::Decimal128:
    case Field::Types::Which::Decimal256:
    {
        expr->set_tp(tipb::MysqlDecimal);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeNewDecimal);
        ft->set_flag(TiDB::ColumnFlagNotNull);
        ft->set_collate(collator_id);
        encodeDAGDecimal(value, ss);
        break;
    }
    case Field::Types::Which::String:
    {
        expr->set_tp(tipb::String);
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeString);
        ft->set_flag(TiDB::ColumnFlagNotNull);
        ft->set_collate(collator_id);
        // TODO: Align with TiDB.
        encodeDAGBytes(value.get<String>(), ss);
        break;
    }
    default:
        throw Exception(String("Unsupported literal type: ") + value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
    }
    expr->set_val(ss.releaseStr());
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


void foldConstant(tipb::Expr * expr, uint32_t collator_id, const Context & context)
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
            column.column = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
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

void functionToPB(const DAGSchema & input, ASTFunction * func, tipb::Expr * expr, uint32_t collator_id, const Context & context);

void identifierToPB(const DAGSchema & input, ASTIdentifier * id, tipb::Expr * expr, uint32_t collator_id);


void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, uint32_t collator_id, const Context & context)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        identifierToPB(input, id, expr, collator_id);
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        functionToPB(input, func, expr, collator_id, context);
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        literalToPB(expr, lit->value, collator_id);
    }
    else
    {
        throw Exception("Unsupported expression " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
}

void functionToPB(const DAGSchema & input, ASTFunction * func, tipb::Expr * expr, uint32_t collator_id, const Context & context)
{
    /// aggregation function is handled in Aggregation, so just treated as a column
    auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
        auto column_name = splitQualifiedName(func->getColumnName());
        auto field_name = splitQualifiedName(field.first);
        if (column_name.first.empty())
            return field_name.second == column_name.second;
        else
            return field_name.first == column_name.first && field_name.second == column_name.second;
    });
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
        throw Exception("No such column " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }
    String func_name_lowercase = Poco::toLower(func->name);
    // TODO: Support more functions.
    // TODO: Support type inference.

    const auto it_sig = func_name_to_sig.find(func_name_lowercase);
    if (it_sig == func_name_to_sig.end())
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

void identifierToPB(const DAGSchema & input, ASTIdentifier * id, tipb::Expr * expr, uint32_t collator_id)
{
    auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
        auto column_name = splitQualifiedName(id->getColumnName());
        auto field_name = splitQualifiedName(field.first);
        if (column_name.first.empty())
            return field_name.second == column_name.second;
        else
            return field_name.first == column_name.first && field_name.second == column_name.second;
    });
    if (ft == input.end())
        throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    expr->set_tp(tipb::ColumnRef);
    *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);
    expr->mutable_field_type()->set_collate(collator_id);
    WriteBufferFromOwnString ss;
    encodeDAGInt64(ft - input.begin(), ss);
    expr->set_val(ss.releaseStr());
}

void collectUsedColumnsFromExpr(const DAGSchema & input, ASTPtr ast, std::unordered_set<String> & used_columns)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto column_name = splitQualifiedName(id->getColumnName());
        if (!column_name.first.empty())
            used_columns.emplace(id->getColumnName());
        else
        {
            bool found = false;
            for (const auto & field : input)
            {
                auto field_name = splitQualifiedName(field.first);
                if (field_name.second == column_name.second)
                {
                    if (found)
                        throw Exception("ambiguous column for " + column_name.second);
                    found = true;
                    used_columns.emplace(field.first);
                }
            }
        }
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            used_columns.emplace(func->getColumnName());
        }
        else
        {
            /// check function
            auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
                auto column_name = splitQualifiedName(func->getColumnName());
                auto field_name = splitQualifiedName(field.first);
                if (column_name.first.empty())
                    return field_name.second == column_name.second;
                else
                    return field_name.first == column_name.first && field_name.second == column_name.second;
            });
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

TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast)
{
    TiDB::ColumnInfo ci;
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        /// check column
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
            auto column_name = splitQualifiedName(id->getColumnName());
            auto field_name = splitQualifiedName(field.first);
            if (column_name.first.empty())
                return field_name.second == column_name.second;
            else
                return field_name.first == column_name.first && field_name.second == column_name.second;
        });
        if (ft == input.end())
            throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        ci = ft->second;
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        /// check function
        String func_name_lowercase = Poco::toLower(func->name);
        const auto it_sig = func_name_to_sig.find(func_name_lowercase);
        if (it_sig == func_name_to_sig.end())
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
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
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
    }
    else
    {
        /// not supported unless this is a literal
        throw Exception("Unsupported expression " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
    return ci;
}

void compileFilter(const DAGSchema & input, ASTPtr ast, std::vector<ASTPtr> & conditions)
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
} // namespace

namespace Debug
{
String LOCAL_HOST = "127.0.0.1:3930";
void setServiceAddr(const std::string & addr)
{
    LOCAL_HOST = addr;
}
} // namespace Debug

std::pair<String, String> splitQualifiedName(const String & s)
{
    std::pair<String, String> ret;
    Poco::StringTokenizer string_tokens(s, ".");
    if (string_tokens.count() == 1)
    {
        ret.second = s;
    }
    else if (string_tokens.count() == 2)
    {
        ret.first = string_tokens[0];
        ret.second = string_tokens[1];
    }
    else
    {
        throw Exception("Invalid identifier name");
    }
    return ret;
}

namespace mock
{
bool ExchangeSender::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExchangeSender);
    tipb_executor->set_executor_id(name);
    tipb::ExchangeSender * exchange_sender = tipb_executor->mutable_exchange_sender();
    exchange_sender->set_tp(type);
    for (auto i : partition_keys)
    {
        auto * expr = exchange_sender->add_partition_keys();
        expr->set_tp(tipb::ColumnRef);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(i, ss);
        expr->set_val(ss.releaseStr());
        auto tipb_type = TiDB::columnInfoToFieldType(output_schema[i].second);
        *expr->mutable_field_type() = tipb_type;
        tipb_type.set_collate(collator_id);
        *exchange_sender->add_types() = tipb_type;
    }
    for (auto task_id : mpp_info.sender_target_task_ids)
    {
        mpp::TaskMeta meta;
        meta.set_start_ts(mpp_info.start_ts);
        meta.set_task_id(task_id);
        meta.set_partition_id(mpp_info.partition_id);
        meta.set_address(Debug::LOCAL_HOST);
        auto * meta_string = exchange_sender->add_encoded_task_meta();
        meta.AppendToString(meta_string);
    }

    for (auto & field : output_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(collator_id);
        auto * field_type = exchange_sender->add_all_field_types();
        *field_type = tipb_type;
    }

    auto * child_executor = exchange_sender->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

bool ExchangeReceiver::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context &)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExchangeReceiver);
    tipb_executor->set_executor_id(name);
    tipb::ExchangeReceiver * exchange_receiver = tipb_executor->mutable_exchange_receiver();
    for (auto & field : output_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(collator_id);

        auto * field_type = exchange_receiver->add_field_types();
        *field_type = tipb_type;
    }
    auto it = mpp_info.receiver_source_task_ids_map.find(name);
    if (it == mpp_info.receiver_source_task_ids_map.end())
        throw Exception("Can not found mpp receiver info");
    for (size_t i = 0; i < it->second.size(); i++)
    {
        mpp::TaskMeta meta;
        meta.set_start_ts(mpp_info.start_ts);
        meta.set_task_id(it->second[i]);
        meta.set_partition_id(i);
        meta.set_address(Debug::LOCAL_HOST);
        auto * meta_string = exchange_receiver->add_encoded_task_meta();
        meta.AppendToString(meta_string);
    }
    return true;
}

void TableScan::columnPrune(std::unordered_set<String> & used_columns)
{
    output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(), [&](const auto & field) { return used_columns.count(field.first) == 0; }),
                        output_schema.end());
}
bool TableScan::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t, const MPPInfo &, const Context &)
{
    if (table_info.is_partition_table)
    {
        tipb_executor->set_tp(tipb::ExecType::TypePartitionTableScan);
        tipb_executor->set_executor_id(name);
        auto * partition_ts = tipb_executor->mutable_partition_table_scan();
        partition_ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
            setTipbColumnInfo(partition_ts->add_columns(), info);
        for (const auto & partition : table_info.partition.definitions)
            partition_ts->add_partition_ids(partition.id);
    }
    else
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTableScan);
        tipb_executor->set_executor_id(name);
        auto * ts = tipb_executor->mutable_tbl_scan();
        ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
            setTipbColumnInfo(ts->add_columns(), info);
    }
    return true;
}

bool Selection::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeSelection);
    tipb_executor->set_executor_id(name);
    auto * sel = tipb_executor->mutable_selection();
    for (auto & expr : conditions)
    {
        tipb::Expr * cond = sel->add_conditions();
        astToPB(children[0]->output_schema, expr, cond, collator_id, context);
    }
    auto * child_executor = sel->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}
void Selection::columnPrune(std::unordered_set<String> & used_columns)
{
    for (auto & expr : conditions)
        collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

bool TopN::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeTopN);
    tipb_executor->set_executor_id(name);
    tipb::TopN * topn = tipb_executor->mutable_topn();
    for (const auto & child : order_columns)
    {
        ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = topn->add_order_by();
        by->set_desc(elem->direction < 0);
        tipb::Expr * expr = by->mutable_expr();
        astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
    }
    topn->set_limit(limit);
    auto * child_executor = topn->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}
void TopN::columnPrune(std::unordered_set<String> & used_columns)
{
    for (auto & expr : order_columns)
        collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

bool Limit::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeLimit);
    tipb_executor->set_executor_id(name);
    tipb::Limit * lt = tipb_executor->mutable_limit();
    lt->set_limit(limit);
    auto * child_executor = lt->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}
void Limit::columnPrune(std::unordered_set<String> & used_columns)
{
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

bool Aggregation::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeAggregation);
    tipb_executor->set_executor_id(name);
    auto * agg = tipb_executor->mutable_aggregation();
    auto & input_schema = children[0]->output_schema;
    for (const auto & expr : agg_exprs)
    {
        const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
        if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            throw Exception("Only agg function is allowed in select for a query with aggregation", ErrorCodes::LOGICAL_ERROR);

        tipb::Expr * agg_func = agg->add_agg_func();

        for (const auto & arg : func->arguments->children)
        {
            tipb::Expr * arg_expr = agg_func->add_children();
            astToPB(input_schema, arg, arg_expr, collator_id, context);
        }
        auto agg_sig_it = agg_func_name_to_sig.find(func->name);
        if (agg_sig_it == agg_func_name_to_sig.end())
            throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
        auto agg_sig = agg_sig_it->second;
        agg_func->set_tp(agg_sig);

        if (agg_sig == tipb::ExprType::Count || agg_sig == tipb::ExprType::Sum)
        {
            auto * ft = agg_func->mutable_field_type();
            ft->set_tp(TiDB::TypeLongLong);
            ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
        }
        else if (agg_sig == tipb::ExprType::Min || agg_sig == tipb::ExprType::Max || agg_sig == tipb::ExprType::First)
        {
            if (agg_func->children_size() != 1)
                throw Exception("udaf " + func->name + " only accept 1 argument");
            auto * ft = agg_func->mutable_field_type();
            ft->set_tp(agg_func->children(0).field_type().tp());
            ft->set_decimal(agg_func->children(0).field_type().decimal());
            ft->set_flag(agg_func->children(0).field_type().flag() & (~TiDB::ColumnFlagNotNull));
            ft->set_collate(collator_id);
        }
        else if (agg_sig == tipb::ExprType::ApproxCountDistinct)
        {
            auto * ft = agg_func->mutable_field_type();
            ft->set_tp(TiDB::TypeString);
            ft->set_flag(1);
        }
        else if (agg_sig == tipb::ExprType::GroupConcat)
        {
            auto * ft = agg_func->mutable_field_type();
            ft->set_tp(TiDB::TypeString);
        }
        if (is_final_mode)
            agg_func->set_aggfuncmode(tipb::AggFunctionMode::FinalMode);
        else
            agg_func->set_aggfuncmode(tipb::AggFunctionMode::Partial1Mode);
    }

    for (const auto & child : gby_exprs)
    {
        tipb::Expr * gby = agg->add_group_by();
        astToPB(input_schema, child, gby, collator_id, context);
    }

    auto * child_executor = agg->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}
void Aggregation::columnPrune(std::unordered_set<String> & used_columns)
{
    /// output schema for partial agg is the original agg's output schema
    output_schema_for_partial_agg = output_schema;
    output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(), [&](const auto & field) { return used_columns.count(field.first) == 0; }),
                        output_schema.end());
    std::unordered_set<String> used_input_columns;
    for (auto & func : agg_exprs)
    {
        if (used_columns.find(func->getColumnName()) != used_columns.end())
        {
            const ASTFunction * agg_func = typeid_cast<const ASTFunction *>(func.get());
            if (agg_func != nullptr)
            {
                /// agg_func should not be nullptr, just double check
                for (auto & child : agg_func->arguments->children)
                    collectUsedColumnsFromExpr(children[0]->output_schema, child, used_input_columns);
            }
        }
    }
    for (auto & gby_expr : gby_exprs)
    {
        collectUsedColumnsFromExpr(children[0]->output_schema, gby_expr, used_input_columns);
    }
    children[0]->columnPrune(used_input_columns);
}
void Aggregation::toMPPSubPlan(size_t & executor_index, const DAGProperties & properties, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map)
{
    if (!is_final_mode)
    {
        children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
        return;
    }
    /// for aggregation, change aggregation to partial_aggregation => exchange_sender => exchange_receiver => final_aggregation
    // todo support avg
    if (has_uniq_raw_res)
        throw Exception("uniq raw res not supported in mpp query");
    std::shared_ptr<Aggregation> partial_agg = std::make_shared<Aggregation>(
        executor_index,
        output_schema_for_partial_agg,
        has_uniq_raw_res,
        false,
        std::move(agg_exprs),
        std::move(gby_exprs),
        false);
    partial_agg->children.push_back(children[0]);
    std::vector<size_t> partition_keys;
    size_t agg_func_num = partial_agg->agg_exprs.size();
    for (size_t i = 0; i < partial_agg->gby_exprs.size(); i++)
    {
        partition_keys.push_back(i + agg_func_num);
    }
    std::shared_ptr<ExchangeSender> exchange_sender
        = std::make_shared<ExchangeSender>(executor_index, output_schema_for_partial_agg, partition_keys.empty() ? tipb::PassThrough : tipb::Hash, partition_keys);
    exchange_sender->children.push_back(partial_agg);

    std::shared_ptr<ExchangeReceiver> exchange_receiver
        = std::make_shared<ExchangeReceiver>(executor_index, output_schema_for_partial_agg);
    exchange_map[exchange_receiver->name] = std::make_pair(exchange_receiver, exchange_sender);
    /// re-construct agg_exprs and gby_exprs in final_agg
    for (size_t i = 0; i < partial_agg->agg_exprs.size(); i++)
    {
        const ASTFunction * agg_func = typeid_cast<const ASTFunction *>(partial_agg->agg_exprs[i].get());
        ASTPtr update_agg_expr = agg_func->clone();
        auto * update_agg_func = typeid_cast<ASTFunction *>(update_agg_expr.get());
        if (agg_func->name == "count")
            update_agg_func->name = "sum";
        update_agg_func->arguments->children.clear();
        update_agg_func->arguments->children.push_back(std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[i].first));
        agg_exprs.push_back(update_agg_expr);
    }
    for (size_t i = 0; i < partial_agg->gby_exprs.size(); i++)
    {
        gby_exprs.push_back(std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[agg_func_num + i].first));
    }
    children[0] = exchange_receiver;
}

bool Project::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeProjection);
    tipb_executor->set_executor_id(name);
    auto * proj = tipb_executor->mutable_projection();
    auto & input_schema = children[0]->output_schema;
    for (const auto & child : exprs)
    {
        if (typeid_cast<ASTAsterisk *>(child.get()))
        {
            /// special case, select *
            for (size_t i = 0; i < input_schema.size(); i++)
            {
                tipb::Expr * expr = proj->add_exprs();
                expr->set_tp(tipb::ColumnRef);
                *(expr->mutable_field_type()) = columnInfoToFieldType(input_schema[i].second);
                WriteBufferFromOwnString ss;
                encodeDAGInt64(i, ss);
                expr->set_val(ss.releaseStr());
            }
            continue;
        }
        tipb::Expr * expr = proj->add_exprs();
        astToPB(input_schema, child, expr, collator_id, context);
    }
    auto * children_executor = proj->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}
void Project::columnPrune(std::unordered_set<String> & used_columns)
{
    output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(), [&](const auto & field) { return used_columns.count(field.first) == 0; }),
                        output_schema.end());
    std::unordered_set<String> used_input_columns;
    for (auto & expr : exprs)
    {
        if (typeid_cast<ASTAsterisk *>(expr.get()))
        {
            /// for select *, just add all its input columns, maybe
            /// can do some optimization, but it is not worth for mock
            /// tests
            for (auto & field : children[0]->output_schema)
            {
                used_input_columns.emplace(field.first);
            }
            break;
        }
        if (used_columns.find(expr->getColumnName()) != used_columns.end())
        {
            collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_input_columns);
        }
    }
    children[0]->columnPrune(used_input_columns);
}

void Join::columnPrune(std::unordered_set<String> & used_columns)
{
    std::unordered_set<String> left_columns;
    std::unordered_set<String> right_columns;
    for (auto & field : children[0]->output_schema)
        left_columns.emplace(field.first);
    for (auto & field : children[1]->output_schema)
        right_columns.emplace(field.first);

    std::unordered_set<String> left_used_columns;
    std::unordered_set<String> right_used_columns;
    for (const auto & s : used_columns)
    {
        if (left_columns.find(s) != left_columns.end())
            left_used_columns.emplace(s);
        else
            right_used_columns.emplace(s);
    }
    for (const auto & child : join_params.using_expression_list->children)
    {
        if (auto * identifier = typeid_cast<ASTIdentifier *>(child.get()))
        {
            auto col_name = identifier->getColumnName();
            for (auto & field : children[0]->output_schema)
            {
                if (col_name == splitQualifiedName(field.first).second)
                {
                    left_used_columns.emplace(field.first);
                    break;
                }
            }
            for (auto & field : children[1]->output_schema)
            {
                if (col_name == splitQualifiedName(field.first).second)
                {
                    right_used_columns.emplace(field.first);
                    break;
                }
            }
        }
        else
        {
            throw Exception("Only support Join on columns");
        }
    }
    children[0]->columnPrune(left_used_columns);
    children[1]->columnPrune(right_used_columns);
    output_schema.clear();
    /// update output schema
    for (auto & field : children[0]->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Right && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    for (auto & field : children[1]->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Left && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
}

void Join::fillJoinKeyAndFieldType(
    ASTPtr key,
    const DAGSchema & schema,
    tipb::Expr * tipb_key,
    tipb::FieldType * tipb_field_type,
    uint32_t collator_id)
{
    auto * identifier = typeid_cast<ASTIdentifier *>(key.get());
    for (size_t index = 0; index < schema.size(); index++)
    {
        const auto & field = schema[index];
        if (splitQualifiedName(field.first).second == identifier->getColumnName())
        {
            auto tipb_type = TiDB::columnInfoToFieldType(field.second);
            tipb_type.set_collate(collator_id);

            tipb_key->set_tp(tipb::ColumnRef);
            WriteBufferFromOwnString ss;
            encodeDAGInt64(index, ss);
            tipb_key->set_val(ss.releaseStr());
            *tipb_key->mutable_field_type() = tipb_type;

            *tipb_field_type = tipb_type;
            break;
        }
    }
}
bool Join::toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeJoin);
    tipb_executor->set_executor_id(name);
    tipb::Join * join = tipb_executor->mutable_join();
    switch (join_params.kind) // todo support more type...
    {
    case ASTTableJoin::Kind::Inner:
        join->set_join_type(tipb::JoinType::TypeInnerJoin);
        break;
    case ASTTableJoin::Kind::Left:
        join->set_join_type(tipb::JoinType::TypeLeftOuterJoin);
        break;
    case ASTTableJoin::Kind::Right:
        join->set_join_type(tipb::JoinType::TypeRightOuterJoin);
        break;
    default:
        throw Exception("Unsupported join type");
    }
    join->set_join_exec_type(tipb::JoinExecType::TypeHashJoin);
    join->set_inner_idx(1);
    for (auto & key : join_params.using_expression_list->children)
    {
        fillJoinKeyAndFieldType(key, children[0]->output_schema, join->add_left_join_keys(), join->add_probe_types(), collator_id);
        fillJoinKeyAndFieldType(key, children[1]->output_schema, join->add_right_join_keys(), join->add_build_types(), collator_id);
    }
    auto * left_child_executor = join->add_children();
    children[0]->toTiPBExecutor(left_child_executor, collator_id, mpp_info, context);
    auto * right_child_executor = join->add_children();
    return children[1]->toTiPBExecutor(right_child_executor, collator_id, mpp_info, context);
}
void Join::toMPPSubPlan(size_t & executor_index, const DAGProperties & properties, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map)
{
    if (properties.use_broadcast_join)
    {
        /// for broadcast join, always use right side as the broadcast side
        std::shared_ptr<ExchangeSender> right_exchange_sender
            = std::make_shared<ExchangeSender>(executor_index, children[1]->output_schema, tipb::Broadcast);
        right_exchange_sender->children.push_back(children[1]);

        std::shared_ptr<ExchangeReceiver> right_exchange_receiver
            = std::make_shared<ExchangeReceiver>(executor_index, children[1]->output_schema);
        children[1] = right_exchange_receiver;
        exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
        return;
    }
    std::vector<size_t> left_partition_keys;
    std::vector<size_t> right_partition_keys;
    for (auto & key : join_params.using_expression_list->children)
    {
        size_t index = 0;
        for (; index < children[0]->output_schema.size(); index++)
        {
            if (splitQualifiedName(children[0]->output_schema[index].first).second == key->getColumnName())
            {
                left_partition_keys.push_back(index);
                break;
            }
        }
        index = 0;
        for (; index < children[1]->output_schema.size(); index++)
        {
            if (splitQualifiedName(children[1]->output_schema[index].first).second == key->getColumnName())
            {
                right_partition_keys.push_back(index);
                break;
            }
        }
    }
    std::shared_ptr<ExchangeSender> left_exchange_sender
        = std::make_shared<ExchangeSender>(executor_index, children[0]->output_schema, tipb::Hash, left_partition_keys);
    left_exchange_sender->children.push_back(children[0]);
    std::shared_ptr<ExchangeSender> right_exchange_sender
        = std::make_shared<ExchangeSender>(executor_index, children[1]->output_schema, tipb::Hash, right_partition_keys);
    right_exchange_sender->children.push_back(children[1]);

    std::shared_ptr<ExchangeReceiver> left_exchange_receiver
        = std::make_shared<ExchangeReceiver>(executor_index, children[0]->output_schema);
    std::shared_ptr<ExchangeReceiver> right_exchange_receiver
        = std::make_shared<ExchangeReceiver>(executor_index, children[1]->output_schema);
    children[0] = left_exchange_receiver;
    children[1] = right_exchange_receiver;

    exchange_map[left_exchange_receiver->name] = std::make_pair(left_exchange_receiver, left_exchange_sender);
    exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
}
} // namespace mock

ExecutorPtr compileTableScan(size_t & executor_index, TableInfo & table_info, String & table_alias, bool append_pk_column)
{
    DAGSchema ts_output;
    for (const auto & column_info : table_info.columns)
    {
        ColumnInfo ci;
        ci.tp = column_info.tp;
        ci.flag = column_info.flag;
        ci.flen = column_info.flen;
        ci.decimal = column_info.decimal;
        ci.elems = column_info.elems;
        ci.default_value = column_info.default_value;
        ci.origin_default_value = column_info.origin_default_value;
        /// use qualified name as the column name to handle multiple table queries, not very
        /// efficient but functionally enough for mock test
        ts_output.emplace_back(std::make_pair(table_alias + "." + column_info.name, std::move(ci)));
    }
    if (append_pk_column)
    {
        ColumnInfo ci;
        ci.tp = TiDB::TypeLongLong;
        ci.setPriKeyFlag();
        ci.setNotNullFlag();
        ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
    }

    return std::make_shared<mock::TableScan>(executor_index, ts_output, table_info);
}

ExecutorPtr compileSelection(ExecutorPtr input, size_t & executor_index, ASTPtr filter)
{
    std::vector<ASTPtr> conditions;
    compileFilter(input->output_schema, filter, conditions);
    auto selection = std::make_shared<mock::Selection>(executor_index, input->output_schema, std::move(conditions));
    selection->children.push_back(input);
    return selection;
}

ExecutorPtr compileTopN(ExecutorPtr input, size_t & executor_index, ASTPtr order_exprs, ASTPtr limit_expr)
{
    std::vector<ASTPtr> order_columns;
    for (const auto & child : order_exprs->children)
    {
        ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        order_columns.push_back(child);
        compileExpr(input->output_schema, elem->children[0]);
    }
    auto limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto top_n = std::make_shared<mock::TopN>(executor_index, input->output_schema, std::move(order_columns), limit);
    top_n->children.push_back(input);
    return top_n;
}

ExecutorPtr compileLimit(ExecutorPtr input, size_t & executor_index, ASTPtr limit_expr)
{
    auto limit_length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto limit = std::make_shared<mock::Limit>(executor_index, input->output_schema, limit_length);
    limit->children.push_back(input);
    return limit;
}

ExecutorPtr compileAggregation(ExecutorPtr input, size_t & executor_index, ASTPtr agg_funcs, ASTPtr group_by_exprs)
{
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    DAGSchema output_schema;
    bool has_uniq_raw_res = false;
    bool need_append_project = false;
    if (agg_funcs != nullptr)
    {
        for (const auto & expr : agg_funcs->children)
        {
            const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
            if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                need_append_project = true;
                continue;
            }

            agg_exprs.push_back(expr);
            std::vector<TiDB::ColumnInfo> children_ci;

            for (const auto & arg : func->arguments->children)
            {
                children_ci.push_back(compileExpr(input->output_schema, arg));
            }

            TiDB::ColumnInfo ci;
            if (func->name == "count")
            {
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull;
            }
            else if (func->name == "max" || func->name == "min" || func->name == "first_row")
            {
                ci = children_ci[0];
                ci.flag &= ~TiDB::ColumnFlagNotNull;
            }
            else if (func->name == uniq_raw_res_name)
            {
                has_uniq_raw_res = true;
                ci.tp = TiDB::TypeString;
                ci.flag = 1;
            }
            // TODO: Other agg func.
            else
            {
                throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
            }

            output_schema.emplace_back(std::make_pair(func->getColumnName(), ci));
        }
    }

    if (group_by_exprs != nullptr)
    {
        for (const auto & child : group_by_exprs->children)
        {
            gby_exprs.push_back(child);
            auto ci = compileExpr(input->output_schema, child);
            output_schema.emplace_back(std::make_pair(child->getColumnName(), ci));
        }
    }

    auto aggregation = std::make_shared<mock::Aggregation>(
        executor_index,
        output_schema,
        has_uniq_raw_res,
        need_append_project,
        std::move(agg_exprs),
        std::move(gby_exprs),
        true);
    aggregation->children.push_back(input);
    return aggregation;
}

ExecutorPtr compileProject(ExecutorPtr input, size_t & executor_index, ASTPtr select_list)
{
    std::vector<ASTPtr> exprs;
    DAGSchema output_schema;
    for (const auto & expr : select_list->children)
    {
        if (typeid_cast<ASTAsterisk *>(expr.get()))
        {
            /// special case, select *
            exprs.push_back(expr);
            const auto & last_output = input->output_schema;
            for (const auto & field : last_output)
            {
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(field.first, field.second);
            }
        }
        else
        {
            exprs.push_back(expr);
            auto ft = std::find_if(input->output_schema.begin(), input->output_schema.end(), [&](const auto & field) { return field.first == expr->getColumnName(); });
            if (ft != input->output_schema.end())
            {
                output_schema.emplace_back(ft->first, ft->second);
                continue;
            }
            const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
            if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                throw Exception("No such agg " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
            }
            else
            {
                auto ci = compileExpr(input->output_schema, expr);
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(std::make_pair(expr->getColumnName(), ci));
            }
        }
    }

    auto project = std::make_shared<mock::Project>(executor_index, output_schema, std::move(exprs));
    project->children.push_back(input);
    return project;
}

ExecutorPtr compileJoin(size_t & executor_index, ExecutorPtr left, ExecutorPtr right, ASTPtr params)
{
    DAGSchema output_schema;
    const auto & join_params = (static_cast<const ASTTableJoin &>(*params));
    for (auto & field : left->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Right && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    for (auto & field : right->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Left && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    auto join = std::make_shared<mock::Join>(executor_index, output_schema, params);
    join->children.push_back(left);
    join->children.push_back(right);
    return join;
}

ExecutorPtr compileExchangeSender(ExecutorPtr input, size_t & executor_index, tipb::ExchangeType exchange_type)
{
    ExecutorPtr exchange_sender = std::make_shared<mock::ExchangeSender>(executor_index, input->output_schema, exchange_type);
    exchange_sender->children.push_back(input);
    return exchange_sender;
}


ExecutorPtr compileExchangeReceiver(size_t & executor_index, DAGSchema schema)
{
    ExecutorPtr exchange_receiver = std::make_shared<mock::ExchangeReceiver>(executor_index, schema);
    return exchange_receiver;
}

} // namespace DB