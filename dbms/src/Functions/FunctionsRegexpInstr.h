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

#pragma once

#include <Functions/FunctionsRegexpCommon.h>

namespace DB
{
extern template class ParamInt<true>;
extern template class ParamInt<false>;

extern template class ParamString<true>;
extern template class ParamString<false>;

extern template class Param<ParamString<true>, false>;
extern template class Param<ParamString<false>, true>;
extern template class Param<ParamString<true>, true>;
extern template class Param<ParamString<false>, false>;
extern template class Param<ParamInt<true>, false>;
extern template class Param<ParamInt<false>, true>;
extern template class Param<ParamInt<true>, true>;
extern template class Param<ParamInt<false>, false>;

#define EXECUTE_REGEXP_INSTR()                 \
    do                                         \
    {                                          \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(       \
            RES_ARG_VAR_NAME,                  \
            *(EXPR_PARAM_PTR_VAR_NAME),        \
            *(PAT_PARAM_PTR_VAR_NAME),         \
            *(POS_PARAM_PTR_VAR_NAME),         \
            *(OCCUR_PARAM_PTR_VAR_NAME),       \
            *(RET_OP_PARAM_PTR_VAR_NAME),      \
            *(MATCH_TYPE_PARAM_PTR_VAR_NAME)); \
    } while (0);

// Method to get actual match type param
#define GET_MATCH_TYPE_ACTUAL_PARAM()                                                                              \
    do                                                                                                             \
    {                                                                                                              \
        GET_ACTUAL_STRING_PARAM(MATCH_TYPE_PV_VAR_NAME, MATCH_TYPE_PARAM_PTR_VAR_NAME, ({EXECUTE_REGEXP_INSTR()})) \
    } while (0);

// Method to get actual return option param
#define GET_RET_OP_ACTUAL_PARAM()                                                                              \
    do                                                                                                         \
    {                                                                                                          \
        GET_ACTUAL_INT_PARAM(RET_OP_PV_VAR_NAME, RET_OP_PARAM_PTR_VAR_NAME, ({GET_MATCH_TYPE_ACTUAL_PARAM()})) \
    } while (0);

// Method to get actual occur param
#define GET_OCCUR_ACTUAL_PARAM()                                                                         \
    do                                                                                                   \
    {                                                                                                    \
        GET_ACTUAL_INT_PARAM(OCCUR_PV_VAR_NAME, OCCUR_PARAM_PTR_VAR_NAME, ({GET_RET_OP_ACTUAL_PARAM()})) \
    } while (0);

// Method to get actual position param
#define GET_POS_ACTUAL_PARAM()                                                                      \
    do                                                                                              \
    {                                                                                               \
        GET_ACTUAL_INT_PARAM(POS_PV_VAR_NAME, POS_PARAM_PTR_VAR_NAME, ({GET_OCCUR_ACTUAL_PARAM()})) \
    } while (0);

// Method to get actual pattern param
#define GET_PAT_ACTUAL_PARAM()                                                                       \
    do                                                                                               \
    {                                                                                                \
        GET_ACTUAL_STRING_PARAM(PAT_PV_VAR_NAME, PAT_PARAM_PTR_VAR_NAME, ({GET_POS_ACTUAL_PARAM()})) \
    } while (0);

// Method to get actual expression param
#define GET_EXPR_ACTUAL_PARAM()                                                                        \
    do                                                                                                 \
    {                                                                                                  \
        GET_ACTUAL_STRING_PARAM(EXPR_PV_VAR_NAME, EXPR_PARAM_PTR_VAR_NAME, ({GET_PAT_ACTUAL_PARAM()})) \
    } while (0);

// The entry to get actual params and execute regexp functions
#define GET_ACTUAL_PARAMS_AND_EXECUTE() \
    do                                  \
    {                                   \
        GET_EXPR_ACTUAL_PARAM()         \
    } while (0);

// Implementation of regexp_instr function
template <typename Name>
class FunctionStringRegexpInstr
    : public FunctionStringRegexpBase
    , public IFunction
{
public:
    using ResultType = Int64;
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringRegexpInstr>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t arg_num = arguments.size();
        if (arg_num < REGEXP_MIN_PARAM_NUM)
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        else if (arg_num > REGEXP_INSTR_MAX_PARAM_NUM)
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

        bool has_nullable_col = false;
        bool has_data_type_nothing = false;
        bool is_str_arg;

        // Check type of arguments
        for (size_t i = 0; i < arg_num; ++i)
        {
            // Index at 0, 1 and 5 arguments should be string type, otherwise int type.
            is_str_arg = (i <= 1 || i == 5);
            checkInputArg(arguments[i], is_str_arg, &has_nullable_col, &has_data_type_nothing);
        }

        if (has_data_type_nothing)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

        if (has_nullable_col)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ResultType>>());
        else
            return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename ExprT, typename PatT, typename PosT, typename OccurT, typename RetOpT, typename MatchTypeT>
    void REGEXP_CLASS_MEM_FUNC_IMPL_NAME(
        ColumnWithTypeAndName & res_arg,
        const ExprT & expr_param,
        const PatT & pat_param,
        const PosT & pos_param,
        const OccurT & occur_param,
        const RetOpT & ret_op_param,
        const MatchTypeT & match_type_param) const
    {
        size_t col_size = expr_param.getDataNum();

        // Get function pointers to process the specific int type
        GetIntFuncPointerType get_pos_func = FunctionsRegexp::getGetIntFuncPointer(pos_param.getIntType());
        GetIntFuncPointerType get_occur_func = FunctionsRegexp::getGetIntFuncPointer(occur_param.getIntType());
        GetIntFuncPointerType get_ret_op_func = FunctionsRegexp::getGetIntFuncPointer(ret_op_param.getIntType());

        // Container will not be used when parm is const
        const void * pos_container = pos_param.getContainer();
        const void * occur_container = occur_param.getContainer();
        const void * ret_op_container = ret_op_param.getContainer();

        // Const value will not be used when param is not const
        Int64 pos_const_val = PosT::isConst() ? pos_param.template getInt<Int64>(0) : -1;
        Int64 occur_const_val = OccurT::isConst() ? occur_param.template getInt<Int64>(0) : -1;
        Int64 ret_op_const_val = RetOpT::isConst() ? ret_op_param.template getInt<Int64>(0) : -1;

        // Check if args are all const columns
        if constexpr (
            ExprT::isConst() && PatT::isConst() && PosT::isConst() && OccurT::isConst() && RetOpT::isConst()
            && MatchTypeT::isConst())
        {
            if (expr_param.isNullAt(0) || pat_param.isNullAt(0) || pos_param.isNullAt(0) || occur_param.isNullAt(0)
                || ret_op_param.isNullAt(0) || match_type_param.isNullAt(0))
            {
                res_arg.column = res_arg.type->createColumnConst(col_size, Null());
                return;
            }

            int flags = FunctionsRegexp::getDefaultFlags();
            String expr = expr_param.getString(0);
            String match_type = match_type_param.getString(0);
            String pat = pat_param.getString(0);
            if (unlikely(pat.empty()))
                throw Exception(EMPTY_PAT_ERR_MSG);

            pat = fmt::format("({})", pat);
            Regexps::Regexp regexp(FunctionsRegexp::addMatchTypeForPattern(pat, match_type, collator), flags);
            ResultType res = regexp.instr(expr.c_str(), expr.size(), pos_const_val, occur_const_val, ret_op_const_val);
            res_arg.column = res_arg.type->createColumnConst(col_size, toField(res));
            return;
        }

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        ResultType default_val = 0;
        vec_res.assign(col_size, default_val);

        constexpr bool has_nullable_col = ExprT::isNullableCol() || PatT::isNullableCol() || PosT::isNullableCol()
            || OccurT::isNullableCol() || RetOpT::isNullableCol() || MatchTypeT::isNullableCol();

#define GET_POS_VALUE(idx)                          \
    do                                              \
    {                                               \
        if constexpr (PosT::isConst())              \
            pos = pos_const_val;                    \
        else                                        \
            pos = get_pos_func(pos_container, idx); \
    } while (0);

#define GET_OCCUR_VALUE(idx)                              \
    do                                                    \
    {                                                     \
        if constexpr (OccurT::isConst())                  \
            occur = occur_const_val;                      \
        else                                              \
            occur = get_occur_func(occur_container, idx); \
    } while (0);

#define GET_RET_OP_VALUE(idx)                                \
    do                                                       \
    {                                                        \
        if constexpr (RetOpT::isConst())                     \
            ret_op = ret_op_const_val;                       \
        else                                                 \
            ret_op = get_ret_op_func(ret_op_container, idx); \
    } while (0);

        StringRef expr_ref;
        String pat;
        Int64 pos;
        Int64 occur;
        Int64 ret_op;
        String match_type;

        // Start to execute instr
        if (canMemorize<PatT, MatchTypeT>())
        {
            std::unique_ptr<Regexps::Regexp> regexp;
            if (col_size > 0)
            {
                regexp = memorize<true>(pat_param, match_type_param, collator);
                if (regexp == nullptr)
                {
                    auto nullmap_col = ColumnUInt8::create();
                    typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                    UInt8 default_val = 1;
                    nullmap.assign(col_size, default_val);
                    res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
                    return;
                }
            }

            if constexpr (has_nullable_col)
            {
                // Process nullable columns with memorized regexp
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & null_map = nullmap_col->getData();
                null_map.resize(col_size);

                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i) || pos_param.isNullAt(i) || occur_param.isNullAt(i)
                        || ret_op_param.isNullAt(i))
                    {
                        null_map[i] = 1;
                        continue;
                    }
                    null_map[i] = 0;
                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    vec_res[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }
                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                // Process pure vector columns with memorized regexp.
                // columns are impossible to be a nullable column here.

                for (size_t i = 0; i < col_size; ++i)
                {
                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    vec_res[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }
                res_arg.column = std::move(col_res);
            }
        }
        else
        {
            // Codes in this if branch execute instr without memorized regexp

            if constexpr (has_nullable_col)
            {
                // Process nullable columns without memorized regexp
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & null_map = nullmap_col->getData();
                null_map.resize(col_size);

                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i) || pat_param.isNullAt(i) || pos_param.isNullAt(i)
                        || occur_param.isNullAt(i) || ret_op_param.isNullAt(i) || match_type_param.isNullAt(i))
                    {
                        null_map[i] = 1;
                        continue;
                    }
                    null_map[i] = 0;
                    expr_param.getStringRef(i, expr_ref);
                    pat = pat_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);
                    pat = fmt::format("({})", pat);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    match_type = match_type_param.getString(i);
                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp.instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }

                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                // Process pure vector columns without memorized regexp
                for (size_t i = 0; i < col_size; ++i)
                {
                    expr_param.getStringRef(i, expr_ref);
                    pat = pat_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);
                    pat = fmt::format("({})", pat);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    match_type = match_type_param.getString(i);
                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp.instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }

                res_arg.column = std::move(col_res);
            }
        }

#undef GET_RET_OP_VALUE
#undef GET_OCCUR_VALUE
#undef GET_POS_VALUE
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        // Do something related with nullable columns
        NullPresence null_presence = getNullPresense(block, arguments);

        if (null_presence.has_null_constant)
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & col_expr = block.getByPosition(arguments[0]).column;
        const ColumnPtr & col_pat = block.getByPosition(arguments[1]).column;

        size_t arg_num = arguments.size();
        auto & RES_ARG_VAR_NAME = block.getByPosition(result);

        ColumnPtr col_pos;
        ColumnPtr col_occur;
        ColumnPtr col_return_option;
        ColumnPtr col_match_type;

        // Go through cases to get arguments
        switch (arg_num)
        {
        case REGEXP_INSTR_MAX_PARAM_NUM:
            col_match_type = block.getByPosition(arguments[5]).column;
        case REGEXP_INSTR_MAX_PARAM_NUM - 1:
            col_return_option = block.getByPosition(arguments[4]).column;
        case REGEXP_INSTR_MAX_PARAM_NUM - 2:
            col_occur = block.getByPosition(arguments[3]).column;
        case REGEXP_INSTR_MAX_PARAM_NUM - 3:
            col_pos = block.getByPosition(arguments[2]).column;
        };

        size_t col_size = col_expr->size();

        ParamVariant EXPR_PV_VAR_NAME(col_expr, col_size, StringRef("", 0));
        ParamVariant PAT_PV_VAR_NAME(col_pat, col_size, StringRef("", 0));
        ParamVariant POS_PV_VAR_NAME(col_pos, col_size, 1);
        ParamVariant OCCUR_PV_VAR_NAME(col_occur, col_size, 1);
        ParamVariant RET_OP_PV_VAR_NAME(col_return_option, col_size, 0);
        ParamVariant MATCH_TYPE_PV_VAR_NAME(col_match_type, col_size, StringRef("", 0));

        GET_ACTUAL_PARAMS_AND_EXECUTE()
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;
};

#undef GET_ACTUAL_PARAMS_AND_EXECUTE
#undef GET_EXPR_ACTUAL_PARAM
#undef GET_PAT_ACTUAL_PARAM
#undef GET_POS_ACTUAL_PARAM
#undef GET_OCCUR_ACTUAL_PARAM
#undef GET_RET_OP_ACTUAL_PARAM
#undef GET_MATCH_TYPE_ACTUAL_PARAM
#undef EXECUTE_REGEXP_INSTR
} // namespace DB

#include <Functions/FunctionsRegexpUndef.h>
