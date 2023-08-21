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

#define EXECUTE_REGEXP_SUBSTR()                \
    do                                         \
    {                                          \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(       \
            RES_ARG_VAR_NAME,                  \
            *(EXPR_PARAM_PTR_VAR_NAME),        \
            *(PAT_PARAM_PTR_VAR_NAME),         \
            *(POS_PARAM_PTR_VAR_NAME),         \
            *(OCCUR_PARAM_PTR_VAR_NAME),       \
            *(MATCH_TYPE_PARAM_PTR_VAR_NAME)); \
    } while (0);

// Method to get actual match type param
#define GET_MATCH_TYPE_ACTUAL_PARAM()                                                                               \
    do                                                                                                              \
    {                                                                                                               \
        GET_ACTUAL_STRING_PARAM(MATCH_TYPE_PV_VAR_NAME, MATCH_TYPE_PARAM_PTR_VAR_NAME, ({EXECUTE_REGEXP_SUBSTR()})) \
    } while (0);

// Method to get actual occur param
#define GET_OCCUR_ACTUAL_PARAM()                                                                             \
    do                                                                                                       \
    {                                                                                                        \
        GET_ACTUAL_INT_PARAM(OCCUR_PV_VAR_NAME, OCCUR_PARAM_PTR_VAR_NAME, ({GET_MATCH_TYPE_ACTUAL_PARAM()})) \
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

// Implementation of regexp_substr function
template <typename Name>
class FunctionStringRegexpSubstr
    : public FunctionStringRegexpBase
    , public IFunction
{
public:
    using ResultType = String;
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringRegexpSubstr>(); }
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
        else if (arg_num > REGEXP_SUBSTR_MAX_PARAM_NUM)
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

        bool has_nullable_col = false;
        bool has_data_type_nothing = false;
        bool is_str_arg;

        // Check type of arguments
        for (size_t i = 0; i < arg_num; ++i)
        {
            // Index at 0, 1 and 4 arguments should be string type, otherwise int type.
            is_str_arg = (i <= 1 || i == 4);
            checkInputArg(arguments[i], is_str_arg, &has_nullable_col, &has_data_type_nothing);
        }

        if (has_data_type_nothing)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    template <typename ExprT, typename PatT, typename PosT, typename OccurT, typename MatchTypeT>
    void REGEXP_CLASS_MEM_FUNC_IMPL_NAME(
        ColumnWithTypeAndName & res_arg,
        const ExprT & expr_param,
        const PatT & pat_param,
        const PosT & pos_param,
        const OccurT & occur_param,
        const MatchTypeT & match_type_param) const
    {
        size_t col_size = expr_param.getDataNum();

        // Get function pointers to process the specific int type
        GetIntFuncPointerType get_pos_func = FunctionsRegexp::getGetIntFuncPointer(pos_param.getIntType());
        GetIntFuncPointerType get_occur_func = FunctionsRegexp::getGetIntFuncPointer(occur_param.getIntType());

        // Container will not be used when parm is const
        const void * pos_container = pos_param.getContainer();
        const void * occur_container = occur_param.getContainer();

        // Const value will not be used when param is not const
        Int64 pos_const_val = PosT::isConst() ? pos_param.template getInt<Int64>(0) : -1;
        Int64 occur_const_val = OccurT::isConst() ? occur_param.template getInt<Int64>(0) : -1;

        // Check if args are all const columns
        if constexpr (
            ExprT::isConst() && PatT::isConst() && PosT::isConst() && OccurT::isConst() && MatchTypeT::isConst())
        {
            if (expr_param.isNullAt(0) || pat_param.isNullAt(0) || pos_param.isNullAt(0) || occur_param.isNullAt(0)
                || match_type_param.isNullAt(0))
            {
                res_arg.column = res_arg.type->createColumnConst(col_size, Null());
                return;
            }

            String pat = pat_param.getString(0);
            if (unlikely(pat.empty()))
                throw Exception(EMPTY_PAT_ERR_MSG);

            int flags = FunctionsRegexp::getDefaultFlags();
            String expr = expr_param.getString(0);
            String match_type = match_type_param.getString(0);
            pat = fmt::format("({})", pat);

            Regexps::Regexp regexp(FunctionsRegexp::addMatchTypeForPattern(pat, match_type, collator), flags);
            auto res = regexp.substr(expr.c_str(), expr.size(), pos_const_val, occur_const_val);
            if (res)
                res_arg.column = res_arg.type->createColumnConst(col_size, toField(res.value().toString()));
            else
                res_arg.column = res_arg.type->createColumnConst(col_size, Null());
            return;
        }

        // Initialize result column
        auto col_res = ColumnString::create();
        col_res->reserve(col_size);

        constexpr bool has_nullable_col = ExprT::isNullableCol() || PatT::isNullableCol() || PosT::isNullableCol()
            || OccurT::isNullableCol() || MatchTypeT::isNullableCol();

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

        StringRef expr_ref;
        String pat;
        Int64 pos;
        Int64 occur;
        String match_type;

        auto null_map_col = ColumnUInt8::create();
        typename ColumnUInt8::Container & null_map = null_map_col->getData();
        UInt8 default_val = 1;
        null_map.assign(col_size, default_val);

        // Start to execute substr
        if (canMemorize<PatT, MatchTypeT>())
        {
            std::unique_ptr<Regexps::Regexp> regexp;
            if (col_size > 0)
            {
                regexp = memorize<true>(pat_param, match_type_param, collator);
                if (regexp == nullptr)
                {
                    FunctionsRegexp::fillColumnStringWhenAllNull(col_res, col_size);
                    res_arg.column = ColumnNullable::create(std::move(col_res), std::move(null_map_col));
                    return;
                }
            }

            if constexpr (has_nullable_col)
            {
                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i) || pos_param.isNullAt(i) || occur_param.isNullAt(i))
                    {
                        // null_map has been set to 1 in the previous
                        col_res->insertData("", 0);
                        continue;
                    }

                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)

                    executeAndSetResult(*regexp, col_res, null_map, i, expr_ref.data, expr_ref.size, pos, occur);
                }
            }
            else
            {
                for (size_t i = 0; i < col_size; ++i)
                {
                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)

                    executeAndSetResult(*regexp, col_res, null_map, i, expr_ref.data, expr_ref.size, pos, occur);
                }
            }
        }
        else
        {
            if constexpr (has_nullable_col)
            {
                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i) || pat_param.isNullAt(i) || pos_param.isNullAt(i)
                        || occur_param.isNullAt(i) || match_type_param.isNullAt(i))
                    {
                        // null_map has been set to 1 in the previous
                        col_res->insertData("", 0);
                        continue;
                    }

                    pat = pat_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);

                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    match_type = match_type_param.getString(i);
                    pat = fmt::format("({})", pat);

                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    executeAndSetResult(regexp, col_res, null_map, i, expr_ref.data, expr_ref.size, pos, occur);
                }
            }
            else
            {
                for (size_t i = 0; i < col_size; ++i)
                {
                    pat = pat_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);

                    expr_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    match_type = match_type_param.getString(i);
                    pat = fmt::format("({})", pat);

                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    executeAndSetResult(regexp, col_res, null_map, i, expr_ref.data, expr_ref.size, pos, occur);
                }
            }
        }
#undef GET_OCCUR_VALUE
#undef GET_POS_VALUE
        res_arg.column = ColumnNullable::create(std::move(col_res), std::move(null_map_col));
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
        ColumnPtr col_match_type;

        // Go through cases to get arguments
        switch (arg_num)
        {
        case REGEXP_SUBSTR_MAX_PARAM_NUM:
            col_match_type = block.getByPosition(arguments[4]).column;
        case REGEXP_SUBSTR_MAX_PARAM_NUM - 1:
            col_occur = block.getByPosition(arguments[3]).column;
        case REGEXP_SUBSTR_MAX_PARAM_NUM - 2:
            col_pos = block.getByPosition(arguments[2]).column;
        };

        size_t col_size = col_expr->size();

        ParamVariant EXPR_PV_VAR_NAME(col_expr, col_size, StringRef("", 0));
        ParamVariant PAT_PV_VAR_NAME(col_pat, col_size, StringRef("", 0));
        ParamVariant POS_PV_VAR_NAME(col_pos, col_size, 1);
        ParamVariant OCCUR_PV_VAR_NAME(col_occur, col_size, 1);
        ParamVariant MATCH_TYPE_PV_VAR_NAME(col_match_type, col_size, StringRef("", 0));

        GET_ACTUAL_PARAMS_AND_EXECUTE()
    }

private:
    void executeAndSetResult(
        Regexps::Regexp & regexp,
        ColumnString::MutablePtr & col_res,
        typename ColumnUInt8::Container & null_map,
        size_t idx,
        const char * subject,
        size_t subject_size,
        Int64 pos,
        Int64 occur) const
    {
        auto res = regexp.substr(subject, subject_size, pos, occur);
        if (res)
        {
            col_res->insertData(res.value().data, res.value().size);
            null_map[idx] = 0;
        }
        else
        {
            // 1 has been assigned when null_map is resized
            col_res->insertData("", 0);
        }
    }

    TiDB::TiDBCollatorPtr collator = nullptr;
};

#undef GET_ACTUAL_PARAMS_AND_EXECUTE
#undef GET_EXPR_ACTUAL_PARAM
#undef GET_PAT_ACTUAL_PARAM
#undef GET_POS_ACTUAL_PARAM
#undef GET_OCCUR_ACTUAL_PARAM
#undef GET_MATCH_TYPE_ACTUAL_PARAM
#undef EXECUTE_REGEXP_SUBSTR
} // namespace DB

#include <Functions/FunctionsRegexpUndef.h>
