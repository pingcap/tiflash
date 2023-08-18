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

// regexp and regexp_like functions are executed in this macro
#define EXECUTE_REGEXP_LIKE()                  \
    do                                         \
    {                                          \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(       \
            RES_ARG_VAR_NAME,                  \
            *(EXPR_PARAM_PTR_VAR_NAME),        \
            *(PAT_PARAM_PTR_VAR_NAME),         \
            *(MATCH_TYPE_PARAM_PTR_VAR_NAME)); \
    } while (0);

// Method to get actual match type param
#define GET_MATCH_TYPE_ACTUAL_PARAM()                                                                             \
    do                                                                                                            \
    {                                                                                                             \
        GET_ACTUAL_STRING_PARAM(MATCH_TYPE_PV_VAR_NAME, MATCH_TYPE_PARAM_PTR_VAR_NAME, ({EXECUTE_REGEXP_LIKE()})) \
    } while (0);

// Method to get actual pattern param
#define GET_PAT_ACTUAL_PARAM()                                                                              \
    do                                                                                                      \
    {                                                                                                       \
        GET_ACTUAL_STRING_PARAM(PAT_PV_VAR_NAME, PAT_PARAM_PTR_VAR_NAME, ({GET_MATCH_TYPE_ACTUAL_PARAM()})) \
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

// Implementation of regexp and regexp_like functions
template <typename Name>
class FunctionStringRegexp
    : public FunctionStringRegexpBase
    , public IFunction
{
public:
    using ResultType = UInt8;
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringRegexp>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t args_max_num;
        constexpr std::string_view class_name(Name::name);

        if constexpr (class_name == regexp_like_name)
            args_max_num = REGEXP_LIKE_MAX_PARAM_NUM;
        else
            args_max_num = REGEXP_MAX_PARAM_NUM;

        size_t arg_num = arguments.size();
        if (arg_num < REGEXP_MIN_PARAM_NUM)
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        else if (arg_num > args_max_num)
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

        bool has_nullable_col = false;
        bool has_data_type_nothing = false;

        for (const auto & arg : arguments)
            checkInputArg(arg, true, &has_nullable_col, &has_data_type_nothing);

        if (has_data_type_nothing)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

        if (has_nullable_col)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ResultType>>());
        else
            return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename ExprT, typename PatT, typename MatchTypeT>
    void REGEXP_CLASS_MEM_FUNC_IMPL_NAME(
        ColumnWithTypeAndName & res_arg,
        const ExprT & expr_param,
        const PatT & pat_param,
        const MatchTypeT & match_type_param) const
    {
        size_t col_size = expr_param.getDataNum();

        // Check if args are all const columns
        if constexpr (ExprT::isConst() && PatT::isConst() && MatchTypeT::isConst())
        {
            if (expr_param.isNullAt(0) || pat_param.isNullAt(0) || match_type_param.isNullAt(0))
            {
                res_arg.column = res_arg.type->createColumnConst(col_size, Null());
                return;
            }

            int flags = FunctionsRegexp::getDefaultFlags();
            String expr = expr_param.getString(0);
            String pat = pat_param.getString(0);
            if (unlikely(pat.empty()))
                throw Exception(EMPTY_PAT_ERR_MSG);

            String match_type = match_type_param.getString(0);

            Regexps::Regexp regexp(FunctionsRegexp::addMatchTypeForPattern(pat, match_type, collator), flags);
            ResultType res{regexp.match(expr)};
            res_arg.column = res_arg.type->createColumnConst(col_size, toField(res));
            return;
        }

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        ResultType default_val = 0;
        vec_res.assign(col_size, default_val);

        constexpr bool has_nullable_col
            = ExprT::isNullableCol() || PatT::isNullableCol() || MatchTypeT::isNullableCol();

        // Start to match
        if constexpr (canMemorize<PatT, MatchTypeT>())
        {
            std::unique_ptr<Regexps::Regexp> regexp;
            if (col_size > 0)
            {
                regexp = memorize<false>(pat_param, match_type_param, collator);
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
                // expr column must be a nullable column here, so we need to check null for each elems
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                nullmap.resize(col_size);

                StringRef expr_ref;
                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i))
                    {
                        nullmap[i] = 1;
                        continue;
                    }

                    nullmap[i] = 0;
                    expr_param.getStringRef(i, expr_ref);
                    vec_res[i] = regexp->match(expr_ref.data, expr_ref.size); // match
                }

                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                // expr column is impossible to be a nullable column here
                StringRef expr_ref;
                for (size_t i = 0; i < col_size; ++i)
                {
                    expr_param.getStringRef(i, expr_ref);
                    auto res = regexp->match(expr_ref.data, expr_ref.size);
                    vec_res[i] = res; // match
                }

                res_arg.column = std::move(col_res);
            }
        }
        else
        {
            if constexpr (has_nullable_col)
            {
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                nullmap.resize(col_size);

                StringRef expr_ref;
                String pat;
                String match_type;
                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expr_param.isNullAt(i) || pat_param.isNullAt(i) || match_type_param.isNullAt(i))
                    {
                        // This is a null result
                        nullmap[i] = 1;
                        continue;
                    }

                    nullmap[i] = 0;
                    expr_param.getStringRef(i, expr_ref);
                    pat = pat_param.getString(i);
                    match_type = match_type_param.getString(i);

                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);

                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp.match(expr_ref.data, expr_ref.size); // match
                }

                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                StringRef expr_ref;
                String pat;
                String match_type;
                for (size_t i = 0; i < col_size; ++i)
                {
                    expr_param.getStringRef(i, expr_ref);
                    pat = pat_param.getString(i);
                    match_type = match_type_param.getString(i);

                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);

                    auto regexp = FunctionsRegexp::createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp.match(expr_ref.data, expr_ref.size); // match
                }

                res_arg.column = std::move(col_res);
            }
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        // Do something related with nullable columns
        NullPresence null_presence = getNullPresense(block, arguments);

        const ColumnPtr & col_expr = block.getByPosition(arguments[0]).column;

        if (null_presence.has_null_constant)
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & col_pat = block.getByPosition(arguments[1]).column;

        size_t arg_num = arguments.size();
        auto & RES_ARG_VAR_NAME = block.getByPosition(result);

        ColumnPtr col_match_type;
        if (arg_num == 3)
            col_match_type = block.getByPosition(arguments[2]).column;

        size_t col_size = col_expr->size();

        ParamVariant EXPR_PV_VAR_NAME(col_expr, col_size, StringRef("", 0));
        ParamVariant PAT_PV_VAR_NAME(col_pat, col_size, StringRef("", 0));
        ParamVariant MATCH_TYPE_PV_VAR_NAME(col_match_type, col_size, StringRef("", 0));

        GET_ACTUAL_PARAMS_AND_EXECUTE()
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;
};

#undef GET_ACTUAL_PARAMS_AND_EXECUTE
#undef GET_EXPR_ACTUAL_PARAM
#undef GET_PAT_ACTUAL_PARAM
#undef GET_MATCH_TYPE_ACTUAL_PARAM
#undef EXECUTE_REGEXP_LIKE

} // namespace DB

#include <Functions/FunctionsRegexpUndef.h>
