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

#pragma once

#include <Common/config.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/re2Util.h>
#include <Functions/Regexps.h>
#include <Functions/StringUtil.h>
#include <Common/Volnitsky.h>

#include <memory>
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnsNumber.h"
#include "Common/Exception.h"
#include "Core/Field.h"
#include "DataTypes/DataTypeNullable.h"
#include "common/types.h"
#include <re2/re2.h>

#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

struct NameTiDBRegexp
{
    static constexpr auto name = "regexp";
};
struct NameRegexpLike
{
    static constexpr auto name = "regexp_like";
};
struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};
struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};
struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};
struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

String getMatchType(const String & match_type, TiDB::TiDBCollatorPtr collator = nullptr);

inline int getDefaultFlags()
{
    int flags = 0;
    flags |= OptimizedRegularExpressionImpl<false>::RE_NO_CAPTURE | OptimizedRegularExpressionImpl<false>::RE_NO_OPTIMIZE;
    return flags;
}

struct NullPresence
{
    bool has_nullable_col = false;
    bool has_const_null_col = false;
};

NullPresence getNullPresense(const Block & block, const ColumnNumbers & args);

inline String addMatchTypeForPattern(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    String flags = getMatchType(match_type, collator);
    return fmt::format("(?{}){}", flags, pattern);
}

inline Regexps::Pool::Pointer createRegexpWithMatchType(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    String final_pattern = addMatchTypeForPattern(pattern, match_type, collator);
    return Regexps::get<false, true>(final_pattern, getDefaultFlags());
}

inline void handleCollatorWithoutMatchType(String & pattern, TiDB::TiDBCollatorPtr collator)
{
    if (collator != nullptr && collator->isCI())
        pattern = fmt::format("(?i){}", pattern);
}

// Columns may be const, nullable or plain vector, we can conveniently handle
// these different type columns with Param.
class Param
{
public:
    DISALLOW_COPY_AND_MOVE(Param);

    Param(const ColumnPtr & ptr, const String & default_value)
        : col_ptr(ptr), col_str(nullptr), col_int64(nullptr), null_map(nullptr),
        is_const(false), data_string(default_value), data_int64(0)
    {
        // arg is not provided and we should use default_value
        if (col_ptr == nullptr) return;

        const auto * col_const = typeid_cast<const ColumnConst *>(&(*col_ptr));

        // Handle const
        if (col_const != nullptr)
        {
            // This is a const column
            auto col_const_data = col_const->getDataColumnPtr();
            if (col_const_data->isColumnNullable())
            {
                // This is a const nullable column
                // const null can't be here as we should have handle it in the previous
                Field field;
                auto p = static_cast<const ColumnNullable &>(*col_const_data).getNestedColumnPtr();
                col_const->get(0, field);
                data_string = field.safeGet<String>();
                null_map = &(static_cast<const ColumnNullable &>(*col_const_data).getNullMapData());
            }
            else
            {
                StringRef tmp_data = col_const->getDataAt(0);
                data_string = String(tmp_data.data, tmp_data.size);
            }
            
            is_const = true;
        }

        if (col_ptr->isColumnNullable())
        {
            // Handle nullable column
            auto nested_ptr = static_cast<const ColumnNullable &>(*col_ptr).getNestedColumnPtr();
            col_str = checkAndGetColumn<ColumnString>(&(*nested_ptr));
            null_map = &(static_cast<const ColumnNullable &>(*col_ptr).getNullMapData());
        }
        else {
            // This is a pure vector column
            col_str = checkAndGetColumn<ColumnString>(&(*col_ptr));
        }
    }

    Param(const ColumnPtr & ptr, Int64 default_value)
        : col_ptr(ptr), col_str(nullptr), col_int64(nullptr), null_map(nullptr),
        is_const(false), data_int64(default_value)
    {
        // arg is not provided and we should use default_value
        if (col_ptr == nullptr) return;

        const auto * col_const = typeid_cast<const ColumnConst *>(&(*col_ptr));

        // Handle const
        if (col_const != nullptr)
        {
            // This is a const column
            auto col_const_data = col_const->getDataColumnPtr();
            if (col_const_data->isColumnNullable())
            {
                // This is a const nullable column
                Field field;
                col_const->get(0, field);
                data_int64 = field.get<Int64>();
                null_map = &(static_cast<const ColumnNullable &>(*col_ptr).getNullMapData());
            }
            else
            {
                data_int64 = col_const->getValue<Int64>();
            }

            is_const = true;
            return;
        }

        if (col_ptr->isColumnNullable())
        {
            // Handle nullable column
            auto nested_ptr = static_cast<const ColumnNullable &>(*col_ptr).getNestedColumnPtr();
            col_int64 = checkAndGetColumn<ColumnInt64>(&(*nested_ptr));
            null_map = &(static_cast<const ColumnNullable &>(*col_ptr).getNullMapData());
        }
        else
        {
            // This is a pure vector column
            col_int64 = checkAndGetColumn<ColumnInt64>(&(*col_ptr));
        }
    }

    Int64 getInt64(size_t idx) const
    {
        // Use default value when arg is const or not provided.
        // For safety, nullptr should be checked
        return !is_const && col_int64 != nullptr ? col_int64->getInt(idx) : data_int64;
    }

    void getStringRef(size_t idx, StringRef & dst) const
    {
        // Use default value when arg is const or not provided.
        // For safety, nullptr should be checked
        if (!is_const && col_str != nullptr)
            dst = col_str->getDataAt(idx);
        else
        {
            dst.data = data_string.c_str();
            dst.size = data_string.size();
        }
    }

    String getString(size_t idx) const
    {
        // Use default value when arg is const or not provided.
        // For safety, nullptr should be checked
        if (!is_const && col_str != nullptr)
        {
            StringRef sr = col_str->getDataAt(idx);
            String ret_str(sr.data, sr.size);
            return ret_str;
        }
        else {
            String ret_str(data_string);
            return ret_str;
        }
    }

    bool isNullAt(size_t idx) const
    {
        if (null_map == nullptr) return false;

        return (*null_map)[idx];
    }

    bool isConstCol() const { return is_const; }
    bool isNullableCol() const { return null_map == nullptr; }
    size_t getDataNum() const { return col_ptr->size(); }
private:
    const ColumnPtr col_ptr;
    const ColumnString * col_str;
    const ColumnInt64 * col_int64;
    ConstNullMapPtr null_map;
    bool is_const; // mark as the const column when it's true
    String data_string;
    Int64 data_int64;
};

class FunctionStringRegexpBase
{
public:
    static constexpr size_t REGEXP_XXX_MIN_PARAM_NUM = 2;

    // Max parameter number the regexp_xxx function could receive
    static constexpr size_t REGEXP_MAX_PARAM_NUM = 2;
    static constexpr size_t REGEXP_LIKE_MAX_PARAM_NUM = 3;
    static constexpr size_t REGEXP_INSTR_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_REPLACE_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_SUBSTR_MAX_PARAM_NUM = 5;

    void memorize(const Param & pat_param, const std::unique_ptr<const Param> & match_type_param, TiDB::TiDBCollatorPtr collator) const
    {
        String && final_pattern = pat_param.getString(0);
        if (final_pattern.empty())
            throw Exception("Empty pattern is invalid");

        if (match_type_param != nullptr)
        {
            String && match_type = match_type_param->getString(0);
            final_pattern = addMatchTypeForPattern(final_pattern, match_type, collator);
        } else
        {
            handleCollatorWithoutMatchType(final_pattern, collator);
        }

        int flags = getDefaultFlags();
        memorized_re = std::make_unique<Regexps::Regexp>(final_pattern, flags);
    }

    // Check if we can memorize the regexp
    template <typename Name>
    static bool canMemorize(size_t arg_num, const Param & pat_param, const std::unique_ptr<const Param> & match_type_param)
    {
        size_t total_param_num = 0;
        constexpr std::string_view class_name_sv(Name::name);
        constexpr std::string_view tidb_regexp_name_sv(NameTiDBRegexp::name);
        constexpr std::string_view regexp_like_name_sv(NameRegexpLike::name);
        
        if constexpr (class_name_sv == tidb_regexp_name_sv)
            total_param_num = REGEXP_MAX_PARAM_NUM;
        else if constexpr (class_name_sv == regexp_like_name_sv)
            total_param_num = REGEXP_LIKE_MAX_PARAM_NUM;
        else
            throw Exception("Unknown regular function.");
        
        if constexpr (class_name_sv == tidb_regexp_name_sv)
        {
            return pat_param.isConstCol();
        } else
        {
            const bool is_pat_const = pat_param.isConstCol();
            if (is_pat_const && (arg_num < total_param_num || (match_type_param->isConstCol())))
                return true;
        }

        return false;        
    }

    bool isMemorized() const { return memorized_re != nullptr; }

    const std::unique_ptr<Regexps::Regexp> & getRegexp() const { return memorized_re; }
private:
    // We should pre compile the regular expression when:
    //  - only pattern column is provided and it's a constant column
    //  - pattern and match type columns are provided and they are both constant columns
    mutable std::unique_ptr<Regexps::Regexp> memorized_re;
};

// Implementation of regexp and regexp_like functions
template<typename Name>
class FunctionStringRegexp : public FunctionStringRegexpBase, public IFunction
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
        constexpr std::string_view class_name_sv(Name::name);
        constexpr std::string_view regexp_like_name_sv(NameRegexpLike::name);

        if constexpr (class_name_sv == regexp_like_name_sv)
            args_max_num = REGEXP_LIKE_MAX_PARAM_NUM;
        else
            args_max_num = REGEXP_MAX_PARAM_NUM;

        size_t arg_num = arguments.size();
        if (arg_num < REGEXP_XXX_MIN_PARAM_NUM || arg_num > args_max_num)
            throw Exception("Illegal argument number");

        bool has_nullable_col = false;
        
        for (size_t i = 0; i < REGEXP_XXX_MIN_PARAM_NUM; ++i)
            checkInputArg(arguments[i], &has_nullable_col);

        // check match_type arg for regexp_like
        if constexpr (class_name_sv == regexp_like_name_sv)
            if (arg_num == args_max_num && !arguments[args_max_num - 1]->isString())
                checkInputArg(arguments[args_max_num - 1], &has_nullable_col);

        if (has_nullable_col)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ResultType>>());
        else
            return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        // Do something related with nullable columns
        NullPresence null_presence = getNullPresense(block, arguments);

        const ColumnPtr & col_expr = block.getByPosition(arguments[0]).column;

        if (null_presence.has_const_null_col)
        {
            // There is a const null column in the input
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & col_pat = block.getByPosition(arguments[1]).column;

        if (col_expr->empty())
        {
            auto null_col_res = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
            block.getByPosition(result).column = ColumnConst::create(std::move(null_col_res), 0);
            return;
        }

        const Param expr_param(col_expr, String(""));
        const Param pat_param(col_pat, String(""));
        auto arg_num = arguments.size();
        size_t col_size = expr_param.getDataNum();

        // match_type_param will be initialized, only when this is a regexp_like function
        std::unique_ptr<const Param> match_type_param;

        constexpr std::string_view class_name(name);
        constexpr std::string_view regexp_like_name(NameRegexpLike::name);
        if constexpr (class_name == regexp_like_name)
        {
            // Try to get match type column only when it's a regexp_like function
            ColumnPtr col_match_type;
            if (arg_num > 2)
            {
                col_match_type = block.getByPosition(arguments[2]).column;
                match_type_param = std::make_unique<const Param>(col_match_type, String(""));
            }
            else
            {
                match_type_param = std::make_unique<const Param>(col_match_type, String(""));
            }
        }

        // Check if args are all const columns
        if (expr_param.isConstCol() && pat_param.isConstCol())
        {
#define GET_CONST_RESULT(block, expr, pat, pat_param, match_type_param, has_match_type, collator) \
    do { \
        int flags = getDefaultFlags(); \
        String final_pattern = (pat); \
        if constexpr (has_match_type) \
        { \
            /* put match_type into pattern */ \
            String match_type = (match_type_param)->getString(0); \
            final_pattern = addMatchTypeForPattern(final_pattern, match_type, (collator)); \
        } else \
            handleCollatorWithoutMatchType(final_pattern, (collator)); \
        Regexps::Regexp regexp(final_pattern, flags); \
        ResultType res{regexp.match(expr)}; \
        (block).getByPosition(result).column = (block).getByPosition(result).type->createColumnConst((pat_param).getDataNum(), toField(res)); \
    } while(0)

            String pat = pat_param.getString(0);
            if (pat.empty())
                throw Exception("Empty pattern is invalid");

            String expr = expr_param.getString(0);
            if constexpr (class_name == regexp_like_name)
            {
                // regexp_like function
                if (arg_num > 2 && match_type_param->isConstCol())
                {
                    constexpr bool has_match_type = true;
                    GET_CONST_RESULT(block, expr, pat, pat_param, match_type_param, has_match_type, collator);
                    return;
                }
                else if (arg_num == 2)
                {
                    constexpr bool has_match_type = false;
                    GET_CONST_RESULT(block, expr, pat, pat_param, match_type_param, has_match_type, collator);
                    return;
                }
                // reach here when arg_num == 3 and match_type is not const
            }
            else
            {
                // regexp function
                constexpr bool has_match_type = false;
                GET_CONST_RESULT(block, expr, pat, pat_param, match_type_param, has_match_type, collator);
                return;
            }
#undef GET_CONST_RESULT
        }

        // Check memorization
        if (canMemorize<Name>(arg_num, pat_param, match_type_param))
            memorize(pat_param, match_type_param, collator);

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(expr_param.getDataNum(), 0);

        // Start to match
        if (isMemorized())
        {
            const auto & regexp = getRegexp();            
            if (null_presence.has_nullable_col)
            {
                // expr column must be a nullable column here, so we need to check null for each elems
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                nullmap.resize(expr_param.getDataNum());

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

                block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
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

                block.getByPosition(result).column = std::move(col_res);
            } 
        }
        else
        {
            if (null_presence.has_nullable_col)
            {
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                nullmap.resize(expr_param.getDataNum());

                for (size_t i = 0; i < col_size; ++i)
                {
                    if constexpr (class_name == regexp_like_name)
                    {
                        if (expr_param.isNullAt(i) || pat_param.isNullAt(i) || (match_type_param != nullptr && match_type_param->isNullAt(i)))
                        {
                            // This is a null result
                            nullmap[i] = 1;
                            continue;
                        }
                    }
                    else
                    {
                        if (expr_param.isNullAt(i) || pat_param.isNullAt(i))
                        {
                            // This is a null result
                            nullmap[i] = 1;
                            continue;
                        }
                    }


                    nullmap[i] = 0;
                    String && expr = expr_param.getString(i);
                    String && pat = pat_param.getString(i);

                    if (pat.empty())
                        throw Exception("Empty pattern is invalid");

                    if constexpr (class_name == regexp_like_name)
                    {
                        // regexp_like function
                        auto regexp = createRegexpWithMatchType(pat, match_type_param->getString(i), collator);
                        vec_res[i] = regexp->match(expr); // match
                    }
                    else
                    {
                        // regexp function
                        handleCollatorWithoutMatchType(pat, collator);
                        int flags = getDefaultFlags();
                        const auto & regexp = Regexps::get<false, true>(pat, flags);
                        vec_res[i] = regexp->match(expr); // match
                    }
                }
                
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                for (size_t i = 0; i < col_size; ++i)
                {
                    String && expr = expr_param.getString(i);
                    String && pat = pat_param.getString(i);

                    if (pat.empty())
                        throw Exception("Empty pattern is invalid");

                    if constexpr (class_name == regexp_like_name)
                    {
                        // regexp_like function
                        auto regexp = createRegexpWithMatchType(pat, match_type_param->getString(i), collator);
                        vec_res[i] = regexp->match(expr); // match
                    }
                    else
                    {
                        // regexp function
                        handleCollatorWithoutMatchType(pat, collator);
                        int flags = getDefaultFlags();
                        const auto & regexp = Regexps::get<false, true>(pat, flags);
                        vec_res[i] = regexp->match(expr); // match
                    }
                }

                block.getByPosition(result).column = std::move(col_res);
            }
        }
    }
private:
    void checkInputArg(const DataTypePtr & arg, bool * has_nullable_col) const
    {
        if (arg->isNullable())
        {
            *has_nullable_col = true;
            const auto & null_type = checkAndGetDataType<DataTypeNullable>(arg.get());
            const auto & nested_type = null_type->getNestedType();
            if (!nested_type->isString())
                throw Exception(fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
        {
            if (!arg->isString())
                throw Exception(fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    TiDB::TiDBCollatorPtr collator = nullptr;
};

template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringReplace>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            return {3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_needle)
        {
            return {2, 3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_replacement)
        {
            return {1, 3, 4, 5};
        }
        else
        {
            return {1, 2, 3, 4, 5};
        }
    }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 3 && !arguments[3]->isInteger())
            throw Exception("Illegal type " + arguments[2]->getName() + " of forth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 4 && !arguments[4]->isInteger())
            throw Exception("Illegal type " + arguments[2]->getName() + " of fifth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 5 && !arguments[5]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of sixth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column_src = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;
        const ColumnPtr & column_replacement = block.getByPosition(arguments[2]).column;
        const ColumnPtr column_pos = arguments.size() > 3 ? block.getByPosition(arguments[3]).column : nullptr;
        const ColumnPtr column_occ = arguments.size() > 4 ? block.getByPosition(arguments[4]).column : nullptr;
        const ColumnPtr column_match_type = arguments.size() > 5 ? block.getByPosition(arguments[5]).column : nullptr;

        if ((column_pos != nullptr && !column_pos->isColumnConst())
            || (column_occ != nullptr && !column_occ->isColumnConst())
            || (column_match_type != nullptr && !column_match_type->isColumnConst()))
            throw Exception("4th, 5th, 6th arguments of function " + getName() + " must be constants.");
        Int64 pos = column_pos == nullptr ? 1 : typeid_cast<const ColumnConst *>(column_pos.get())->getInt(0);
        Int64 occ = column_occ == nullptr ? 0 : typeid_cast<const ColumnConst *>(column_occ.get())->getInt(0);
        String match_type = column_match_type == nullptr ? "" : typeid_cast<const ColumnConst *>(column_match_type.get())->getValue<String>();

        ColumnWithTypeAndName & column_result = block.getByPosition(result);

        bool needle_const = column_needle->isColumnConst();
        bool replacement_const = column_replacement->isColumnConst();

        if (needle_const && replacement_const)
        {
            executeImpl(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else if (needle_const)
        {
            executeImplNonConstReplacement(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else if (replacement_const)
        {
            executeImplNonConstNeedle(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else
        {
            executeImplNonConstNeedleReplacement(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
    }

private:
    void executeImpl(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos,
        Int64 occ,
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        const auto * c1_const = typeid_cast<const ColumnConst *>(column_needle.get());
        const auto * c2_const = typeid_cast<const ColumnConst *>(column_replacement.get());
        auto needle = c1_const->getValue<String>();
        auto replacement = c2_const->getValue<String>();

        if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), needle, replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vectorFixed(col->getChars(), col->getN(), needle, replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeImplNonConstNeedle(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle)
        {
            const auto * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const auto * col_replacement_const = typeid_cast<const ColumnConst *>(column_replacement.get());
            auto replacement = col_replacement_const->getValue<String>();

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedle(col->getChars(), col->getOffsets(), col_needle->getChars(), col_needle->getOffsets(), replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedle(col->getChars(), col->getN(), col_needle->getChars(), col_needle->getOffsets(), replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 2 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_replacement)
        {
            const auto * col_needle_const = typeid_cast<const ColumnConst *>(column_needle.get());
            auto needle = col_needle_const->getValue<String>();
            const auto * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstReplacement(col->getChars(), col->getOffsets(), needle, col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstReplacement(col->getChars(), col->getN(), needle, col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 3 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstNeedleReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            const auto * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const auto * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedleReplacement(col->getChars(), col->getOffsets(), col_needle->getChars(), col_needle->getOffsets(), col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedleReplacement(col->getChars(), col->getN(), col_needle->getChars(), col_needle->getOffsets(), col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 2 and 3 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    TiDB::TiDBCollatorPtr collator{};
};
} // namespace DB

