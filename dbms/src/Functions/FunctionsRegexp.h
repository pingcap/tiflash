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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/Volnitsky.h>
#include <Common/config.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsStringReplace.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Functions/StringUtil.h>
#include <Functions/re2Util.h>
#include <Parsers/Lexer.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/types.h>
#include <re2/re2.h>

#include <memory>
#include <type_traits>

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
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

const char * EMPTY_PAT_ERR_MSG = "Empty pattern is invalid";

struct NameTiDBRegexp
{
    static constexpr auto name = "regexp";
};
struct NameRegexpLike
{
    static constexpr auto name = "regexp_like";
};
struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};
struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

static constexpr std::string_view regexp_name(NameTiDBRegexp::name);
static constexpr std::string_view regexp_like_name(NameRegexpLike::name);

inline int getDefaultFlags()
{
    int flags = 0;
    flags |= OptimizedRegularExpressionImpl<false>::RE_NO_CAPTURE | OptimizedRegularExpressionImpl<false>::RE_NO_OPTIMIZE;
    return flags;
}

inline String addMatchTypeForPattern(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    String mode = re2Util::getRE2ModeModifiers(match_type, collator);
    if (mode.empty())
        return pattern;
    return fmt::format("{}{}", mode, pattern);
}

inline Regexps::Pool::Pointer createRegexpWithMatchType(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    String final_pattern = addMatchTypeForPattern(pattern, match_type, collator);
    return Regexps::get<false, true>(final_pattern, getDefaultFlags());
}

// Only int types used in ColumnsNumber.h can be valid
template <typename T>
inline constexpr bool check_int_type()
{
    return std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64> || std::is_same_v<T, Int8> || std::is_same_v<T, Int16> || std::is_same_v<T, Int32> || std::is_same_v<T, Int64>;
}

enum class IntType
{
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64
};

template <bool is_const>
class ParamString
{
public:
    DISALLOW_COPY_AND_MOVE(ParamString);

    using Chars_t = ColumnString::Chars_t;
    using Offsets = ColumnString::Offsets;

    // For passing compilation
    explicit ParamString(Int64)
        : const_string(nullptr, 0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    explicit ParamString(const StringRef & str_ref)
        : const_string(str_ref)
        , chars(nullptr)
        , offsets(nullptr)
    {
        if constexpr (!is_const)
            throw Exception("non-const parm should not call this constructor");
    }

    // For passing compilation
    explicit ParamString(const void *)
        : const_string(nullptr, 0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    ParamString(const void * chars_, const void * offsets_)
        : const_string(nullptr, 0)
        , chars(reinterpret_cast<const Chars_t *>(chars_))
        , offsets(reinterpret_cast<const Offsets *>(offsets_))
    {
        if constexpr (is_const)
            throw Exception("const parm should not call this constructor");
    }

    Int64 getInt(size_t) const { throw Exception("ParamString not supports this function"); }

    String getString(size_t idx) const
    {
        if constexpr (is_const)
            return String(const_string.data, const_string.size);
        else
            return String(reinterpret_cast<const char *>(&(*chars)[offsetAt(idx)]), sizeAt(idx) - 1);
    }

    void getStringRef(size_t idx, StringRef & dst) const
    {
        if constexpr (is_const)
        {
            dst.data = const_string.data;
            dst.size = const_string.size;
        }
        else
        {
            auto tmp = StringRef(reinterpret_cast<const char *>(&(*chars)[offsetAt(idx)]), sizeAt(idx) - 1);
            dst.data = tmp.data;
            dst.size = tmp.size;
        }
    }

    constexpr static bool isConst() { return is_const; }

private:
    size_t offsetAt(size_t i) const { return i == 0 ? 0 : (*offsets)[i - 1]; }
    size_t sizeAt(size_t i) const { return i == 0 ? (*offsets)[0] : ((*offsets)[i] - (*offsets)[i - 1]); }

    StringRef const_string;

    // for vector string
    const Chars_t * chars;
    const Offsets * offsets;
};

template <bool is_const>
class ParamInt
{
public:
    DISALLOW_COPY_AND_MOVE(ParamInt);

    explicit ParamInt(Int64 val)
        : const_int_val(val)
        , int_type(IntType::UInt8)
        , int_container(nullptr)
    {
        if constexpr (!is_const)
            throw Exception("non-const parm should not call this constructor");
    }

    // For passing compilation
    explicit ParamInt(const StringRef &)
        : const_int_val(0)
        , int_type(IntType::UInt8)
        , int_container(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    ParamInt(const void * int_container_, IntType int_type_)
        : const_int_val(0)
        , int_type(int_type_)
        , int_container(int_container_)
    {
        if constexpr (is_const)
            throw Exception("const parm should not call this constructor");
    }

    // For passing compilation
    ParamInt(const void *, const void *)
        : const_int_val(0)
        , int_type(IntType::UInt8)
        , int_container(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    template <typename T>
    Int64 getInt(size_t idx) const
    {
        if constexpr (is_const)
            return const_int_val;
        else
        {
            const auto * tmp = reinterpret_cast<const typename ColumnVector<std::enable_if_t<check_int_type<T>(), T>>::Container *>(int_container);
            return static_cast<Int64>((*tmp)[idx]);
        }
    }

    void setIntType(IntType int_type_) { int_type = int_type_; }
    IntType getIntType() const { return int_type; }
    String getString(size_t) const { throw Exception("ParamInt not supports this function"); }
    void getStringRef(size_t, StringRef &) const { throw Exception("ParamInt not supports this function"); }
    constexpr static bool isConst() { return is_const; }
    void setContainer(const void * container) { int_container = container; }
    const void * getContainer() const { return int_container; }

private:
    Int64 const_int_val;
    IntType int_type;

    // for vector int
    // type: ColumnVector::Container
    const void * int_container;
};

// Columns may be const, nullable or plain vector, we can conveniently handle
// these different type columns with Param.
template <typename ParamImplType, bool is_nullable>
class Param
{
public:
    DISALLOW_COPY_AND_MOVE(Param);

    // const string param
    Param(size_t col_size_, const StringRef & str_ref)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(str_ref)
    {
        checkNullableLogic();
    }

    // const nullable string param
    Param(size_t col_size_, const StringRef & str_ref, ConstNullMapPtr null_map_)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(str_ref)
    {
        checkNullableLogic();
    }

    // const int param
    Param(size_t col_size_, Int64 val)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(val)
    {
        checkNullableLogic();
    }

    // const nullable int param
    Param(size_t col_size_, Int64 val, ConstNullMapPtr null_map_)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(val)
    {
        checkNullableLogic();
    }

    // pure vector string param
    // chars_ type: ParamImplType::Chars_t
    // offsets_ type: ParamImplType::Offsets
    Param(size_t col_size_, const void * chars_, const void * offsets_)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(chars_, offsets_)
    {
        checkNullableLogic();
    }

    // pure vector int param
    // int_container_ type: ParamImplType::Container
    Param(size_t col_size_, const void * int_container_)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(int_container_)
    {
        checkNullableLogic();
    }

    // nullable vector string param
    // chars_ type: ParamImplType::Chars_t
    // offsets_ type: ParamImplType::Offsets
    Param(size_t col_size_, ConstNullMapPtr null_map_, const void * chars_, const void * offsets_)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(chars_, offsets_)
    {
        checkNullableLogic();
    }

    // nullable vector int param
    // int_container_ type: ParamImplType::Container
    Param(size_t col_size_, ConstNullMapPtr null_map_, const void * int_container_)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(int_container_)
    {
        checkNullableLogic();
    }

    Int64 getInt(size_t idx) const { return data.getInt(idx); }
    void getStringRef(size_t idx, StringRef & dst) const { return data.getStringRef(idx, dst); }
    String getString(size_t idx) const { return data.getString(idx); }

    bool isNullAt(size_t idx) const
    {
        if constexpr (is_nullable && ParamImplType::isConst())
            return (*null_map)[0];
        else if constexpr (is_nullable)
            return (*null_map)[idx];
        else
            return false;
    }

    size_t getDataNum() const { return col_size; }
    constexpr static bool isNullableCol() { return is_nullable; }
    constexpr static bool isConst() { return ParamImplType::isConst(); }

private:
    // When this is a nullable param, we should ensure the null_map is not nullptr
    inline void checkNullableLogic()
    {
        if (is_nullable && (null_map == nullptr))
            throw Exception("Nullable Param with nullptr null_map");
    }

    const size_t col_size;
    ConstNullMapPtr null_map;
    ParamImplType data;
};

#define APPLY_FOR_PARAM_STRING_VARIANTS(M, pv_name, param_name, next_process) \
    M(StringNullableAndNotConst, pv_name, param_name, next_process)           \
    M(StringNotNullableAndConst, pv_name, param_name, next_process)           \
    M(StringNotNullableAndNotConst, pv_name, param_name, next_process)        \
    M(StringNullableAndConst, pv_name, param_name, next_process)

#define APPLY_FOR_PARAM_INT_VARIANTS(M, pv_name, param_name, next_process) \
    M(IntNullableAndNotConst, pv_name, param_name, next_process)           \
    M(IntNotNullableAndConst, pv_name, param_name, next_process)           \
    M(IntNotNullableAndNotConst, pv_name, param_name, next_process)        \
    M(IntNullableAndConst, pv_name, param_name, next_process)

#define ACTUAL_PARAM_TYPE(NAME) ParamVariant::Param##NAME

#define GET_ACTUAL_PARAM_PTR(NAME, param_ptr_name) (reinterpret_cast<ACTUAL_PARAM_TYPE(NAME) *>(param_ptr_name))

#define ENUMERATE_PARAM_VARIANT_CASES(NAME, param_ptr_name, actual_param_ptr_name, next_process) \
    case ParamVariant::ParamType::NAME:                                                          \
    {                                                                                            \
        auto *(actual_param_ptr_name) = GET_ACTUAL_PARAM_PTR(NAME, param_ptr_name);              \
        next_process;                                                                            \
        break;                                                                                   \
    }

class ParamVariant
{
public:
    // String type
    using ParamStringNullableAndNotConst = Param<ParamString<false>, true>;
    using ParamStringNotNullableAndConst = Param<ParamString<true>, false>;
    using ParamStringNotNullableAndNotConst = Param<ParamString<false>, false>;
    using ParamStringNullableAndConst = Param<ParamString<true>, true>;

    // Int type
    using ParamIntNullableAndNotConst = Param<ParamInt<false>, true>;
    using ParamIntNotNullableAndConst = Param<ParamInt<true>, false>;
    using ParamIntNotNullableAndNotConst = Param<ParamInt<false>, false>;
    using ParamIntNullableAndConst = Param<ParamInt<true>, true>;

    enum class ParamType
    {
        StringNullableAndNotConst,
        StringNotNullableAndConst,
        StringNotNullableAndNotConst,
        StringNullableAndConst,
        IntNullableAndNotConst,
        IntNotNullableAndConst,
        IntNotNullableAndNotConst,
        IntNullableAndConst
    };

    // default ParamString's ParamType should be ParamType::StringNotNullAndNotConst
    ParamVariant(ColumnPtr col, size_t col_size, const StringRef & default_val)
        : col_ptr(col)
        , default_str(default_val)
        , default_int(0)
        , param(nullptr)
    {
        if (col_ptr != nullptr)
        {
            setParamStringTypeAndGenerateParam(col_size);
        }
        else
        {
            // This param is not provided by user, so we should use default value.
            param = new ParamStringNotNullableAndConst(col_size, default_val);
            param_type = ParamType::StringNotNullableAndConst;
        }
    }

    // default ParamInt's ParamType should be ParamType::IntNotNullAndNotConst
    ParamVariant(ColumnPtr col, size_t col_size [[maybe_unused]], Int64 default_val)
        : col_ptr(col)
        , default_str("", 0)
        , default_int(default_val)
        , param(nullptr)
    {
        // TODO implement it in next pr
        throw Exception("Not implemented so far");
    }

    ~ParamVariant()
    {
        if (param != nullptr)
        {
            switch (param_type)
            {
                // Expand the macro to enumerate string param cases
                APPLY_FOR_PARAM_STRING_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, param, actual_param_ptr, ({ delete actual_param_ptr; }))

                // Expand the macro to enumerate int param cases
                APPLY_FOR_PARAM_INT_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, param, actual_param_ptr, ({ delete actual_param_ptr; }))
            default:
                throw Exception("Unexpected ParamType");
            }
        }
    }

    ParamType getParamType() const { return param_type; }

private:
    void handleStringConstCol(size_t col_size, const ColumnConst * col_const)
    {
        const auto & col_const_data = col_const->getDataColumnPtr();
        if (col_const_data->isColumnNullable())
        {
            Field field;
            col_const->get(0, field);
            String tmp = field.isNull() ? String("") : field.safeGet<String>();
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(col_const_data)).getNullMapData());

            // Construct actual param
            param = new ParamStringNullableAndConst(col_size, StringRef(tmp.data(), tmp.size()), null_map);
            param_type = ParamType::StringNullableAndConst;
        }
        else
        {
            // Construct actual param
            param = new ParamStringNotNullableAndConst(col_size, col_const->getDataAt(0));
            param_type = ParamType::StringNotNullableAndConst;
        }
    }

    void handleStringNonConstCol(size_t col_size)
    {
        if (col_ptr->isColumnNullable())
        {
            auto nested_ptr = static_cast<const ColumnNullable &>(*(col_ptr)).getNestedColumnPtr();
            const auto * tmp = checkAndGetColumn<ColumnString>(&(*nested_ptr));
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(col_ptr)).getNullMapData());

            // Construct actual param
            param = new ParamStringNullableAndNotConst(col_size, null_map, static_cast<const void *>(&(tmp->getChars())), static_cast<const void *>(&(tmp->getOffsets())));
            param_type = ParamType::StringNullableAndNotConst;
        }
        else
        {
            // This is a pure string vector column
            const auto * tmp = checkAndGetColumn<ColumnString>(&(*(col_ptr)));

            // Construct actual param
            param = new ParamStringNotNullableAndNotConst(col_size, static_cast<const void *>(&(tmp->getChars())), static_cast<const void *>(&(tmp->getOffsets())));
            param_type = ParamType::StringNotNullableAndNotConst;
        }
    }

    void setParamStringTypeAndGenerateParam(size_t col_size)
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(&(*(col_ptr)));
        if (col_const != nullptr)
            handleStringConstCol(col_size, col_const);
        else
            handleStringNonConstCol(col_size);
    }

    // TODO implement it in next pr
    void setParamIntTypeAndGenerateParam(size_t col_size [[maybe_unused]])
    {
        throw Exception("Not implemented so far");
    }

    ParamType param_type;
    ColumnPtr col_ptr;
    StringRef default_str;
    Int64 default_int [[maybe_unused]];

public:
    // ATTENTION! Be careful to change this variable's name as many macros use it
    //
    // This variable should be reinterpret_cast to specific type before used
    void * param;
};

// Unifying these names is necessary in macros
#define EXPR_PV_VAR_NAME expr_pv
#define PAT_PV_VAR_NAME pat_pv
#define MATCH_TYPE_PV_VAR_NAME match_type_pv

#define EXPR_PARAM_PTR_VAR_NAME expr_param
#define PAT_PARAM_PTR_VAR_NAME pat_param
#define MATCH_TYPE_PARAM_PTR_VAR_NAME match_type_param

#define RES_ARG_VAR_NAME res_arg

// Unify the name of functions that actually execute regexp
#define REGEXP_CLASS_MEM_FUNC_IMPL_NAME executeRegexpFunc

// Do not merge GET_ACTUAL_STRING_PARAM and GET_ACTUAL_INT_PARAM together,
// as this will generate more useless codes and templates.

#define GET_ACTUAL_STRING_PARAM(pv_name, param_name, next_process)                                                    \
    do                                                                                                                \
    {                                                                                                                 \
        switch ((pv_name).getParamType())                                                                             \
        {                                                                                                             \
            /* Expand this macro to enumerate all string cases */                                                     \
            APPLY_FOR_PARAM_STRING_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, (pv_name).param, param_name, next_process) \
        default:                                                                                                      \
            throw Exception("Unexpected ParamType");                                                                  \
        }                                                                                                             \
    } while (0);

// Common method to get actual string param
// TODO implement it in next pr
#define GET_ACTUAL_INT_PARAM(pv_name, param_name, next_process) \
    do                                                          \
    {                                                           \
    } while (0);

class FunctionStringRegexpBase
{
public:
    static constexpr size_t REGEXP_MIN_PARAM_NUM = 2;

    // Max parameter number the regexp_xxx function could receive
    static constexpr size_t REGEXP_MAX_PARAM_NUM = 2;
    static constexpr size_t REGEXP_LIKE_MAX_PARAM_NUM = 3;
    static constexpr size_t REGEXP_INSTR_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_REPLACE_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_SUBSTR_MAX_PARAM_NUM = 5;

    // We should pre compile the regular expression when:
    //  - only pattern column is provided and it's a constant column
    //  - pattern and match type columns are provided and they are both constant columns
    template <typename ExprT, typename MatchTypeT>
    std::unique_ptr<Regexps::Regexp> memorize(const ExprT & pat_param, const MatchTypeT & match_type_param, TiDB::TiDBCollatorPtr collator) const
    {
        if (pat_param.isNullAt(0) || match_type_param.isNullAt(0))
            return nullptr;

        String final_pattern = pat_param.getString(0);
        if (unlikely(final_pattern.empty()))
            throw Exception(EMPTY_PAT_ERR_MSG);

        String match_type = match_type_param.getString(0);
        final_pattern = addMatchTypeForPattern(final_pattern, match_type, collator);

        int flags = getDefaultFlags();
        return std::make_unique<Regexps::Regexp>(final_pattern, flags);
    }

    // Check if we can memorize the regexp
    template <typename PatT, typename MatchTypeT>
    constexpr static bool canMemorize()
    {
        return (PatT::isConst() && MatchTypeT::isConst());
    }

    static void checkInputArg(const DataTypePtr & arg, bool is_str, bool * has_nullable_col, bool * has_data_type_nothing)
    {
        if (is_str)
        {
            // Check string type argument
            if (arg->isNullable())
            {
                *has_nullable_col = true;
                const auto * null_type = checkAndGetDataType<DataTypeNullable>(arg.get());
                assert(null_type != nullptr);

                const auto & nested_type = null_type->getNestedType();

                // It may be DataTypeNothing if it's not string
                if (!nested_type->isString())
                {
                    if (nested_type->getTypeId() != TypeIndex::Nothing)
                        throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    else
                        *has_data_type_nothing = true;
                }
            }
            else
            {
                if (!arg->isString())
                    throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        else
        {
            // Check int type argument
            if (arg->isNullable())
            {
                *has_nullable_col = true;
                const auto * null_type = checkAndGetDataType<DataTypeNullable>(arg.get());
                assert(null_type != nullptr);

                const auto & nested_type = null_type->getNestedType();

                // It may be DataTypeNothing if it's not string
                if (!nested_type->isInteger())
                {
                    if (nested_type->getTypeId() != TypeIndex::Nothing)
                        throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    else
                        *has_data_type_nothing = true;
                }
            }
            else
            {
                if (!arg->isInteger())
                    throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }
};

// regexp and regexp_like functions are executed in this macro
#define EXECUTE_REGEXP_LIKE()                                                                                                                       \
    do                                                                                                                                              \
    {                                                                                                                                               \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(RES_ARG_VAR_NAME, *(EXPR_PARAM_PTR_VAR_NAME), *(PAT_PARAM_PTR_VAR_NAME), *(MATCH_TYPE_PARAM_PTR_VAR_NAME)); \
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
class FunctionStringRegexp : public FunctionStringRegexpBase
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
    void REGEXP_CLASS_MEM_FUNC_IMPL_NAME(ColumnWithTypeAndName & res_arg, const ExprT & expr_param, const PatT & pat_param, const MatchTypeT & match_type_param) const
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

            int flags = getDefaultFlags();
            String expr = expr_param.getString(0);
            String pat = pat_param.getString(0);
            if (unlikely(pat.empty()))
                throw Exception(EMPTY_PAT_ERR_MSG);

            String match_type = match_type_param.getString(0);

            Regexps::Regexp regexp(addMatchTypeForPattern(pat, match_type, collator), flags);
            ResultType res{regexp.match(expr)};
            res_arg.column = res_arg.type->createColumnConst(col_size, toField(res));
            return;
        }

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(col_size, 0);

        constexpr bool has_nullable_col = ExprT::isNullableCol() || PatT::isNullableCol() || MatchTypeT::isNullableCol();

        // Start to match
        if constexpr (canMemorize<PatT, MatchTypeT>())
        {
            std::unique_ptr<Regexps::Regexp> regexp;
            if (col_size > 0)
            {
                regexp = memorize(pat_param, match_type_param, collator);
                if (regexp == nullptr)
                {
                    auto nullmap_col = ColumnUInt8::create();
                    typename ColumnUInt8::Container & nullmap = nullmap_col->getData();
                    nullmap.resize(col_size, 1);
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

                    auto regexp = createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp->match(expr_ref.data, expr_ref.size); // match
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

                    auto regexp = createRegexpWithMatchType(pat, match_type, collator);
                    vec_res[i] = regexp->match(expr_ref.data, expr_ref.size); // match
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
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
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


#undef GET_ACTUAL_INT_PARAM
#undef GET_ACTUAL_STRING_PARAM
#undef REGEXP_CLASS_MEM_FUNC_IMPL_NAME
#undef RES_ARG_VAR_NAME
#undef MATCH_TYPE_PARAM_PTR_VAR_NAME
#undef PAT_PARAM_PTR_VAR_NAME
#undef EXPR_PARAM_PTR_VAR_NAME
#undef MATCH_TYPE_PV_VAR_NAME
#undef PAT_PV_VAR_NAME
#undef EXPR_PV_VAR_NAME

#undef ACTUAL_PARAM_TYPE
#undef GET_ACTUAL_PARAM_PTR
#undef ENUMERATE_PARAM_VARIANT_CASES
#undef APPLY_FOR_PARAM_INT_VARIANTS
#undef APPLY_FOR_PARAM_STRING_VARIANTS

} // namespace DB
