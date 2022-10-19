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
#include <Columns/ColumnString.h>
#include <Common/Volnitsky.h>
#include <Common/config.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Functions/StringUtil.h>
#include <Functions/re2Util.h>
#include <re2/re2.h>

#include <memory>
#include <type_traits>

#include "Columns/ColumnNullable.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Common/Exception.h"
#include "Core/Field.h"
#include "Core/Types.h"
#include "DataTypes/DataTypeNothing.h"
#include "DataTypes/DataTypeNullable.h"
#include "Parsers/Lexer.h"
#include "common/StringRef.h"
#include "common/defines.h"
#include "common/types.h"

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
struct NameRegexpInstr
{
    static constexpr auto name = "regexp_instr";
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

static constexpr std::string_view regexp_name(NameTiDBRegexp::name);
static constexpr std::string_view regexp_like_name(NameRegexpLike::name);
static constexpr std::string_view regexp_instr_name(NameRegexpInstr::name);

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
    bool has_data_type_nothing = false;
};

NullPresence getNullPresense(const Block & block, const ColumnNumbers & args);

// add '()' outside of the pattern to get the matched substr
inline String addMatchTypeForPattern(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    String flags = getMatchType(match_type, collator);
    if (flags.empty())
        return fmt::format("({})", pattern);
    
    return fmt::format("(?{})({})", flags, pattern);
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
    return static_cast<bool>(std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64> || std::is_same_v<T, UInt128> || std::is_same_v<T, Int8> || std::is_same_v<T, Int16> || std::is_same_v<T, Int32> || std::is_same_v<T, Int64>);
}

enum class IntType { UInt8 = 0, UInt16, UInt32, UInt64, UInt128, Int8, Int16, Int32, Int64 };

Int64 getUInt8(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<UInt8>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getUInt16(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<UInt16>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getUInt32(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<UInt32>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getUInt64(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<UInt64>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getUInt128(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<UInt128>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getInt8(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<Int8>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getInt16(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<Int16>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getInt32(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<Int32>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

Int64 getInt64(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<Int64>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

using GetIntFuncPointerType = Int64 (*)(const void *, size_t);

GetIntFuncPointerType getGetIntFuncPointer(IntType int_type)
{
    switch (int_type)
    {
    case IntType::UInt8:
        return getUInt8;
    case IntType::UInt16:
        return getUInt16;
    case IntType::UInt32:
        return getUInt32;
    case IntType::UInt64:
        return getUInt64;
    case IntType::UInt128:
        return getUInt128;
    case IntType::Int8:
        return getInt8;
    case IntType::Int16:
        return getInt16;
    case IntType::Int32:
        return getInt32;
    case IntType::Int64:
        return getInt64;
    default:
        throw Exception("Unexpected int type");
    }
}

// Use this type when param is not provided
class ParamDefault
{
public:
    explicit ParamDefault(Int64 val)
        : default_int(val)
        , default_string("")
    {}
    explicit ParamDefault(const StringRef & str)
        : default_int(0)
        , default_string(str)
    {}

    // For passing compilation
    explicit ParamDefault(const void *)
        : default_int(0)
        , default_string("")
    {
        throw Exception("Shouldn't call this constructor");
    }

    // For passing compilation
    ParamDefault(const void *, const void *)
        : default_int(0)
        , default_string("")
    {
        throw Exception("Shouldn't call this constructor");
    }

    static IntType getIntType() { return IntType::Int64; }

    template <typename T>
    Int64 getInt(size_t) const { return default_int; }
    static String getString(size_t) { return String(""); }
    void getStringRef(size_t, StringRef &) const {}
    constexpr static bool isConst() { return true; }
    static const void * getContainer() { throw Exception("ParamDefault not supports this function"); }

private:
    Int64 default_int;
    StringRef default_string;
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

    static IntType getIntType() { throw Exception("ParamString not supports this function"); }

    template <typename T>
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

    const void * getContainer() const { return nullptr; }

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

    explicit ParamInt(const void * int_container_, IntType int_type_)
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
            const auto * tmp = reinterpret_cast<const typename ColumnVector<std::enable_if_t<check_int_type<T>, T>>::Container *>(int_container);
            return static_cast<Int64>((*tmp)[idx]);
        }
    }

    IntType getIntType() const { return int_type; }
    String getString(size_t) const { throw Exception("ParamInt not supports this function"); }
    void getStringRef(size_t, StringRef &) const { throw Exception("ParamInt not supports this function"); }
    constexpr static bool isConst() { return is_const; }
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
template <typename ParamImplType, bool is_null>
class Param
{
public:
    DISALLOW_COPY_AND_MOVE(Param);

    // const string param
    Param(size_t col_size_, const StringRef & str_ref)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(str_ref)
    {}

    // const int param
    Param(size_t col_size_, Int64 val)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(val)
    {}

    // pure vector string param
    // chars_ type: ParamImplType::Chars_t
    // offsets_ type: ParamImplType::Offsets
    Param(size_t col_size_, const void * chars_, const void * offsets_)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(chars_, offsets_)
    {}

    // pure vector int param
    // int_container_ type: ParamImplType::Container
    Param(size_t col_size_, const void * int_container_, IntType int_type)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(int_container_, int_type)
    {}

    // nullable vector string param
    // chars_ type: ParamImplType::Chars_t
    // offsets_ type: ParamImplType::Offsets
    Param(size_t col_size_, ConstNullMapPtr null_map_, const void * chars_, const void * offsets_)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(chars_, offsets_)
    {}

    // nullable vector int param
    // int_container_ type: ParamImplType::Container
    Param(size_t col_size_, ConstNullMapPtr null_map_, const void * int_container_, IntType int_type)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(int_container_, int_type)
    {}

    template <typename T>
    Int64 getInt(size_t idx) const { return data.template getInt<T>(idx); }
    void getStringRef(size_t idx, StringRef & dst) const { return data.getStringRef(idx, dst); }
    String getString(size_t idx) const { return data.getString(idx); }

    bool isNullAt(size_t idx) const
    {
        // null_map works only when we are non-const nullable column
        if constexpr (is_null && !ParamImplType::isConst())
            return (*null_map)[idx];
        else
            return false;
    }

    IntType getIntType() const { return data.getIntType(); }
    size_t getDataNum() const { return col_size; }
    constexpr static bool isNullableCol() { return is_null; }
    constexpr static bool isConst() { return ParamImplType::isConst(); }
    const void * getContainer() const { return data.getContainer(); }

private:
    const size_t col_size;
    ConstNullMapPtr null_map;
    ParamImplType data;
};

// Unifying these names is necessary in macros
#define EXPR_COL_PTR_VAR_NAME col_expr
#define PAT_COL_PTR_VAR_NAME col_pat
#define POS_COL_PTR_VAR_NAME col_pos
#define OCCUR_COL_PTR_VAR_NAME col_occur
#define RET_OP_COL_PTR_VAR_NAME col_ret_op
#define MATCH_TYPE_COL_PTR_VAR_NAME col_match_type

#define RES_ARG_VAR_NAME res_arg

#define EXPR_PARAM_VAR_NAME expr_param
#define PAT_PARAM_VAR_NAME pat_param
#define POS_PARAM_VAR_NAME pos_param
#define OCCUR_PARAM_VAR_NAME occur_param
#define RET_OP_PARAM_VAR_NAME ret_op_param
#define MATCH_TYPE_PARAM_VAR_NAME match_type_param

#define SELF_CLASS_NAME (name)
#define ARG_NUM_VAR_NAME arg_num
#define VEC_RES_VAR_NAME vec_res
#define COLLATOR_VAR_NAME collator

// Unify the name of functions that actually execute regexp
#define REGEXP_CLASS_MEM_FUNC_IMPL_NAME process

// Method to convert nullable string column
// processed_col is impossible to be const here
#define CONVERT_NULL_STR_COL_TO_PARAM(param_name, processed_col, next_process)                                                                                               \
    do                                                                                                                                                                       \
    {                                                                                                                                                                        \
        size_t col_size = (processed_col)->size();                                                                                                                           \
        if (((processed_col)->isColumnNullable()))                                                                                                                           \
        {                                                                                                                                                                    \
            auto nested_ptr = static_cast<const ColumnNullable &>(*(processed_col)).getNestedColumnPtr();                                                                    \
            const auto * tmp = checkAndGetColumn<ColumnString>(&(*nested_ptr));                                                                                              \
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(processed_col)).getNullMapData());                                                               \
            Param<ParamString<false>, true>(param_name)(col_size, null_map, static_cast<const void *>(&(tmp->getChars())), static_cast<const void *>(&(tmp->getOffsets()))); \
            next_process;                                                                                                                                                    \
        }                                                                                                                                                                    \
        else                                                                                                                                                                 \
        {                                                                                                                                                                    \
            /* This is a pure string vector column */                                                                                                                        \
            const auto * tmp = checkAndGetColumn<ColumnString>(&(*(processed_col)));                                                                                         \
            Param<ParamString<false>, false>(param_name)(col_size, static_cast<const void *>(&(tmp->getChars())), static_cast<const void *>(&(tmp->getOffsets())));          \
            next_process;                                                                                                                                                    \
        }                                                                                                                                                                    \
    } while (0);

// Method to convert const string column to param
#define CONVERT_CONST_STR_COL_TO_PARAM(param_name, processed_col, next_process)                                             \
    do                                                                                                                      \
    {                                                                                                                       \
        size_t col_size = (processed_col)->size();                                                                          \
        const auto * col_const = typeid_cast<const ColumnConst *>(&(*(processed_col)));                                     \
        if (col_const != nullptr)                                                                                           \
        {                                                                                                                   \
            auto col_const_data = col_const->getDataColumnPtr();                                                            \
            if (col_const_data->isColumnNullable())                                                                         \
            {                                                                                                               \
                /* This is a const column and it can't be const null column as we should have handled it in the previous */ \
                Field field;                                                                                                \
                col_const->get(0, field);                                                                                   \
                String tmp = field.safeGet<String>();                                                                       \
                /* const col */                                                                                             \
                Param<ParamString<true>, true>(param_name)(col_size, StringRef(tmp.data(), tmp.size()));                    \
                next_process;                                                                                               \
            }                                                                                                               \
            else                                                                                                            \
            {                                                                                                               \
                /* const col */                                                                                             \
                Param<ParamString<true>, false>(param_name)(col_size, col_const->getDataAt(0));                             \
                next_process;                                                                                               \
            }                                                                                                               \
        }                                                                                                                   \
        else                                                                                                                \
        {                                                                                                                   \
            CONVERT_NULL_STR_COL_TO_PARAM((param_name), (processed_col), next_process)                                     \
        }                                                                                                                   \
    } while (0);

// Method to convert nullable int column to param
// processed_col is impossible to be const here
#define CONVERT_NULL_INT_COL_TO_PARAM(param_name, processed_col, next_process)                                 \
    do                                                                                                         \
    {                                                                                                          \
        size_t col_size = (processed_col)->size();                                                             \
        if ((processed_col)->isColumnNullable())                                                               \
        {                                                                                                      \
            auto nested_ptr = static_cast<const ColumnNullable &>(*(processed_col)).getNestedColumnPtr();      \
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(processed_col)).getNullMapData());              \
            /* various int types may be input, we need to check them one by one */                             \
            if (const auto * ptr = typeid_cast<const ColumnUInt8 *>(&(*(nested_ptr))))                      \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::UInt8);        \
            else if (const auto * ptr = typeid_cast<const ColumnUInt16 *>(&(*(nested_ptr))))                \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::UInt16);       \
            else if (const auto * ptr = typeid_cast<const ColumnUInt32 *>(&(*(nested_ptr))))                 \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::UInt32);       \
            else if (const auto * ptr = typeid_cast<const ColumnUInt64 *>(&(*(nested_ptr))))                 \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::UInt64);       \
            else if (const auto * ptr = typeid_cast<const ColumnUInt128 *>(&(*(nested_ptr))))                \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::UInt128);      \
            else if (const auto * ptr = typeid_cast<const ColumnInt8 *>(&(*(nested_ptr))))                   \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::Int8);         \
            else if (const auto * ptr = typeid_cast<const ColumnInt16 *>(&(*(nested_ptr))))                  \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::Int16);        \
            else if (const auto * ptr = typeid_cast<const ColumnInt32 *>(&(*(nested_ptr))))                  \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::Int32);        \
            else if (const auto * ptr = typeid_cast<const ColumnInt64 *>(&(*(nested_ptr))))                  \
                Param<ParamInt<false>, true>(param_name)(col_size, null_map, &(ptr->getData()), IntType::Int64);        \
            else                                                                                               \
                throw Exception("Invalid int type int regexp function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT); \
        }                                                                                                      \
        else                                                                                                   \
        {                                                                                                      \
            /* This is a pure vector column */                                                                 \
            /* various int types may be input, we need to check them one by one */                             \
            if (const auto * ptr = typeid_cast<const ColumnUInt8 *>(&(*(processed_col))))                    \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::UInt8);                 \
            else if (const auto * ptr = typeid_cast<const ColumnUInt16 *>(&(*(processed_col))))              \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::UInt16);                \
            else if (const auto * ptr = typeid_cast<const ColumnUInt32 *>(&(*(processed_col))))              \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::UInt32);                \
            else if (const auto * ptr = typeid_cast<const ColumnUInt64 *>(&(*(processed_col))))              \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::UInt64);                \
            else if (const auto * ptr = typeid_cast<const ColumnUInt128 *>(&(*(processed_col))))             \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::UInt128);               \
            else if (const auto * ptr = typeid_cast<const ColumnInt8 *>(&(*(processed_col))))                \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::Int8);                  \
            else if (const auto * ptr = typeid_cast<const ColumnInt16 *>(&(*(processed_col))))               \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::Int16);                 \
            else if (const auto * ptr = typeid_cast<const ColumnInt32 *>(&(*(processed_col))))               \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::Int32);                 \
            else if (const auto * ptr = typeid_cast<const ColumnInt64 *>(&(*(processed_col))))               \
                Param<ParamInt<false>, false>(param_name)(col_size, &(ptr->getData()), IntType::Int64);                 \
            else                                                                                               \
                throw Exception("Invalid int type int regexp function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT); \
        }                                                                                                      \
    } while (0);

// Method to convert const int column
#define CONVERT_CONST_INT_COL_TO_PARAM(param_name, processed_col, next_process)                                                              \
    do                                                                                                                                       \
    {                                                                                                                                        \
        size_t col_size = (processed_col)->size();                                                                                           \
        const auto * col_const = typeid_cast<const ColumnConst *>(&(*(processed_col)));                                                      \
        if (col_const != nullptr)                                                                                                            \
        {                                                                                                                                    \
            auto col_const_data = col_const->getDataColumnPtr();                                                                             \
            if (col_const_data->isColumnNullable())                                                                                          \
            {                                                                                                                                \
                /* This is a const nullable column */                                                                                        \
                Field field;                                                                                                                 \
                col_const->get(0, field);                                                                                                    \
                auto data_int64 = field.get<Int64>();                                                                                             \
                /* type template of ParamInt is useless when column is const, so we can arbitrary designate a valid as template parameter */ \
                Param<ParamInt<true>, true>(param_name)(col_size, data_int64);                                                        \
                next_process;                                                                                                                \
            }                                                                                                                                \
            else                                                                                                                             \
            {                                                                                                                                \
                /* type template of ParamInt is useless when column is const, so we can arbitrary designate a valid as template parameter */ \
                auto data_int64 = col_const->getValue<Int64>();                                                                                   \
                Param<ParamInt<true>, false>(param_name)(col_size, data_int64);                                                       \
                next_process;                                                                                                                \
            }                                                                                                                                \
        }                                                                                                                                    \
        else                                                                                                                                 \
            CONVERT_NULL_INT_COL_TO_PARAM((param_name), (processed_col), next_process)                                                      \
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

    template <typename ExprT, typename MatchTypeT>
    void memorize(const ExprT & pat_param, const MatchTypeT & match_type_param, TiDB::TiDBCollatorPtr collator) const
    {
        String final_pattern = pat_param.getString(0);
        if (unlikely(final_pattern.empty()))
            throw Exception(EMPTY_PAT_ERR_MSG);

        String match_type = match_type_param.getString(0);
        final_pattern = addMatchTypeForPattern(final_pattern, match_type, collator);

        int flags = getDefaultFlags();
        memorized_re = std::make_unique<Regexps::Regexp>(final_pattern, flags);
    }

    // Check if we can memorize the regexp
    template <typename PatT, typename MatchTypeT>
    constexpr static bool canMemorize()
    {
        return (PatT::isConst() && MatchTypeT::isConst());
    }

    bool isMemorized() const { return memorized_re != nullptr; }

    const std::unique_ptr<Regexps::Regexp> & getRegexp() const { return memorized_re; }

    static void checkInputArg(const DataTypePtr & arg, bool is_str, bool * has_nullable_col, bool * has_data_type_nothing)
    {
        if (is_str)
        {
            // Check string type argument
            if (arg->isNullable())
            {
                *has_nullable_col = true;
                const auto * null_type = checkAndGetDataType<DataTypeNullable>(arg.get());
                if (null_type == nullptr)
                    throw Exception("Get unexpected nullptr in FunctionStringRegexpInstr", ErrorCodes::LOGICAL_ERROR);

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
                {
                    // It may be DataTypeNothing if it's not string
                    if (arg->getTypeId() != TypeIndex::Nothing)
                        throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    else
                        *has_data_type_nothing = true;
                }
            }
        }
        else
        {
            // Check int type argument
            if (arg->isNullable())
            {
                *has_nullable_col = true;
                const auto * null_type = checkAndGetDataType<DataTypeNullable>(arg.get());
                if (null_type == nullptr)
                    throw Exception("Get unexpected nullptr in FunctionStringRegexpInstr", ErrorCodes::LOGICAL_ERROR);
                
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
                {
                    // It may be DataTypeNothing if it's not string
                    if (arg->getTypeId() != TypeIndex::Nothing)
                        throw Exception(fmt::format("Illegal type {} of argument of regexp function", arg->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    else
                        *has_data_type_nothing = true;
                }
            }
        }
    }

private:
    // We should pre compile the regular expression when:
    //  - only pattern column is provided and it's a constant column
    //  - pattern and match type columns are provided and they are both constant columns
    mutable std::unique_ptr<Regexps::Regexp> memorized_re;
};

// regexp and regexp_like functions are executed in this macro
#define EXECUTE_REGEXP_LIKE()                                                                                                  \
    do                                                                                                                         \
    {                                                                                                                          \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(RES_ARG_VAR_NAME, EXPR_PARAM_VAR_NAME, PAT_PARAM_VAR_NAME, MATCH_TYPE_PARAM_VAR_NAME); \
    } while (0);

// Method to convert match type column
#define CONVERT_MATCH_TYPE_COL_TO_PARAM()                                                                                     \
    do                                                                                                                        \
    {                                                                                                                         \
        if ((ARG_NUM_VAR_NAME) == 3)                                                                                          \
            CONVERT_CONST_STR_COL_TO_PARAM(MATCH_TYPE_PARAM_VAR_NAME, MATCH_TYPE_COL_PTR_VAR_NAME, ({EXECUTE_REGEXP_LIKE()})) \
        else                                                                                                                  \
        {                                                                                                                     \
            /* match_type is not provided here and set default values */                                                                             \
            Param<ParamDefault, false> MATCH_TYPE_PARAM_VAR_NAME(-1, StringRef("", 0));                                       \
            EXECUTE_REGEXP_LIKE()                                                                                             \
        }                                                                                                                     \
    } while (0);

// Method to convert pattern column
#define CONVERT_PAT_COL_TO_PARAM()                                                                                      \
    do                                                                                                                  \
    {                                                                                                                   \
        CONVERT_CONST_STR_COL_TO_PARAM(PAT_PARAM_VAR_NAME, PAT_COL_PTR_VAR_NAME, ({CONVERT_MATCH_TYPE_COL_TO_PARAM()})) \
    } while (0);

// Method to convert expression column
#define CONVERT_EXPR_COL_TO_PARAM()                                                                                \
    do                                                                                                             \
    {                                                                                                              \
        CONVERT_CONST_STR_COL_TO_PARAM(EXPR_PARAM_VAR_NAME, EXPR_COL_PTR_VAR_NAME, ({CONVERT_PAT_COL_TO_PARAM()})) \
    } while (0);

// The entry to convert columns to params and execute regexp_xxx functions
#define CONVERT_COLS_TO_PARAMS_AND_EXECUTE() \
    do                                       \
    {                                        \
        CONVERT_EXPR_COL_TO_PARAM()          \
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
            throw Exception("Too mant arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

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

        // Check memorization
        if constexpr (canMemorize<PatT, MatchTypeT>())
            memorize(pat_param, match_type_param, collator);

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(col_size, 0);

        constexpr bool has_nullable_col = ExprT::isNullableCol() || PatT::isNullableCol() || MatchTypeT::isNullableCol();

        // Start to match
        if (isMemorized())
        {
            const auto & regexp = getRegexp();
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

        if (null_presence.has_const_null_col || null_presence.has_data_type_nothing)
        {
            // There is a const null column in the input
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & EXPR_COL_PTR_VAR_NAME = block.getByPosition(arguments[0]).column;
        const ColumnPtr & PAT_COL_PTR_VAR_NAME = block.getByPosition(arguments[1]).column;

        size_t ARG_NUM_VAR_NAME = arguments.size();
        auto & RES_ARG_VAR_NAME = block.getByPosition(result);

        ColumnPtr MATCH_TYPE_COL_PTR_VAR_NAME;
        if ((ARG_NUM_VAR_NAME) == REGEXP_LIKE_MAX_PARAM_NUM)
            MATCH_TYPE_COL_PTR_VAR_NAME = block.getByPosition(arguments[2]).column;

        CONVERT_COLS_TO_PARAMS_AND_EXECUTE()
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;
};

#undef CONVERT_COLS_TO_PARAMS_AND_EXECUTE
#undef CONVERT_EXPR_COL_TO_PARAM
#undef CONVERT_PAT_COL_TO_PARAM
#undef CONVERT_MATCH_TYPE_COL_TO_PARAM
#undef EXECUTE_REGEXP_LIKE

#define EXECUTE_REGEXP_INSTR() \
    do \
    { \
        REGEXP_CLASS_MEM_FUNC_IMPL_NAME(RES_ARG_VAR_NAME, EXPR_PARAM_VAR_NAME, PAT_PARAM_VAR_NAME, POS_PARAM_VAR_NAME, OCCUR_PARAM_VAR_NAME, RET_OP_PARAM_VAR_NAME, MATCH_TYPE_PARAM_VAR_NAME); \
    } while (0);

// Method to convert match type column to param
#define CONVERT_MATCH_TYPE_COL_TO_PARAM()                                                                                          \
    do                                                                                                                             \
    {                                                                                                                              \
        if (ARG_NUM_VAR_NAME == REGEXP_INSTR_MAX_PARAM_NUM) \
        { \
            CONVERT_CONST_STR_COL_TO_PARAM(MATCH_TYPE_PARAM_VAR_NAME, MATCH_TYPE_COL_PTR_VAR_NAME, ({EXECUTE_REGEXP_INSTR()})) \
        } \
        else if (ARG_NUM_VAR_NAME == REGEXP_MIN_PARAM_NUM + 3) \
        { \
            /* match_type is not provided here and set default values */                                                                             \
            Param<ParamDefault, false> MATCH_TYPE_PARAM_VAR_NAME(-1, StringRef("", 0));                                       \
            EXECUTE_REGEXP_INSTR() \
        } \
        else if (ARG_NUM_VAR_NAME == REGEXP_MIN_PARAM_NUM + 2) \
        { \
            /* return_option and match_type are not provided here and set default values */                                                                             \
            Param<ParamDefault, false> RET_OP_PARAM_VAR_NAME(-1, 0); \
            Param<ParamDefault, false> MATCH_TYPE_PARAM_VAR_NAME(-1, StringRef("", 0));                                       \
            EXECUTE_REGEXP_INSTR() \
        } \
        else if (ARG_NUM_VAR_NAME == REGEXP_MIN_PARAM_NUM + 1) \
        { \
            /* occurrence, return_option and match_type are not provided here and set default values */                                                                             \
            Param<ParamDefault, false> OCCUR_PARAM_VAR_NAME(-1, 1); \
            Param<ParamDefault, false> RET_OP_PARAM_VAR_NAME(-1, 0); \
            Param<ParamDefault, false> MATCH_TYPE_PARAM_VAR_NAME(-1, StringRef("", 0));                                       \
            EXECUTE_REGEXP_INSTR() \
        } \
        else \
        { \
            /* position, occurrence, return_option and match_type are not provided here and set default values */                                                                             \
            Param<ParamDefault, false> POS_PARAM_VAR_NAME(-1, 1); \
            Param<ParamDefault, false> OCCUR_PARAM_VAR_NAME(-1, 1); \
            Param<ParamDefault, false> RET_OP_PARAM_VAR_NAME(-1, 0); \
            Param<ParamDefault, false> MATCH_TYPE_PARAM_VAR_NAME(-1, StringRef("", 0));                                       \
            EXECUTE_REGEXP_INSTR() \
        } \
    } while (0);

// Method to convert return option column
#define CONVERT_RET_OP_COL_TO_PARAM() \
    do \
    { \
        CONVERT_CONST_INT_COL_TO_PARAM(RET_OP_PARAM_VAR_NAME, RET_OP_COL_PTR_VAR_NAME, ({CONVERT_MATCH_TYPE_COL_TO_PARAM()})) \
    } while (0);

// Method to convert occurrence column to param
#define CONVERT_OCCUR_COL_TO_PARAM() \
    do \
    { \
        CONVERT_CONST_INT_COL_TO_PARAM(OCCUR_PARAM_VAR_NAME, OCCUR_COL_PTR_VAR_NAME, ({CONVERT_RET_OP_COL_TO_PARAM()})) \
    } while (0);

// Method to convert position column to param
#define CONVERT_POS_COL_TO_PARAM() \
    do \
    { \
        CONVERT_CONST_INT_COL_TO_PARAM(POS_PARAM_VAR_NAME, POS_COL_PTR_VAR_NAME, ({CONVERT_OCCUR_COL_TO_PARAM()})) \
    } while (0);

// Method to convert pattern column to param
#define CONVERT_PAT_COL_TO_PARAM()                                                                                           \
    do                                                                                                                       \
    {                                                                                                                        \
        CONVERT_CONST_STR_COL_TO_PARAM(PAT_PARAM_VAR_NAME, PAT_COL_PTR_VAR_NAME, ({CONVERT_POS_COL_TO_PARAM()})) \
    } while (0);

// Method to convert expression column to param
#define CONVERT_EXPR_COL_TO_PARAM()                                                                                 \
    do                                                                                                              \
    {                                                                                                               \
        CONVERT_CONST_STR_COL_TO_PARAM(EXPR_PARAM_VAR_NAME, EXPR_COL_PTR_VAR_NAME, ({CONVERT_PAT_COL_TO_PARAM()})) \
    } while (0);

// The entry to convert columns to params and execute regexp_xxx functions
#define CONVERT_COLS_TO_PARAMS_AND_EXECUTE() \
    do                                       \
    {                                        \
        CONVERT_EXPR_COL_TO_PARAM()          \
    } while (0);

// Implementation of regexp_instr function
template <typename Name>
class FunctionStringRegexpInstr : public FunctionStringRegexpBase
    , public IFunction
{
public:
    using ResultType = Int64;
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringRegexpInstr>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { COLLATOR_VAR_NAME = collator_; }
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
    void REGEXP_CLASS_MEM_FUNC_IMPL_NAME(ColumnWithTypeAndName & res_arg, const ExprT & expar_param, const PatT & par_param, const PosT & pos_param, const OccurT & occur_param, const RetOpT & ret_op_param, const MatchTypeT & match_type_param) const
    {
        size_t col_size = expar_param.getDataNum();

        // Check if args are all const columns
        if constexpr (ExprT::isConst() && PatT::isConst() && PosT::isConst() && OccurT::isConst() && RetOpT::isConst() && MatchTypeT::isConst())
        {
            int flags = getDefaultFlags();
            String expr = expar_param.getString(0);
            String pat = par_param.getString(0);
            if (unlikely(pat.empty()))
                throw Exception(EMPTY_PAT_ERR_MSG);

            Int64 pos = pos_param.template getInt<Int64>(0);
            Int64 occur = occur_param.template getInt<Int64>(0);
            Int64 ret_op = ret_op_param.template getInt<Int64>(0);
            String match_type = match_type_param.getString(0);

            Regexps::Regexp regexp(addMatchTypeForPattern(pat, match_type, COLLATOR_VAR_NAME), flags);
            ResultType res = regexp.instr(expr.c_str(), expr.size(), pos, occur, ret_op);
            res_arg.column = res_arg.type->createColumnConst(col_size, toField(res));
            return;
        }

        // Check memorization
        if constexpr (canMemorize<PatT, MatchTypeT>())
            memorize(par_param, match_type_param, COLLATOR_VAR_NAME);

        // Initialize result column
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & VEC_RES_VAR_NAME = col_res->getData();
        VEC_RES_VAR_NAME.resize(col_size, 0);

        constexpr bool has_nullable_col = ExprT::isNullableCol() || PatT::isNullableCol() || PosT::isNullableCol() || OccurT::isNullableCol() || RetOpT::isNullableCol() || MatchTypeT::isNullableCol();

        // Get function pointers to process the specific int type
        GetIntFuncPointerType get_pos_func = getGetIntFuncPointer(pos_param.getIntType());
        GetIntFuncPointerType get_occur_func = getGetIntFuncPointer(occur_param.getIntType());
        GetIntFuncPointerType get_ret_op_func = getGetIntFuncPointer(ret_op_param.getIntType());

        const void * pos_container =  pos_param.getContainer();
        const void * occur_container =  occur_param.getContainer();
        const void * ret_op_container =  ret_op_param.getContainer();

        Int64 pos_const_val = PosT::isConst() ? pos_param.template getInt<Int64>(0) : -1;
        Int64 occur_const_val = OccurT::isConst() ? occur_param. template getInt<Int64>(0) : -1;
        Int64 ret_op_const_val = RetOpT::isConst() ? ret_op_param. template getInt<Int64>(0) : -1;

#define GET_POS_VALUE(idx) \
    do \
    { \
        if constexpr (PosT::isConst()) \
            pos = pos_const_val; \
        else \
            pos = get_pos_func(pos_container, idx); \
    } while (0);

#define GET_OCCUR_VALUE(idx) \
    do \
    { \
        if constexpr (OccurT::isConst()) \
            occur = occur_const_val; \
        else \
            occur = get_occur_func(occur_container, idx); \
    } while (0);

#define GET_RET_OP_VALUE(idx) \
    do \
    { \
        if constexpr (RetOpT::isConst()) \
            ret_op = ret_op_const_val; \
        else \
            ret_op = get_ret_op_func(ret_op_container, idx); \
    } while (0);

        StringRef expr_ref;
        String pat;
        Int64 pos;
        Int64 occur;
        Int64 ret_op;
        String match_type;

        // Start to execute instr
        if (isMemorized())
        {
            // Codes in this if-condition execute instr with memorized regexp
            const auto & regexp = getRegexp();
            if constexpr (has_nullable_col)
            {
                // Process nullable columns with memorized regexp
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & null_map = nullmap_col->getData();
                null_map.resize(col_size);

                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expar_param.isNullAt(i) || pos_param.isNullAt(i) || occur_param.isNullAt(i) || ret_op_param.isNullAt(i))
                    {
                        null_map[i] = 1;
                        continue;
                    }
                    null_map[i] = 0;
                    expar_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    VEC_RES_VAR_NAME[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }
                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                // Process pure vector columns with memorized regexp.
                // columns are impossible to be a nullable column here.

                for (size_t i = 0; i < col_size; ++i)
                {
                    expar_param.getStringRef(i, expr_ref);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    VEC_RES_VAR_NAME[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }
                res_arg.column = std::move(col_res);
            }
        }
        else
        {
            // Codes in this if-condition execute instr without memorized regexp
            if constexpr (has_nullable_col)
            {
                // Process nullable columns without memorized regexp
                auto nullmap_col = ColumnUInt8::create();
                typename ColumnUInt8::Container & null_map = nullmap_col->getData();
                null_map.resize(col_size);

                for (size_t i = 0; i < col_size; ++i)
                {
                    if (expar_param.isNullAt(i) || pos_param.isNullAt(i) || occur_param.isNullAt(i) || ret_op_param.isNullAt(i))
                    {
                        null_map[i] = 1;
                        continue;
                    }
                    null_map[i] = 0;
                    expar_param.getStringRef(i, expr_ref);
                    pat = par_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    match_type = match_type_param.getString(i);
                    auto regexp = createRegexpWithMatchType(pat, match_type, COLLATOR_VAR_NAME);
                    VEC_RES_VAR_NAME[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
                }

                res_arg.column = ColumnNullable::create(std::move(col_res), std::move(nullmap_col));
            }
            else
            {
                // Process pure vector columns without memorized regexp
                for (size_t i = 0; i < col_size; ++i)
                {
                    expar_param.getStringRef(i, expr_ref);
                    pat = par_param.getString(i);
                    if (unlikely(pat.empty()))
                        throw Exception(EMPTY_PAT_ERR_MSG);
                    GET_POS_VALUE(i)
                    GET_OCCUR_VALUE(i)
                    GET_RET_OP_VALUE(i)
                    match_type = match_type_param.getString(i);
                    auto regexp = createRegexpWithMatchType(pat, match_type, COLLATOR_VAR_NAME);
                    vec_res[i] = regexp->instr(expr_ref.data, expr_ref.size, pos, occur, ret_op);
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

        if (null_presence.has_const_null_col || null_presence.has_data_type_nothing)
        {
            // There is a const null column in the input
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & EXPR_COL_PTR_VAR_NAME = block.getByPosition(arguments[0]).column;
        const ColumnPtr & PAT_COL_PTR_VAR_NAME = block.getByPosition(arguments[1]).column;

        size_t ARG_NUM_VAR_NAME = arguments.size();
        auto & RES_ARG_VAR_NAME = block.getByPosition(result);

        ColumnPtr POS_COL_PTR_VAR_NAME;
        ColumnPtr OCCUR_COL_PTR_VAR_NAME;
        ColumnPtr RET_OP_COL_PTR_VAR_NAME;
        ColumnPtr MATCH_TYPE_COL_PTR_VAR_NAME;

        // Go through cases to get arguments
        switch((ARG_NUM_VAR_NAME))
        {
        case REGEXP_INSTR_MAX_PARAM_NUM:
            MATCH_TYPE_COL_PTR_VAR_NAME = block.getByPosition(arguments[5]).column;
        case REGEXP_MIN_PARAM_NUM + 3:
            RET_OP_COL_PTR_VAR_NAME = block.getByPosition(arguments[4]).column;
        case REGEXP_MIN_PARAM_NUM + 2:
            OCCUR_COL_PTR_VAR_NAME = block.getByPosition(arguments[3]).column;
        case REGEXP_MIN_PARAM_NUM + 1:
            POS_COL_PTR_VAR_NAME = block.getByPosition(arguments[2]).column;
        };

        CONVERT_COLS_TO_PARAMS_AND_EXECUTE()
    }

private:
    TiDB::TiDBCollatorPtr COLLATOR_VAR_NAME = nullptr;
};

#undef CHOOSE_TYPE_AND_EXECUTE
#undef CHOOSE_AND_EXEC_FOR_POS_PARAM
#undef CHOOSE_AND_EXEC_FOR_OCCUR_PARAM
#undef CHOOSE_AND_EXEC_FOR_RET_OP_PARAM

#undef CONVERT_COLS_TO_PARAMS_AND_EXECUTE
#undef CONVERT_EXPR_COL_TO_PARAM
#undef CONVERT_PAT_COL_TO_PARAM
#undef CONVERT_POS_COL_TO_PARAM
#undef CONVERT_OCCUR_COL_TO_PARAM
#undef CONVERT_RET_OP_COL_TO_PARAM
#undef CONVERT_MATCH_TYPE_COL_TO_PARAM
#undef EXECUTE_REGEXP_INSTR

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

#undef CONVERT_CONST_STR_COL_TO_PARAM
#undef CONVERT_NULL_STR_COL_TO_PARAM
#undef REGEXP_CLASS_MEM_FUNC_IMPL_NAME
#undef COLLATOR_VAR_NAME
#undef VEC_RES_VAR_NAME
#undef ARG_NUM_VAR_NAME
#undef SELF_CLASS_NAME
#undef MATCH_TYPE_PARAM_VAR_NAME
#undef PAT_PARAM_VAR_NAME
#undef EXPR_PARAM_VAR_NAME
#undef RES_ARG_VAR_NAME
#undef MATCH_TYPE_COL_PTR_VAR_NAME
#undef PAT_COL_PTR_VAR_NAME
#undef EXPR_COL_PTR_VAR_NAME

} // namespace DB
