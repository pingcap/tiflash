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

#include <cstring>
#include <memory>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

inline const char * EMPTY_PAT_ERR_MSG = "Empty pattern is invalid";

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
struct NameRegexpSubstr
{
    static constexpr auto name = "regexp_substr";
};
struct NameRegexpReplace
{
    static constexpr auto name = "regexp_replace";
};

static constexpr std::string_view regexp_like_name(NameRegexpLike::name);

enum class IntType
{
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64
};

using GetIntFuncPointerType = Int64 (*)(const void *, size_t);

namespace FunctionsRegexp
{
inline int getDefaultFlags()
{
    int flags = 0;
    flags
        |= OptimizedRegularExpressionImpl<false>::RE_NO_CAPTURE | OptimizedRegularExpressionImpl<false>::RE_NO_OPTIMIZE;
    return flags;
}

template <bool need_subpattern = false>
inline String addMatchTypeForPattern(const String & pattern, const String & match_type, TiDB::TiDBCollatorPtr collator)
{
    if (pattern.empty())
        throw Exception("Length of the pattern argument must be greater than 0.");

    String mode = re2Util::getRE2ModeModifiers(match_type, collator);
    String final_pattern;
    if constexpr (need_subpattern)
        final_pattern = fmt::format("({})", pattern);
    else
        final_pattern = pattern;

    if (mode.empty())
        return final_pattern;

    final_pattern = fmt::format("{}{}", mode, final_pattern);
    return final_pattern;
}

template <bool need_subpattern = false>
inline Regexps::Regexp createRegexpWithMatchType(
    const String & pattern,
    const String & match_type,
    TiDB::TiDBCollatorPtr collator,
    int flags = 0)
{
    String final_pattern = addMatchTypeForPattern<need_subpattern>(pattern, match_type, collator);
    if (flags == 0)
        return Regexps::createRegexp<false>(final_pattern, getDefaultFlags());
    else
        return Regexps::createRegexp<false>(final_pattern, flags);
}

// Only int types used in ColumnsNumber.h can be valid
template <typename T>
inline constexpr bool check_int_type()
{
    return std::is_same_v<
               T,
               UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64> || std::is_same_v<T, Int8> || std::is_same_v<T, Int16> || std::is_same_v<T, Int32> || std::is_same_v<T, Int64>;
}

inline Int64 getIntFromField(Field & field)
{
    switch (field.getType())
    {
    case Field::Types::Int64:
        return field.safeGet<Int64>();
    case Field::Types::UInt64:
        return field.safeGet<UInt64>();
    default:
        throw Exception("Unexpected int type");
    }
}

template <typename T>
inline Int64 getInt(const void * container, size_t idx)
{
    const auto * tmp = reinterpret_cast<const typename ColumnVector<T>::Container *>(container);
    return static_cast<Int64>((*tmp)[idx]);
}

inline GetIntFuncPointerType getGetIntFuncPointer(IntType int_type)
{
    switch (int_type)
    {
    case IntType::UInt8:
        return &getInt<UInt8>;
    case IntType::UInt16:
        return &getInt<UInt16>;
    case IntType::UInt32:
        return &getInt<UInt32>;
    case IntType::UInt64:
        return &getInt<UInt64>;
    case IntType::Int8:
        return &getInt<Int8>;
    case IntType::Int16:
        return &getInt<Int16>;
    case IntType::Int32:
        return &getInt<Int32>;
    case IntType::Int64:
        return &getInt<Int64>;
    default:
        throw Exception("Unexpected int type");
    }
}

// We need to fill something into StringColumn when all elements are null
inline void fillColumnStringWhenAllNull(decltype(ColumnString::create()) & col_res, size_t size)
{
    auto & col_res_data = col_res->getChars();
    auto & col_res_offsets = col_res->getOffsets();
    col_res_data.resize(size);
    col_res_offsets.resize(size);

    size_t offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        col_res_data[offset++] = 0;
        col_res_offsets[i] = offset;
    }
}
} // namespace FunctionsRegexp

template <bool is_const>
class ParamString
{
public:
    DISALLOW_COPY_AND_MOVE(ParamString);

    using Chars_t = ColumnString::Chars_t;
    using Offsets = ColumnString::Offsets;

    // For passing compilation
    explicit ParamString(Int64)
        : const_string_data(nullptr)
        , const_string_data_size(0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    explicit ParamString(const StringRef & str_ref)
        : const_string_data(nullptr)
        , const_string_data_size(0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        // Deep copy
        const_string_data = new char[str_ref.size];
        memcpy(const_string_data, str_ref.data, str_ref.size);
        const_string_data_size = str_ref.size;
        if constexpr (!is_const)
            throw Exception("non-const parm should not call this constructor");
    }

    // For passing compilation
    explicit ParamString(const void *)
        : const_string_data(nullptr)
        , const_string_data_size(0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        throw Exception("Shouldn't call this constructor");
    }

    ParamString(const void * chars_, const void * offsets_)
        : const_string_data(nullptr)
        , const_string_data_size(0)
        , chars(reinterpret_cast<const Chars_t *>(chars_))
        , offsets(reinterpret_cast<const Offsets *>(offsets_))
    {
        if constexpr (is_const)
            throw Exception("const parm should not call this constructor");
    }

    ParamString(const void *, IntType)
        : const_string_data(nullptr)
        , const_string_data_size(0)
        , chars(nullptr)
        , offsets(nullptr)
    {
        throw Exception("ParamString should not call this constructor");
    }

    ~ParamString() { delete[] const_string_data; }

    static IntType getIntType() { throw Exception("ParamString not supports this function"); }

    template <typename T>
    Int64 getInt(size_t) const
    {
        throw Exception("ParamString not supports this function");
    }

    String getString(size_t idx) const
    {
        if constexpr (is_const)
            return String(const_string_data, const_string_data_size);
        else
            return String(reinterpret_cast<const char *>(&(*chars)[offsetAt(idx)]), sizeAt(idx) - 1);
    }

    void getStringRef(size_t idx, StringRef & dst) const
    {
        if constexpr (is_const)
        {
            dst.data = const_string_data;
            dst.size = const_string_data_size;
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

    char * const_string_data;
    size_t const_string_data_size;

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
        , int_type(IntType::Int64)
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
            const auto * tmp = reinterpret_cast<
                const typename ColumnVector<std::enable_if_t<FunctionsRegexp::check_int_type<T>(), T>>::Container *>(
                int_container);
            return static_cast<Int64>((*tmp)[idx]);
        }
    }

    IntType getIntType() const { return int_type; }
    String getString(size_t) const { throw Exception("ParamInt not supports this function"); }
    void getStringRef(size_t, StringRef &) const { throw Exception("ParamInt not supports this function"); }
    const void * getContainer() const { return int_container; }
    constexpr static bool isConst() { return is_const; }

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
    Param(size_t col_size_, const void * int_container_, IntType int_type)
        : col_size(col_size_)
        , null_map(nullptr)
        , data(int_container_, int_type)
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
    Param(size_t col_size_, ConstNullMapPtr null_map_, const void * int_container_, IntType int_type)
        : col_size(col_size_)
        , null_map(null_map_)
        , data(int_container_, int_type)
    {
        checkNullableLogic();
    }

    template <typename T>
    Int64 getInt(size_t idx) const
    {
        return data.template getInt<T>(idx);
    }
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

    IntType getIntType() const { return data.getIntType(); }
    size_t getDataNum() const { return col_size; }
    const void * getContainer() const { return data.getContainer(); }
    constexpr static bool isNullableCol() { return is_nullable; }
    constexpr static bool isConst() { return ParamImplType::isConst(); }

private:
    // When this is a nullable param, we should ensure the null_map is not nullptr
    void checkNullableLogic()
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
    ParamVariant(ColumnPtr col, size_t col_size, Int64 default_val)
        : col_ptr(col)
        , param(nullptr)
    {
        if (col_ptr != nullptr)
        {
            setParamIntTypeAndGenerateParam(col_size);
        }
        else
        {
            // This param is not provided by user, so we should use default value.
            param = new ParamIntNotNullableAndConst(col_size, default_val);
            param_type = ParamType::IntNotNullableAndConst;
        }
    }

    ~ParamVariant()
    {
        if (param != nullptr)
        {
            switch (param_type)
            {
                // Expand the macro to enumerate string param cases
                APPLY_FOR_PARAM_STRING_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, param, actual_param_ptr, ({
                                                    delete actual_param_ptr;
                                                }))

                // Expand the macro to enumerate int param cases
                APPLY_FOR_PARAM_INT_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, param, actual_param_ptr, ({
                                                 delete actual_param_ptr;
                                             }))
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
            param = new ParamStringNullableAndNotConst(
                col_size,
                null_map,
                static_cast<const void *>(&(tmp->getChars())),
                static_cast<const void *>(&(tmp->getOffsets())));
            param_type = ParamType::StringNullableAndNotConst;
        }
        else
        {
            // This is a pure string vector column
            const auto * tmp = checkAndGetColumn<ColumnString>(&(*(col_ptr)));

            // Construct actual param
            param = new ParamStringNotNullableAndNotConst(
                col_size,
                static_cast<const void *>(&(tmp->getChars())),
                static_cast<const void *>(&(tmp->getOffsets())));
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

private:
    void handleIntConstCol(size_t col_size, const ColumnConst * col_const)
    {
        Field field;
        col_const->get(0, field);
        auto data_int64 = field.isNull() ? -1 : FunctionsRegexp::getIntFromField(field);
        const auto & col_const_data = col_const->getDataColumnPtr();
        if (col_const_data->isColumnNullable())
        {
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(col_const_data)).getNullMapData());

            // Construct actual param
            param = new ParamIntNullableAndConst(col_size, data_int64, null_map);
            param_type = ParamType::IntNullableAndConst;
        }
        else
        {
            // Construct actual param
            param = new ParamIntNotNullableAndConst(col_size, data_int64);
            param_type = ParamType::IntNotNullableAndConst;
        }
    }

#define APPLY_FOR_INT_CONTAINER(M, col_ptr, null_map, param) \
    M(UInt8, col_ptr, null_map, param)                       \
    M(UInt16, col_ptr, null_map, param)                      \
    M(UInt32, col_ptr, null_map, param)                      \
    M(UInt64, col_ptr, null_map, param)                      \
    M(Int8, col_ptr, null_map, param)                        \
    M(Int16, col_ptr, null_map, param)                       \
    M(Int32, col_ptr, null_map, param)                       \
    M(Int64, col_ptr, null_map, param)

    void handleIntNonConstCol(size_t col_size)
    {
        if (col_ptr->isColumnNullable())
        {
            auto nested_ptr = static_cast<const ColumnNullable &>(*(col_ptr)).getNestedColumnPtr();
            const auto * null_map = &(static_cast<const ColumnNullable &>(*(col_ptr)).getNullMapData());

            // Construct actual param
            param_type = ParamType::IntNullableAndNotConst;

#define M(INT_TYPE, col_ptr, null_map, param)                                         \
    else if (const auto * ptr = typeid_cast<const Column##INT_TYPE *>(&(*(col_ptr)))) \
    {                                                                                 \
        (param) = new ParamIntNullableAndNotConst(                                    \
            col_size,                                                                 \
            null_map,                                                                 \
            reinterpret_cast<const void *>(&(ptr->getData())),                        \
            IntType::INT_TYPE);                                                       \
    }

            if (false) {}
            APPLY_FOR_INT_CONTAINER(M, nested_ptr, null_map, param)
            else throw Exception("Invalid int type int regexp function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

#undef M
        }
        else
        {
            // Construct actual param
            param_type = ParamType::IntNotNullableAndNotConst;

#define M(INT_TYPE, col_ptr, null_map, param)                                         \
    else if (const auto * ptr = typeid_cast<const Column##INT_TYPE *>(&(*(col_ptr)))) \
    {                                                                                 \
        (param) = new ParamIntNotNullableAndNotConst(                                 \
            col_size,                                                                 \
            reinterpret_cast<const void *>(&(ptr->getData())),                        \
            IntType::INT_TYPE);                                                       \
    }

            if (false) {}
            APPLY_FOR_INT_CONTAINER(M, col_ptr, null_map, param)
            else throw Exception("Invalid int type int regexp function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

#undef M
        }
    }

#undef APPLY_FOR_INT_CONTAINER

    void setParamIntTypeAndGenerateParam(size_t col_size)
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(&(*(col_ptr)));
        if (col_const != nullptr)
            handleIntConstCol(col_size, col_const);
        else
            handleIntNonConstCol(col_size);
    }

    ParamType param_type;
    ColumnPtr col_ptr;

public:
    // ATTENTION! Be careful to change this variable's name as many macros use it
    //
    // This variable should be reinterpret_cast to specific type before used
    void * param;
};

// Macros defined here should be undefined in FunctionsRegexpUndef.h

// Unifying these names is necessary in macros
#define EXPR_PV_VAR_NAME expr_pv
#define PAT_PV_VAR_NAME pat_pv
#define POS_PV_VAR_NAME pos_pv
#define OCCUR_PV_VAR_NAME occur_pv
#define RET_OP_PV_VAR_NAME return_option_pv
#define MATCH_TYPE_PV_VAR_NAME match_type_pv
#define REPL_PV_VAR_NAME repl_pv

#define EXPR_PARAM_PTR_VAR_NAME expr_param
#define PAT_PARAM_PTR_VAR_NAME pat_param
#define POS_PARAM_PTR_VAR_NAME pos_param
#define OCCUR_PARAM_PTR_VAR_NAME occur_param
#define RET_OP_PARAM_PTR_VAR_NAME return_option_param
#define MATCH_TYPE_PARAM_PTR_VAR_NAME match_type_param
#define REPL_PARAM_PTR_VAR_NAME repl_param

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

// Common method to get actual int param
#define GET_ACTUAL_INT_PARAM(pv_name, param_name, next_process)                                                    \
    do                                                                                                             \
    {                                                                                                              \
        switch ((pv_name).getParamType())                                                                          \
        {                                                                                                          \
            /* Expand this macro to enumerate all int cases */                                                     \
            APPLY_FOR_PARAM_INT_VARIANTS(ENUMERATE_PARAM_VARIANT_CASES, (pv_name).param, param_name, next_process) \
        default:                                                                                                   \
            throw Exception("Unexpected ParamType");                                                               \
        }                                                                                                          \
    } while (0);

class FunctionStringRegexpBase
{
public:
    static constexpr size_t REGEXP_MIN_PARAM_NUM = 2;
    static constexpr size_t REGEXP_REPLACE_MIN_PARAM_NUM = 3;

    // Max parameter number the regexp_xxx function could receive
    static constexpr size_t REGEXP_MAX_PARAM_NUM = 2;
    static constexpr size_t REGEXP_LIKE_MAX_PARAM_NUM = 3;
    static constexpr size_t REGEXP_INSTR_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_REPLACE_MAX_PARAM_NUM = 6;
    static constexpr size_t REGEXP_SUBSTR_MAX_PARAM_NUM = 5;

    // We should pre compile the regular expression when:
    //  - only pattern column is provided and it's a constant column
    //  - pattern and match type columns are provided and they are both constant columns
    template <bool need_subpattern, typename ExprT, typename MatchTypeT>
    std::unique_ptr<Regexps::Regexp> memorize(
        const ExprT & pat_param,
        const MatchTypeT & match_type_param,
        TiDB::TiDBCollatorPtr collator,
        int flags = 0) const
    {
        if (pat_param.isNullAt(0) || match_type_param.isNullAt(0))
            return nullptr;

        String final_pattern = pat_param.getString(0);
        if (unlikely(final_pattern.empty())) // TODO delete it
            throw Exception(EMPTY_PAT_ERR_MSG);

        String match_type = match_type_param.getString(0);
        final_pattern = FunctionsRegexp::addMatchTypeForPattern<need_subpattern>(final_pattern, match_type, collator);

        if (flags == 0)
            return std::make_unique<Regexps::Regexp>(final_pattern, FunctionsRegexp::getDefaultFlags());
        else
            return std::make_unique<Regexps::Regexp>(final_pattern, flags);
    }

    // Check if we can memorize the regexp
    template <typename PatT, typename MatchTypeT>
    constexpr static bool canMemorize()
    {
        return (PatT::isConst() && MatchTypeT::isConst());
    }

    static void checkInputArg(
        const DataTypePtr & arg,
        bool is_str,
        bool * has_nullable_col,
        bool * has_data_type_nothing)
    {
        if (is_str)
            checkStringTypeArg(arg, has_nullable_col, has_data_type_nothing);
        else
            checkIntTypeArg(arg, has_nullable_col, has_data_type_nothing);
    }

private:
    static void checkStringTypeArg(const DataTypePtr & arg, bool * has_nullable_col, bool * has_data_type_nothing)
    {
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
                    throw Exception(
                        fmt::format("Illegal type {} of argument of regexp function", arg->getName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                else
                    *has_data_type_nothing = true;
            }
        }
        else
        {
            if (!arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument of regexp function", arg->getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static void checkIntTypeArg(const DataTypePtr & arg, bool * has_nullable_col, bool * has_data_type_nothing)
    {
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
                    throw Exception(
                        fmt::format("Illegal type {} of argument of regexp function", arg->getName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                else
                    *has_data_type_nothing = true;
            }
        }
        else
        {
            if (!arg->isInteger())
                throw Exception(
                    fmt::format("Illegal type {} of argument of regexp function", arg->getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};
} // namespace DB
