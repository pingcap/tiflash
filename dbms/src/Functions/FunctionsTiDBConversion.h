#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionsDateTime.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <ext/collection_cast.h>
#include <ext/enumerate.h>
#include <ext/range.h>
#include <type_traits>


namespace DB
{

/// cast int as int , cast int as real , cast real as int , cast real as real
template <typename FromDataType, typename ToDataType, bool return_nullable>
struct TiDBConvertImpl
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    static constexpr bool from_integer = std::is_integral_v<FromFieldType>;
    static constexpr bool to_integer = std::is_integral_v<ToFieldType>;
    static constexpr bool from_floating_point = std::is_floating_point_v<FromFieldType>;
    static constexpr bool to_floating_point = std::is_floating_point_v<ToFieldType>;
    static constexpr bool from_unsigned = std::is_unsigned_v<FromFieldType>;
    static constexpr bool to_unsigned = std::is_unsigned_v<ToFieldType>;

    static void execute(
        Block & block, const ColumnNumbers & arguments, size_t result, bool in_union, const tipb::FieldType &, const Context &)
    {
        if (const ColumnVector<FromFieldType> * col_from
            = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            size_t size = block.getByPosition(arguments[0]).column->size();
            ColumnUInt8::MutablePtr col_null_map_to;
            //ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            if constexpr (return_nullable)
            {
                col_null_map_to = ColumnUInt8::create(size);
                //vec_null_map_to = &col_null_map_to->getData();
            }
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            auto col_to = ColumnVector<ToFieldType>::create();
            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
            vec_to.resize(size);

            if constexpr (from_integer && to_integer)
            {
                /// cast int as int, do not care about overflow because the target type should be Int64/UInt64
                if ((from_unsigned && to_unsigned) || from_unsigned || !in_union)
                {
                    for (size_t i = 0; i < size; ++i)
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                }
                else
                {
                    /// cast signed to unsigned in union context
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (vec_from[i] < 0)
                            vec_to[i] = 0;
                        else
                            vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                    }
                }
            }
            if constexpr (from_floating_point && to_floating_point)
            {
                /// cast real as real, do not care about overflow because the target type should be float64
                // todo support unsigned real
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }
            if constexpr (from_integer && to_floating_point)
            {
                /// cast integer as real
                // todo support unsigned real
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }
            if constexpr (from_floating_point && to_integer)
            {
                /// cast real as integer
                // todo support overflow checking
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }

            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/** special case: FromDataType and ToDataType are are identical.
  */
template <typename T>
struct TiDBConvertImpl<std::enable_if_t<!T::is_parametric, T>, T, false>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType &, const Context &)
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};

/// cast int/real/decimal/time as string
template <typename FromDataType, bool return_nullable>
struct TiDBConvertImpl<FromDataType, std::enable_if_t<!std::is_same_v<FromDataType, DataTypeString>, DataTypeString>, return_nullable>
{
    using FromFieldType = typename FromDataType::FieldType;
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType &, const Context &)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        if (const auto col_from = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get()))
        {
            auto col_to = ColumnString::create();
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();

            if constexpr (std::is_same_v<FromDataType, DataTypeMyDate>)
                data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
            if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss") + 1 + (type.getFraction() ? 0 : 1 + type.getFraction())));
            else
                data_to.resize(size * 3);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            for (size_t i = 0; i < size; ++i)
            {
                FormatImpl<FromDataType>::execute(vec_from[i], write_buffer, &type, nullptr);
                // todo check max length
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function TiDB_cast",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <typename DecimalDataType, typename ToDataType, bool return_nullable>
struct TiDBConvertFromDecimal
{
    using DecimalFieldType = typename DecimalDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    static void execute(Block & block [[maybe_unused]], const ColumnNumbers & arguments [[maybe_unused]], size_t result [[maybe_unused]],
        bool, const tipb::FieldType &, const Context &)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (std::is_floating_point_v<ToFieldType> || std::is_integral_v<ToFieldType>)
        {
            const auto * col_from = checkAndGetColumn<ColumnDecimal<DecimalFieldType>>(block.getByPosition(arguments[0]).column.get());
            auto col_to = ColumnVector<ToFieldType>::create();

            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                auto field = (*col_from)[i].template safeGet<DecimalField<DecimalFieldType>>();
                // todo support unsigned float, support overflow check for integer
                vec_to[i] = static_cast<ToFieldType>(field);
            }

            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else if constexpr (std::is_same_v<String, ToFieldType>)
        {

            const auto * col_from = checkAndGetColumn<ColumnDecimal<DecimalFieldType>>(block.getByPosition(arguments[0]).column.get());
            auto col_to = ColumnString::create();

            const typename ColumnDecimal<DecimalFieldType>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();

            data_to.resize(size * decimal_max_prec + size);

            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                writeText(vec_from[i], vec_from.getScale(), write_buffer);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());

            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/// cast int/real/decimal/time/string as int
template <typename FromDataType, typename ToDataType, bool return_nullable, bool to_unsigned>
struct TiDBConvertToInteger
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename T, typename ToFieldType>
    static std::enable_if_t<std::is_floating_point_v<T>, ToFieldType> toUInt(const T & value, const Context &)
    {
        T rounded_value = std::round(value);
        if (rounded_value < 0) {
            // todo handle overflow error, check if need clip to zero
            return static_cast<ToFieldType>(rounded_value);
        }
        if (rounded_value > std::numeric_limits<ToFieldType>::max())
            // todo handle overflow error
            return std::numeric_limits<ToFieldType>::max();
        else if (rounded_value == std::numeric_limits<ToFieldType>::max())
            return std::numeric_limits<ToFieldType>::max();
        else
            return static_cast<ToFieldType>(rounded_value);
    }

    template <typename T, typename ToFieldType>
    static std::enable_if_t<std::is_floating_point_v<T>, ToFieldType> toInt(const T & value, const Context &)
    {
        T rounded_value = std::round(value);
        if (rounded_value < std::numeric_limits<ToFieldType>::min())
            // todo handle overflow check
            return std::numeric_limits<ToFieldType>::min();
        if (rounded_value >= std::numeric_limits<ToFieldType>::max())
        {
            // todo handle overflow check when round_value > max()
            return std::numeric_limits<ToFieldType>::max();
        }
        return static_cast<ToFieldType>(rounded_value);
    }

    template <typename T, typename ToFieldType>
    static std::enable_if_t<isDecimalField<T>, ToFieldType> toUInt(const T & value, const Context &)
    {
        auto v = value.dec.value;
        if (v < 0)
            // todo check overflow
            return static_cast<ToFieldType>(0);
        ScaleType scale = value.getScale();
        for (ScaleType i = 0; i < scale; i++)
        {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (v > std::numeric_limits<ToFieldType>::max())
        {
            // todo check overflow
            return std::numeric_limits<ToFieldType>::max();
        }
        return static_cast<ToFieldType>(v);
    }

    template <typename T, typename ToFieldType>
    static std::enable_if_t<isDecimalField<T>, ToFieldType> toInt(const T & value, const Context &)
    {
        auto v = value.dec.value;
        ScaleType scale = value.getScale();
        for (ScaleType i = 0; i < scale; i++)
        {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (v > std::numeric_limits<ToFieldType>::max() || v < std::numeric_limits<ToFieldType>::min())
        {
            // todo overflow check
            if (v > 0)
                return std::numeric_limits<ToFieldType>::max();
            return std::numeric_limits<ToFieldType>::min();
        }
        return static_cast<ToFieldType>(v);
    }

    static StringRef trim(const StringRef & value)
    {
        StringRef ret;
        ret.size = 0;
        size_t start = 0;
        static std::unordered_set<const char> spaces{' ','\t','\n','\v','\f','\r',' ',0x85, 0xA0};
        for (; start < value.size; start++)
        {
            if (!spaces.count(value.data[start]))
                break;
        }
        size_t end = ret.size;
        for (; start < end; end--)
        {
            if (!spaces.count(value.data[end-1]))
                break;
        }
        if (start >= end)
            return ret;
        ret.data = value.data + start;
        ret.size = end - start;
        return ret;
    }
    //template <typename ToFieldType>
    //static ToFieldType toInt(const StringRef & value, const Context &)
    //{
    //    // trim space
    //    StringRef trim_string = trim(value);
    //    if (trim_string.size == 0)
    //        // todo handle truncated error
    //        return static_cast<ToFieldType>(0);
    //}

    //template <typename ToFieldType>
    //static ToFieldType toUInt(const StringRef & value, const Context &)
    //{
    //
    //}

    static void execute(
            Block & block, const ColumnNumbers & arguments, size_t result, bool in_union, const tipb::FieldType &, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();

        auto col_to = ColumnVector<ToFieldType>::create();
        typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
        vec_to.resize(size);

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as int
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());

            for (size_t i = 0; i < size; ++i)
            {
                auto field = (*col_from)[i].template safeGet<DecimalField<FromFieldType>>();
                // todo support unsigned float, support overflow check for integer
                vec_to[i] = static_cast<ToFieldType>(field);
                if constexpr (to_unsigned)
                {
                    vec_to[i] = toUInt<DecimalField<FromFieldType>, ToFieldType>(field, context);
                }
                else
                {
                    vec_to[i] = toInt<DecimalField<FromFieldType>, ToFieldType>(field, context);
                }
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime> || std::is_same_v<FromDataType, DataTypeMyDate>)
        {
            /// cast time as int
            const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
            const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

            const ColumnVector<FromFieldType> * col_from
                    = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            for (size_t i = 0; i < size; i++)
            {
                if constexpr (std::is_same_v<DataTypeMyDate, FromDataType>)
                {
                    MyDate date(vec_from[i]);
                    vec_to[i] = date.year * 10000 + date.month * 100 + date.day;
                }
                else
                {
                    MyDateTime date_time(vec_from[i]);
                    vec_to[i] = date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 100000
                                               + date_time.hour * 1000 + date_time.minute * 100 + date_time.second;
                }
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            // todo support cast string as int
            /// cast string as int
            const IColumn * col_from = block.getByPosition(arguments[0]).column.get();
            const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
            const ColumnString::Chars_t * chars = &col_from_string->getChars();
            const IColumn::Offsets * offsets = &col_from_string->getOffsets();
            size_t current_offset = 0;
            for (size_t i = 0; i < size; i++)
            {
                size_t next_offset = (*offsets)[i];
                size_t string_size = next_offset - current_offset - 1;
                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);
            }
        }
        else if constexpr (std::is_integral_v<FromFieldType>)
        {
            /// cast int as int
            const ColumnVector<FromFieldType> * col_from =
                    checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            constexpr bool from_unsigned = std::is_unsigned_v<FromFieldType>;
            if ((from_unsigned && to_unsigned) || from_unsigned || !in_union)
            {
                for (size_t i = 0; i < size; i++)
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }
            else
            {
                /// cast signed to unsigned in union context
                for (size_t i = 0; i < size; ++i)
                {
                    if (vec_from[i] < 0)
                        vec_to[i] = 0;
                    else
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                }
            }
        }
        else if constexpr (std::is_floating_point_v<FromFieldType>)
        {
            /// cast real as int
            const ColumnVector<FromFieldType> * col_from =
                    checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            if constexpr (to_unsigned)
            {
                for (size_t i = 0; i < size; i++)
                    vec_to[i] = toUInt<FromFieldType, ToFieldType>(vec_from[i], context);
            }
            else
            {
                for (size_t i = 0; i < size; i++)
                    vec_to[i] = toInt<FromFieldType, ToFieldType>(vec_from[i], context);
            }
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() +
                " of first argument of function tidb_cast", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

};
/// cast int/real/decimal/time/string as decimal
template <typename FromDataType, typename ToFieldType, bool return_nullable>
struct TiDBConvertToDecimal
{
    using FromFieldType = typename FromDataType::FieldType;

    template <typename T, typename U>
    static U ToTiDBDecimalInternal(T value, PrecType prec, ScaleType scale)
    {
        using UType = typename U::NativeType;
        auto maxValue = DecimalMaxValue::Get(prec);
        if (value > maxValue || value < -maxValue)
        {
            // todo should throw exception or log warnings based on the flag in dag request
            if (value > 0)
                return static_cast<UType>(maxValue);
            else
                return static_cast<UType>(-maxValue);
        }
        UType scale_mul = getScaleMultiplier<U>(scale);
        U result = static_cast<UType>(value) * scale_mul;
        return result;
    }

    template <typename U>
    static U ToTiDBDecimal(MyDateTime & date_time, PrecType prec, ScaleType scale, bool in_union, const tipb::FieldType & tp, int fsp)
    {
        UInt64 value_without_fsp = date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 100000
            + date_time.hour * 1000 + date_time.minute * 100 + date_time.second;
        if (fsp > 0)
        {
            Int128 value = value_without_fsp * 1000000 + date_time.micro_second;
            Decimal128 decimal(value);
            return ToTiDBDecimal<Decimal128, U>(decimal, 6, prec, scale, in_union, tp);
        }
        else
        {
            return ToTiDBDecimalInternal<UInt64, U>(value_without_fsp, prec, scale);
        }
    }

    template <typename U>
    static U ToTiDBDecimal(MyDate & date, PrecType prec, ScaleType scale, bool, const tipb::FieldType &)
    {
        UInt64 value = date.year * 10000 + date.month * 100 + date.day;
        return ToTiDBDecimalInternal<UInt64, U>(value, prec, scale);
    }

    template <typename T, typename U>
    static std::enable_if_t<std::is_integral_v<T>, U> ToTiDBDecimal(
        T value, PrecType prec, ScaleType scale, bool in_union, const tipb::FieldType & tp)
    {
        using UType = typename U::NativeType;
        constexpr bool from_unsigned = std::is_unsigned_v<T>;
        if (!from_unsigned && !hasUnsignedFlag(tp))
            return ToTiDBDecimalInternal<T, U>(value, prec, scale);
        else if (unlikely(in_union && !from_unsigned && value < 0))
            /// return zero
            return static_cast<UType>(0);
        else
            return ToTiDBDecimalInternal<UInt64, U>(static_cast<UInt64>(value), prec, scale);
    }

    template <typename T, typename U>
    static std::enable_if_t<std::is_floating_point_v<T>, U> ToTiDBDecimal(
        T value, PrecType prec, ScaleType scale, bool in_union, const tipb::FieldType & tp)
    {
        using UType = typename U::NativeType;
        /// copied from TiDB code, might be some bugs here
        if (!in_union || value >= 0)
        {
            bool neg = false;
            if (value < 0)
            {
                if (unlikely(hasUnsignedFlag(tp)))
                    return static_cast<UType>(0);
                neg = true;
                value = -value;
            }
            for (ScaleType i = 0; i < scale; i++)
            {
                value *= 10;
            }
            auto max_value = DecimalMaxValue::Get(prec);
            if (std::abs(value) > static_cast<Float64>(max_value))
            {
                if (value > 0)
                    return static_cast<UType>(max_value);
                else
                    return static_cast<UType>(-max_value);
            }
            // rounding
            T tenTimesValue = value * 10;
            UType v(value);
            if (Int256(tenTimesValue) % 10 >= 5)
            {
                v++;
            }
            if (neg)
            {
                v = -v;
            }
            return v;
        }
        else
            return static_cast<UType>(0);
    }

    template <typename T, typename U>
    static std::enable_if_t<IsDecimal<T>, U> ToTiDBDecimal(
        const T & v, ScaleType v_scale, PrecType prec, ScaleType scale, bool in_union, const tipb::FieldType & tp)
    {
        using UType = typename U::NativeType;
        auto value = Int256(v.value);
        if (unlikely(in_union && hasUnsignedFlag(tp) && value < 0))
            return static_cast<UType>(0);

        if (v_scale <= scale)
        {
            for (ScaleType i = v_scale; i < scale; i++)
                value *= 10;
        }
        else
        {
            bool need2Round = false;
            for (ScaleType i = scale; i < v_scale; i++)
            {
                need2Round = (value < 0 ? -value : value) % 10 >= 5;
                value /= 10;
            }
            if (need2Round)
            {
                if (value < 0)
                    value--;
                else
                    value++;
            }
        }
        if (unlikely(hasUnsignedFlag(tp) && value < 0))
            return static_cast<UType>(0);

        auto max_value = DecimalMaxValue::Get(prec);
        if (value > max_value || value < -max_value)
        {
            // todo should throw exception or log warnings based on the flag in dag request
            if (value > 0)
                return static_cast<UType>(max_value);
            else
                return static_cast<UType>(-max_value);
        }
        return static_cast<UType>(value);
    }

    template <typename T, typename U>
    static std::enable_if_t<!IsDecimal<T>, U> ToTiDBDecimal(const T & /*v*/, ScaleType /*v_scale*/, ScaleType /*scale*/)
    {
        throw Exception("Should not call here", ErrorCodes::LOGICAL_ERROR);
    }

    /// cast int/real/time/decimal as decimal
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, PrecType prec [[maybe_unused]], ScaleType scale,
        bool in_union, const tipb::FieldType & tp, const Context &)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as decimal
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            auto col_to = ColumnDecimal<ToFieldType>::create(0, scale);

            const typename ColumnDecimal<FromFieldType>::Container & vec_from = col_from->getData();
            typename ColumnDecimal<ToFieldType>::Container & vec_to = col_to->getData();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                vec_to[i] = ToTiDBDecimal<FromFieldType, ToFieldType>(vec_from[i], vec_from.getScale(), prec, scale, in_union, tp);
            }

            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else if constexpr (std::is_same_v<DataTypeMyDateTime, FromDataType> || std::is_same_v<DataTypeMyDate, FromDataType>)
        {
            /// cast time as decimal
            const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
            const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get());
            auto col_to = ColumnDecimal<ToFieldType>::create(0, scale);

            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            typename ColumnDecimal<ToFieldType>::Container & vec_to = col_to->getData();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (std::is_same_v<DataTypeMyDate, FromDataType>)
                {
                    MyDate date(vec_from[i]);
                    vec_to[i] = ToTiDBDecimal<ToFieldType>(date, prec, scale, in_union, tp);
                }
                else
                {
                    MyDateTime date_time(vec_from[i]);
                    vec_to[i] = ToTiDBDecimal<ToFieldType>(date_time, prec, scale, in_union, tp, type.getFraction());
                }
            }
            if constexpr (return_nullable)
                block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                block.getByPosition(result).column = std::move(col_to);
        }
        else if constexpr (std::is_same_v<DataTypeString, FromDataType>)
        {
            /// cast string as decimal
        }
        else
        {
            /// cast int/real as decimal
            if (const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
            {
                auto col_to = ColumnDecimal<ToFieldType>::create(0, scale);

                const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
                typename ColumnDecimal<ToFieldType>::Container & vec_to = col_to->getData();
                vec_to.resize(size);

                for (size_t i = 0; i < size; ++i)
                {
                    vec_to[i] = ToTiDBDecimal<FromFieldType, ToFieldType>(vec_from[i], prec, scale, in_union, tp);
                }

                if constexpr (return_nullable)
                    block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
                else
                    block.getByPosition(result).column = std::move(col_to);
            }
            else
                throw Exception(
                    "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

class PreparedFunctionTiDBCast : public PreparedFunctionImpl
{
public:
    using WrapperType = std::function<void(Block &, const ColumnNumbers &, size_t, bool, const tipb::FieldType &, const Context &)>;

    PreparedFunctionTiDBCast(
        WrapperType && wrapper_function, const char * name_, bool in_union_, const tipb::FieldType & tidb_tp_, const Context & context_)
        : wrapper_function(std::move(wrapper_function)), name(name_), in_union(in_union_), tidb_tp(tidb_tp_), context(context_)
    {}

    String getName() const override { return name; }

protected:
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        ColumnNumbers new_arguments{arguments.front()};
        wrapper_function(block, new_arguments, result, in_union, tidb_tp, context);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char * name;
    bool in_union;
    const tipb::FieldType & tidb_tp;
    const Context & context;
};

class FunctionTiDBCast final : public IFunctionBase
{
public:
    using WrapperType = std::function<void(Block &, const ColumnNumbers &, size_t, bool, const tipb::FieldType &, const Context &)>;
    using MonotonicityForRange = std::function<Monotonicity(const IDataType &, const Field &, const Field &)>;

    FunctionTiDBCast(const Context & context, const char * name, MonotonicityForRange && monotonicity_for_range,
        const DataTypes & argument_types, const DataTypePtr & return_type, bool in_union_, const tipb::FieldType & tidb_tp_)
        : context(context),
          name(name),
          monotonicity_for_range(monotonicity_for_range),
          argument_types(argument_types),
          return_type(return_type),
          in_union(in_union_),
          tidb_tp(tidb_tp_)
    {}

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block & /*sample_block*/) const override
    {
        return std::make_shared<PreparedFunctionTiDBCast>(
            prepare(getArgumentTypes()[0], getReturnType()), name, in_union, tidb_tp, context);
    }

    String getName() const override { return name; }

    bool hasInformationAboutMonotonicity() const override
    {
        //return static_cast<bool>(monotonicity_for_range);
        return false;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }

private:
    const Context & context;
    const char * name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    bool in_union;
    const tipb::FieldType & tidb_tp;

    template <typename FromDataType, bool return_nullable>
    WrapperType createWrapperFromCommonType(const DataTypePtr & to_type) const
    {
        if (checkDataType<DataTypeUInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertImpl<FromDataType, DataTypeUInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertImpl<FromDataType, DataTypeInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (const auto decimal_type = checkAndGetDataType<DataTypeDecimal32>(to_type.get()))
            return [decimal_type](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_,
                       const tipb::FieldType & tidb_tp_, const Context & context_) {
                TiDBConvertToDecimal<FromDataType, DataTypeDecimal32::FieldType, return_nullable>::execute(
                    block, arguments, result, decimal_type->getPrec(), decimal_type->getScale(), in_union_, tidb_tp_, context_);
            };
        if (const auto decimal_type = checkAndGetDataType<DataTypeDecimal64>(to_type.get()))
            return [decimal_type](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_,
                       const tipb::FieldType & tidb_tp_, const Context & context_) {
                TiDBConvertToDecimal<FromDataType, DataTypeDecimal64::FieldType, return_nullable>::execute(
                    block, arguments, result, decimal_type->getPrec(), decimal_type->getScale(), in_union_, tidb_tp_, context_);
            };
        if (const auto decimal_type = checkAndGetDataType<DataTypeDecimal128>(to_type.get()))
            return [decimal_type](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_,
                       const tipb::FieldType & tidb_tp_, const Context & context_) {
                TiDBConvertToDecimal<FromDataType, DataTypeDecimal128::FieldType, return_nullable>::execute(
                    block, arguments, result, decimal_type->getPrec(), decimal_type->getScale(), in_union_, tidb_tp_, context_);
            };
        if (const auto decimal_type = checkAndGetDataType<DataTypeDecimal256>(to_type.get()))
            return [decimal_type](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_,
                       const tipb::FieldType & tidb_tp_, const Context & context_) {
                TiDBConvertToDecimal<FromDataType, DataTypeDecimal256::FieldType, return_nullable>::execute(
                    block, arguments, result, decimal_type->getPrec(), decimal_type->getScale(), in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeFloat64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertImpl<FromDataType, DataTypeFloat64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        /*
        if (checkDataType<DataTypeString>(to_type.get()))
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result, const Context & context1)
            {
                TiDBConvertImpl<FromDataType, DataTypeString, return_nullable>::execute(block, arguments, result, context1);
            };
        if (checkDataType<DataTypeMyDate>(to_type.get()))
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result, const Context & context1)
            {
                TiDBConvertImpl<FromDataType, DataTypeMyDate, return_nullable>::execute(block, arguments, result, context1);
            };
        if (checkDataType<DataTypeMyDateTime>(to_type.get()))
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result, const Context & context1)
            {
                TiDBConvertImpl<FromDataType, DataTypeMyDateTime, return_nullable>::execute(block, arguments, result, context1);
            };
            */

        // todo support convert to duration/json type
        throw Exception{"Conversion to " + to_type->getName() + " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
    }

    template <typename DecimalDataType, bool return_nullable>
    WrapperType createWrapperFromDecimal(const DataTypePtr & to_type) const
    {
        if (checkDataType<DataTypeUInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertFromDecimal<DecimalDataType, DataTypeUInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertFromDecimal<DecimalDataType, DataTypeInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeFloat64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertFromDecimal<DecimalDataType, DataTypeFloat64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeString>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertFromDecimal<DecimalDataType, DataTypeString, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        throw Exception{"Conversion Decimal type to to " + to_type->getName() + " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
    }

    WrapperType createIdentityWrapper(const DataTypePtr &) const
    {
        return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool, const tipb::FieldType &, const Context &) {
            block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
        };
    }

    template <bool return_nullable>
    WrapperType createWrapper(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt8>(from_type.get()))
            return createWrapperFromCommonType<DataTypeUInt8, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt16>(from_type.get()))
            return createWrapperFromCommonType<DataTypeUInt16, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt32>(from_type.get()))
            return createWrapperFromCommonType<DataTypeUInt32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt64>(from_type.get()))
            return createWrapperFromCommonType<DataTypeUInt64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt8>(from_type.get()))
            return createWrapperFromCommonType<DataTypeInt8, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt16>(from_type.get()))
            return createWrapperFromCommonType<DataTypeInt16, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt32>(from_type.get()))
            return createWrapperFromCommonType<DataTypeInt32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt64>(from_type.get()))
            return createWrapperFromCommonType<DataTypeInt64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeFloat32>(from_type.get()))
            return createWrapperFromCommonType<DataTypeFloat32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeFloat64>(from_type.get()))
            return createWrapperFromCommonType<DataTypeFloat64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal32>(from_type.get()))
            return createWrapperFromDecimal<DataTypeDecimal32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal64>(from_type.get()))
            return createWrapperFromDecimal<DataTypeDecimal64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal128>(from_type.get()))
            return createWrapperFromDecimal<DataTypeDecimal128, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal256>(from_type.get()))
            return createWrapperFromDecimal<DataTypeDecimal256, return_nullable>(to_type);
        /*
        if (const auto from_actual_type = checkAndGetDataType<DataTypeMyDate>(from_type.get()))
            return createWrapperForToType<DataTypeMyDate, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeMyDateTime>(from_type.get()))
            return createWrapperForToType<DataTypeMyDateTime, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeString>(from_type.get()))
            return createWrapperFromCommonType<DataTypeString, return_nullable>(to_type);
            */

        // todo support convert to duration/json type
        throw Exception{
            "Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
    }

    WrapperType prepare(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        if (from_type->onlyNull())
        {
            return [](Block & block, const ColumnNumbers &, const size_t result, bool, const tipb::FieldType &, const Context &) {
                auto & res = block.getByPosition(result);
                res.column = res.type->createColumnConstWithDefaultValue(block.rows())->convertToFullColumnIfConst();
            };
        }

        DataTypePtr from_inner_type = removeNullable(from_type);
        DataTypePtr to_inner_type = removeNullable(to_type);

        return prepareImpl(from_inner_type, to_inner_type);
    }

    WrapperType prepareImpl(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        if (from_type->equals(*to_type))
            return createIdentityWrapper(from_type);
        bool return_nullable = to_type->isNullable();
        if (return_nullable)
            return createWrapper<true>(from_type, to_type);
        else
            return createWrapper<false>(from_type, to_type);
    }
};

class FunctionBuilderTiDBCast : public FunctionBuilderImpl
{
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;

    static constexpr auto name = "tidb_cast";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderTiDBCast>(context); }

    FunctionBuilderTiDBCast(const Context & context) : context(context) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    void setInUnion(bool in_union_) { in_union = in_union_; }
    void setTiDBFieldType(const tipb::FieldType * tidb_tp_) { tidb_tp = tidb_tp_; }


protected:
    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, std::shared_ptr<TiDB::ITiDBCollator>) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_shared<FunctionTiDBCast>(context, name, std::move(monotonicity), data_types, return_type, in_union, *tidb_tp);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto type_col = checkAndGetColumnConst<ColumnString>(arguments.back().column.get());
        if (!type_col)
            throw Exception(
                "Second argument to " + getName() + " must be a constant string describing type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypeFactory::instance().get(type_col->getValue<String>());
    }

private:
    // todo support monotonicity
    //template <typename DataType>
    //static auto monotonicityForType(const DataType * const)
    //{
    //    return FunctionTo<DataType>::Type::Monotonic::get;
    //}

    MonotonicityForRange getMonotonicityInformation(const DataTypePtr &, const IDataType *) const
    {
        /*
        if (const auto type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt8>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt16>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeDate>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeDateTime>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeString>(to_type))
            return monotonicityForType(type);
        else if (from_type->isEnum())
        {
            if (const auto type = checkAndGetDataType<DataTypeEnum8>(to_type))
                return monotonicityForType(type);
            else if (const auto type = checkAndGetDataType<DataTypeEnum16>(to_type))
                return monotonicityForType(type);
        }
         */
        return {};
    }

    const Context & context;
    bool in_union;
    const tipb::FieldType * tidb_tp;
};

} // namespace DB
