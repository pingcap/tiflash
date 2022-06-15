#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/FieldVisitors.h>
#include <Common/MyTime.h>
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
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
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
#include <Storages/Transaction/Collator.h>

#include <ext/collection_cast.h>
#include <ext/enumerate.h>
#include <ext/range.h>
#include <type_traits>


namespace DB
{

String trim(const StringRef & value);

enum CastError
{
    NONE = 0,
    TRUNCATED_ERR,
    OVERFLOW_ERR,
};

/// cast int/real/decimal/time as string
template <typename FromDataType, bool return_nullable>
struct TiDBConvertToString
{
    using FromFieldType = typename FromDataType::FieldType;

    static size_t charLengthToByteLengthFromUTF8(const char * data, size_t length, size_t char_length)
    {
        size_t ret = 0;
        for (size_t char_index = 0; char_index < char_length && ret < length; char_index++)
        {
            uint8_t c = data[ret];
            if (c < 0x80)
                ret += 1;
            else if (c < 0xE0)
                ret += 2;
            else if (c < 0xF0)
                ret += 3;
            else
                ret += 4;
        }
        return ret;
    }

    static void execute(
        Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType & tp, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size, 0);
            vec_null_map_to = &col_null_map_to->getData();
        }
        bool need_padding = tp.tp() == TiDB::TypeString && tp.flen() > 0 && tp.collate() == TiDB::ITiDBCollator::BINARY;

        String padding_string;
        if (need_padding)
            padding_string.resize(tp.flen(), 0);

        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        auto col_to = ColumnString::create();
        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            /// cast string as string
            const IColumn * col_from = block.getByPosition(arguments[0]).column.get();
            const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
            const ColumnString::Chars_t * data_from = &col_from_string->getChars();
            const IColumn::Offsets * offsets_from = &col_from_string->getOffsets();

            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            size_t current_offset = 0;
            for (size_t i = 0; i < size; i++)
            {
                size_t next_offset = (*offsets_from)[i];
                size_t org_length = next_offset - current_offset - 1;
                size_t byte_length = org_length;
                if (tp.flen() > 0)
                {
                    byte_length = tp.flen();
                    if (tp.charset() == "utf8" || tp.charset() == "utf8mb4")
                        byte_length = charLengthToByteLengthFromUTF8(
                            reinterpret_cast<const char *>(&(*data_from)[current_offset]), org_length, byte_length);
                    byte_length = std::min(byte_length, org_length);
                }
                if (byte_length < org_length)
                    context.getDAGContext()->handleTruncateError("Data Too Long");
                write_buffer.write(reinterpret_cast<const char *>(&(*data_from)[current_offset]), byte_length);
                if (need_padding && byte_length < static_cast<size_t>(tp.flen()))
                    write_buffer.write(padding_string.data(), tp.flen() - byte_length);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
                current_offset = next_offset;
            }

            data_to.resize(write_buffer.count());
        }
        else if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as string
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnDecimal<FromFieldType>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t container_per_element;

            data_to.resize(size * decimal_max_prec + size);
            container_per_element.resize(decimal_max_prec);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                WriteBufferFromVector<ColumnString::Chars_t> element_write_buffer(container_per_element);
                FormatImpl<FromDataType>::execute(vec_from[i], element_write_buffer, &type, nullptr);
                size_t byte_length = element_write_buffer.count();
                if (tp.flen() > 0)
                    byte_length = std::min(byte_length, tp.flen());
                if (byte_length < element_write_buffer.count())
                    context.getDAGContext()->handleTruncateError("Data Too Long");
                write_buffer.write(reinterpret_cast<char *>(container_per_element.data()), byte_length);
                if (need_padding && byte_length < static_cast<size_t>(tp.flen()))
                    write_buffer.write(padding_string.data(), tp.flen() - byte_length);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());
        }
        else if (const auto col_from = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get()))
        {
            /// cast int/real/time as string
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t container_per_element;

            if constexpr (std::is_same_v<FromDataType, DataTypeMyDate>)
            {
                auto length = strlen("YYYY-MM-DD") + 1;
                data_to.resize(size * length);
                container_per_element.resize(length);
            }
            if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime>)
            {
                auto length = strlen("YYYY-MM-DD hh:mm:ss") + 1 + (type.getFraction() ? 0 : 1 + type.getFraction());
                data_to.resize(size * length);
                container_per_element.resize(length);
            }
            else
            {
                data_to.resize(size * 3);
                container_per_element.resize(3);
            }
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                WriteBufferFromVector<ColumnString::Chars_t> element_write_buffer(container_per_element);
                FormatImpl<FromDataType>::execute(vec_from[i], element_write_buffer, &type, nullptr);
                size_t byte_length = element_write_buffer.count();
                if (tp.flen() > 0)
                    byte_length = std::min(byte_length, tp.flen());
                if (byte_length < element_write_buffer.count())
                    context.getDAGContext()->handleTruncateError("Data Too Long");
                write_buffer.write(reinterpret_cast<char *>(container_per_element.data()), byte_length);
                if (need_padding && byte_length < static_cast<size_t>(tp.flen()))
                    write_buffer.write(padding_string.data(), tp.flen() - byte_length);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);

        if constexpr (return_nullable)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};

/// cast int/real/decimal/time/string as int
template <typename FromDataType, typename ToDataType, bool return_nullable>
struct TiDBConvertToInteger
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    static constexpr bool to_unsigned = std::is_unsigned_v<ToFieldType>;

    template <typename T, typename ToFieldType>
    static std::enable_if_t<std::is_floating_point_v<T>, ToFieldType> toUInt(const T & value, const Context & context)
    {
        T rounded_value = std::round(value);
        if (rounded_value < 0)
        {
            context.getDAGContext()->handleOverflowError("Cast real as integer", Errors::Types::Truncated);
            if (context.getDAGContext()->shouldClipToZero())
                return static_cast<ToFieldType>(0);
            return static_cast<ToFieldType>(rounded_value);
        }
        if (rounded_value > std::numeric_limits<ToFieldType>::max())
        {
            context.getDAGContext()->handleOverflowError("Cast real as integer", Errors::Types::Truncated);
            return std::numeric_limits<ToFieldType>::max();
        }
        else if (rounded_value == std::numeric_limits<ToFieldType>::max())
        {
            context.getDAGContext()->handleOverflowError("cast real as int", Errors::Types::Truncated);
            return std::numeric_limits<ToFieldType>::max();
        }
        else
            return static_cast<ToFieldType>(rounded_value);
    }

    template <typename T, typename ToFieldType>
    static std::enable_if_t<std::is_floating_point_v<T>, ToFieldType> toInt(const T & value, const Context & context)
    {
        T rounded_value = std::round(value);
        if (rounded_value < std::numeric_limits<ToFieldType>::min())
        {
            context.getDAGContext()->handleOverflowError("cast real as int", Errors::Types::Truncated);
            return std::numeric_limits<ToFieldType>::min();
        }
        if (rounded_value >= std::numeric_limits<ToFieldType>::max())
        {
            context.getDAGContext()->handleOverflowError("cast real as int", Errors::Types::Truncated);
            return std::numeric_limits<ToFieldType>::max();
        }
        return static_cast<ToFieldType>(rounded_value);
    }

    template <typename T, typename ToFieldType>
    static ToFieldType decToUInt(const DecimalField<T> & value, const Context & context)
    {
        auto v = value.getValue().value;
        if (v < 0)
        {
            context.getDAGContext()->handleOverflowError("cast decimal as int", Errors::Types::Truncated);
            return static_cast<ToFieldType>(0);
        }
        ScaleType scale = value.getScale();
        for (ScaleType i = 0; i < scale; i++)
        {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }

        Int128 max_value = std::numeric_limits<ToFieldType>::max();
        if (v > max_value)
        {
            context.getDAGContext()->handleOverflowError("cast decimal as int", Errors::Types::Truncated);
            return max_value;
        }
        return static_cast<ToFieldType>(v);
    }

    template <typename T, typename ToFieldType>
    static ToFieldType decToInt(const DecimalField<T> & value, const Context & context)
    {
        auto v = value.getValue().value;
        ScaleType scale = value.getScale();
        for (ScaleType i = 0; i < scale; i++)
        {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (v > std::numeric_limits<ToFieldType>::max() || v < std::numeric_limits<ToFieldType>::min())
        {
            context.getDAGContext()->handleOverflowError("cast decimal as int", Errors::Types::Truncated);
            if (v > 0)
                return std::numeric_limits<ToFieldType>::max();
            return std::numeric_limits<ToFieldType>::min();
        }
        return static_cast<ToFieldType>(v);
    }

    static StringRef getValidIntPrefix(const StringRef & value)
    {
        StringRef ret;
        ret.data = value.data;
        ret.size = 0;
        for (; ret.size < value.size; ret.size++)
        {
            char current = value.data[ret.size];
            if ((current >= '0' && current <= '9') || (ret.size == 0 && (current == '+' || current == '-')))
                continue;
            break;
        }
        return ret;
    }

    template <typename T>
    static std::tuple<T, CastError> toUInt(const StringRef & value)
    {
        static const T cut_off = std::numeric_limits<T>::max() / 10;
        if (value.data[0] == '-')
            return std::make_tuple(0, OVERFLOW_ERR);
        size_t pos = value.data[0] == '+' ? 1 : 0;
        T ret = 0;
        for (; pos < value.size; pos++)
        {
            if (ret > cut_off)
                /// overflow
                return std::make_tuple(std::numeric_limits<T>::max(), OVERFLOW_ERR);
            int next = value.data[pos] - '0';
            if (static_cast<T>(ret * 10 + next) < ret)
                /// overflow
                return std::make_tuple(std::numeric_limits<T>::max(), OVERFLOW_ERR);
            ret = ret * 10 + next;
        }

        return std::make_tuple(ret, NONE);
    }

    template <typename T>
    static std::tuple<T, CastError> toInt(const StringRef & value)
    {
        bool is_negative = false;
        UInt64 uint_value = 0;
        CastError err = NONE;
        if (value.data[0] == '-')
        {
            is_negative = true;
            StringRef uint_string(value.data + 1, value.size - 1);
            std::tie(uint_value, err) = toUInt<std::make_unsigned_t<T>>(uint_string);
        }
        else
        {
            std::tie(uint_value, err) = toUInt<std::make_unsigned_t<T>>(value);
        }
        if (err == OVERFLOW_ERR)
            return std::make_tuple(is_negative ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max(), err);
        // todo handle truncate error

        if (is_negative)
        {
            if (uint_value > std::numeric_limits<std::make_unsigned_t<T>>::max() / 2 + 1)
                return std::make_tuple(std::numeric_limits<T>::min(), OVERFLOW_ERR);
            return std::make_tuple(static_cast<T>(-uint_value), NONE);
        }
        else
        {
            if (uint_value > std::numeric_limits<T>::max())
                return std::make_tuple(std::numeric_limits<T>::max(), OVERFLOW_ERR);
            return std::make_tuple(static_cast<T>(uint_value), NONE);
        }
    }

    template <typename T>
    static T strToInt(const StringRef & value, const Context & context)
    {
        // trim space
        String trim_string = trim(value);
        if (trim_string.size() == 0)
        {
            if (value.size != 0)
                context.getDAGContext()->handleTruncateError("cast str as int");
            return static_cast<T>(0);
        }
        StringRef int_string = getValidIntPrefix(StringRef(trim_string));
        if (int_string.size == 0)
        {
            if (value.size != 0)
                context.getDAGContext()->handleTruncateError("cast str as int");
            return static_cast<T>(0);
        }
        bool is_negative = false;
        if (int_string.data[0] == '-')
        {
            is_negative = true;
        }
        if (!is_negative)
        {
            auto [value, err] = toUInt<T>(int_string);
            if (err == OVERFLOW_ERR)
                context.getDAGContext()->handleOverflowError("cast str as int", Errors::Types::Truncated);
            return static_cast<T>(value);
        }
        else
        {
            /// TODO: append warning CastAsSignedOverflow if try to cast negative value to unsigned
            auto [value, err] = toInt<T>(int_string);
            if (err == OVERFLOW_ERR)
                context.getDAGContext()->handleOverflowError("cast str as int", Errors::Types::Truncated);
            return static_cast<T>(value);
        }
    }

    static void execute(
        Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType &, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();

        auto col_to = ColumnVector<ToFieldType>::create(size, 0);
        typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size, 0);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as int
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());

            for (size_t i = 0; i < size; ++i)
            {
                auto field = (*col_from)[i].template safeGet<DecimalField<FromFieldType>>();
                if constexpr (to_unsigned)
                {
                    vec_to[i] = decToUInt<FromFieldType, ToFieldType>(field, context);
                }
                else
                {
                    vec_to[i] = decToInt<FromFieldType, ToFieldType>(field, context);
                }
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime> || std::is_same_v<FromDataType, DataTypeMyDate>)
        {
            /// cast time as int
            const auto & col_with_type_and_name = block.getByPosition(arguments[0]);

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
                    vec_to[i] = date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 1000000
                        + date_time.hour * 10000 + date_time.minute * 100 + date_time.second;
                }
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
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
                StringRef string_value(&(*chars)[current_offset], string_size);
                vec_to[i] = strToInt<ToFieldType>(string_value, context);
                current_offset = next_offset;
            }
        }
        else if constexpr (std::is_integral_v<FromFieldType>)
        {
            /// cast int as int
            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            for (size_t i = 0; i < size; i++)
                vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
        }
        else if constexpr (std::is_floating_point_v<FromFieldType>)
        {
            /// cast real as int
            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
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
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr (return_nullable)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};

/// cast int/real/decimal/time/string as real
template <typename FromDataType, typename ToDataType, bool return_nullable, bool to_unsigned>
struct TiDBConvertToFloat
{
    static_assert(std::is_same_v<ToDataType, DataTypeFloat32> || std::is_same_v<ToDataType, DataTypeFloat64>);
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    static Float64 produceTargetFloat64(Float64 value, bool need_truncate, Float64 shift, Float64 max_f, const Context & context)
    {
        if (need_truncate)
        {
            value *= shift;
            value = std::round(value) / shift;
            if (value > max_f)
            {
                context.getDAGContext()->handleOverflowError("cast as real", Errors::Types::Truncated);
                value = max_f;
            }
            if (value < -max_f)
            {
                context.getDAGContext()->handleOverflowError("cast as real", Errors::Types::Truncated);
                value = -max_f;
            }
        }
        if constexpr (to_unsigned)
        {
            if (value < 0)
            {
                context.getDAGContext()->handleOverflowError("cast as real", Errors::Types::Truncated);
                value = 0;
            }
        }
        return value;
    }

    template <typename T>
    static std::enable_if_t<std::is_floating_point_v<T> || std::is_integral_v<T>, Float64> toFloat(
        const T & value, bool need_truncate, Float64 shift, Float64 max_f, const Context & context)
    {
        Float64 float_value = static_cast<Float64>(value);
        return produceTargetFloat64(float_value, need_truncate, shift, max_f, context);
    }

    template <typename T>
    static std::enable_if_t<std::is_floating_point_v<T> || std::is_integral_v<T>, Float64> toFloat(const T & value)
    {
        return static_cast<Float64>(value);
    }

    template <typename T>
    static Float64 toFloat(const DecimalField<T> & value)
    {
        return static_cast<Float64>(value);
    }

    static StringRef getValidFloatPrefix(const StringRef & value)
    {
        StringRef ret;
        ret.data = value.data;
        ret.size = 0;
        bool sawDot = false;
        bool sawDigit = false;
        int eIdx = -1;
        int i = 0;
        for (; i < static_cast<int>(value.size); i++)
        {
            char c = ret.data[i];
            if (c == '+' || c == '-')
            {
                if (i != 0 && i != eIdx + 1)
                    // "1e+1" is valid.
                    break;
            }
            else if (c == '.')
            {
                if (sawDot || eIdx > 0)
                    // "1.1." or "1e1.1"
                    break;
                sawDot = true;
            }
            else if (c == 'e' || c == 'E')
            {
                if (!sawDigit)
                    // "+.e"
                    break;
                if (eIdx != -1)
                    // "1e5e"
                    break;
                eIdx = i;
            }
            else if (c < '0' || c > '9')
            {
                break;
            }
            else
            {
                sawDigit = true;
            }
        }
        ret.size = i;
        return ret;
    }

    static Float64 strToFloat(const StringRef & value, bool need_truncate, Float64 shift, Float64 max_f, const Context & context)
    {
        String trim_string = trim(value);
        StringRef float_string = getValidFloatPrefix(StringRef(trim_string));
        if (trim_string.size() == 0 && value.size != 0)
        {
            context.getDAGContext()->handleTruncateError("Truncated incorrect DOUBLE value");
            return 0.0;
        }
        if (float_string.size < trim_string.size())
            trim_string[float_string.size] = '\0';
        Float64 f = strtod(float_string.data, nullptr);
        if (f == std::numeric_limits<Float64>::infinity())
        {
            context.getDAGContext()->handleOverflowError("Truncated incorrect DOUBLE value", Errors::Types::Truncated);
            return std::numeric_limits<Float64>::max();
        }
        if (f == -std::numeric_limits<double>::infinity())
        {
            context.getDAGContext()->handleOverflowError("Truncated incorrect DOUBLE value", Errors::Types::Truncated);
            return -std::numeric_limits<Float64>::max();
        }
        return produceTargetFloat64(f, need_truncate, shift, max_f, context);
    }

    static void execute(
        Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType & tp, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();

        /// NOTICE: Since ToFieldType only can be Float32 or Float64, convert from_value to Float64 and then implicitly cast to ToFieldType is fine.
        auto col_to = ColumnVector<ToFieldType>::create(size, 0);
        typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size, 0);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as real
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());

            for (size_t i = 0; i < size; ++i)
            {
                auto & field = (*col_from)[i].template safeGet<DecimalField<FromFieldType>>();
                vec_to[i] = toFloat(field);
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime> || std::is_same_v<FromDataType, DataTypeMyDate>)
        {
            /// cast time as real
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
                    vec_to[i] = toFloat(date.year * 10000 + date.month * 100 + date.day);
                }
                else
                {
                    MyDateTime date_time(vec_from[i]);
                    if (type.getFraction() > 0)
                        vec_to[i] = toFloat(date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 100000
                            + date_time.hour * 1000 + date_time.minute * 100 + date_time.second + date_time.micro_second / 1000000.0);
                    else
                        vec_to[i] = toFloat(date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 100000
                            + date_time.hour * 1000 + date_time.minute * 100 + date_time.second);
                }
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            /// cast string as real
            const IColumn * col_from = block.getByPosition(arguments[0]).column.get();
            const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
            const ColumnString::Chars_t * chars = &col_from_string->getChars();
            const IColumn::Offsets * offsets = &col_from_string->getOffsets();
            size_t current_offset = 0;
            bool need_truncate = tp.flen() != -1 && tp.decimal() != -1 && tp.flen() >= tp.decimal();
            Float64 shift = 0;
            Float64 max_f = 0;
            if (need_truncate)
            {
                shift = std::pow((Float64)10, tp.flen());
                max_f = std::pow((Float64)10, tp.flen() - tp.decimal()) - 1.0 / shift;
            }
            for (size_t i = 0; i < size; i++)
            {
                size_t next_offset = (*offsets)[i];
                size_t string_size = next_offset - current_offset - 1;
                StringRef string_value(&(*chars)[current_offset], string_size);
                vec_to[i] = strToFloat(string_value, need_truncate, shift, max_f, context);
                current_offset = next_offset;
            }
        }
        else if constexpr (std::is_integral_v<FromFieldType> || std::is_floating_point_v<FromFieldType>)
        {
            /// cast int/real as real
            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            for (size_t i = 0; i < size; i++)
                vec_to[i] = toFloat(vec_from[i]);
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr (return_nullable)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};

/// cast int/real/decimal/time/string as decimal
// todo TiKV does not check unsigned flag but TiDB checks, currently follow TiKV's code, maybe changed latter
template <typename FromDataType, typename ToFieldType, bool return_nullable>
struct TiDBConvertToDecimal
{
    using FromFieldType = typename FromDataType::FieldType;

    template <typename T, typename U>
    static U toTiDBDecimalInternal(T int_value, PrecType prec, ScaleType scale, const Context & context)
    {
        // int_value is the value that exposes to user. Such as cast(val to decimal), val is the int_value which used by user.
        // And val * scale_mul is the scaled_value, which is stored in ColumnDecimal internally.
        static_assert(std::is_integral_v<T>);
        using UType = typename U::NativeType;
        UType scale_mul = getScaleMultiplier<U>(scale);

        Int256 scaled_value = static_cast<Int256>(int_value) * static_cast<Int256>(scale_mul);
        Int256 scaled_max_value = DecimalMaxValue::Get(prec);

        if (scaled_value > scaled_max_value || scaled_value < -scaled_max_value)
        {
            context.getDAGContext()->handleOverflowError("cast to decimal", Errors::Types::Truncated);
            if (int_value > 0)
                return static_cast<UType>(scaled_max_value);
            else
                return static_cast<UType>(-scaled_max_value);
        }

        return static_cast<UType>(scaled_value);
    }

    template <typename U>
    static U toTiDBDecimal(MyDateTime & date_time, PrecType prec, ScaleType scale, int fsp, const Context & context)
    {
        UInt64 value_without_fsp = date_time.year * 10000000000ULL + date_time.month * 100000000ULL + date_time.day * 1000000ULL
            + date_time.hour * 10000ULL + date_time.minute * 100 + date_time.second;
        if (fsp > 0)
        {
            Int128 value = static_cast<Int128>(value_without_fsp) * 1000000 + date_time.micro_second;
            Decimal128 decimal(value);
            return toTiDBDecimal<Decimal128, U>(decimal, 6, prec, scale, context);
        }
        else
        {
            return toTiDBDecimalInternal<UInt64, U>(value_without_fsp, prec, scale, context);
        }
    }

    template <typename U>
    static U toTiDBDecimal(MyDate & date, PrecType prec, ScaleType scale, const Context & context)
    {
        UInt64 value = date.year * 10000 + date.month * 100 + date.day;
        return toTiDBDecimalInternal<UInt64, U>(value, prec, scale, context);
    }

    template <typename T, typename U>
    static std::enable_if_t<std::is_integral_v<T>, U> toTiDBDecimal(T value, PrecType prec, ScaleType scale, const Context & context)
    {
        if constexpr (std::is_signed_v<T>)
            return toTiDBDecimalInternal<T, U>(value, prec, scale, context);
        else
            return toTiDBDecimalInternal<UInt64, U>(static_cast<UInt64>(value), prec, scale, context);
    }

    template <typename T, typename U>
    static std::enable_if_t<std::is_floating_point_v<T>, U> toTiDBDecimal(T value, PrecType prec, ScaleType scale, const Context & context)
    {
        using UType = typename U::NativeType;
        bool neg = false;
        if (value < 0)
        {
            neg = true;
            value = -value;
        }
        for (ScaleType i = 0; i < scale; i++)
        {
            value *= 10;
        }
        auto max_value = DecimalMaxValue::Get(prec);
        if (value > static_cast<Float64>(max_value))
        {
            context.getDAGContext()->handleOverflowError("cast real to decimal", Errors::Types::Truncated);
            if (!neg)
                return static_cast<UType>(max_value);
            else
                return static_cast<UType>(-max_value);
        }
        // rounding
        T tenTimesValue = value * 10;
        UType v(value);
        Int32 remain = static_cast<Int32>(Int256(tenTimesValue) % 10);
        if (remain != 0)
            context.getDAGContext()->handleTruncateError("cast real as decimal");
        if (remain % 10 >= 5)
        {
            v++;
        }
        if (neg)
        {
            v = -v;
        }
        return v;
    }

    template <typename T, typename U>
    static std::enable_if_t<IsDecimal<T>, U> toTiDBDecimal(
        const T & v, ScaleType v_scale, PrecType prec, ScaleType scale, const Context & context)
    {
        using UType = typename U::NativeType;
        auto value = Int256(v.value);

        if (v_scale <= scale)
        {
            for (ScaleType i = v_scale; i < scale; i++)
                value *= 10;
        }
        else
        {
            context.getDAGContext()->handleTruncateError("cast decimal as decimal");
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

        auto max_value = DecimalMaxValue::Get(prec);
        if (value > max_value || value < -max_value)
        {
            context.getDAGContext()->handleOverflowError("cast decimal as decimal", Errors::Types::Truncated);
            if (value > 0)
                return static_cast<UType>(max_value);
            else
                return static_cast<UType>(-max_value);
        }
        return static_cast<UType>(value);
    }

    struct DecimalParts
    {
        StringRef int_part;
        StringRef frac_part;
        StringRef exp_part;
    };

    static DecimalParts splitDecimalString(const StringRef & value)
    {
        DecimalParts ret;
        ret.int_part.size = ret.frac_part.size = ret.exp_part.size = 0;
        size_t start = 0;
        size_t end = 0;
        if (value.data[end] == '+' || value.data[end] == '-')
            end++;
        for (; end < value.size; end++)
        {
            if (value.data[end] > '9' || value.data[end] < '0')
                break;
        }
        ret.int_part.data = &value.data[start];
        ret.int_part.size = end - start;
        if (end < value.size && value.data[end] == '.')
        {
            /// frac part
            start = end + 1;
            end = start;
            for (; end < value.size; end++)
            {
                if (value.data[end] > '9' || value.data[end] < '0')
                    break;
            }
            ret.frac_part.data = &value.data[start];
            ret.frac_part.size = end - start;
        }
        if (end < value.size && (value.data[end] == 'e' || value.data[end] == 'E'))
        {
            /// exponent part
            start = end + 1;
            end = start;
            if (value.data[end] == '+' || value.data[end] == '-')
                end++;
            for (; end < value.size; end++)
            {
                if (value.data[end] > '9' || value.data[end] < '0')
                    break;
            }
            ret.exp_part.data = &value.data[start];
            ret.exp_part.size = end - start;
        }
        return ret;
    }

    template <typename U>
    static U strToTiDBDecimal(const StringRef & value, PrecType prec, ScaleType scale, const Context & context)
    {
        using UType = typename U::NativeType;
        const StringRef trim_string = trim(value);
        if (trim_string.size == 0)
            return static_cast<UType>(0);
        DecimalParts decimal_parts = splitDecimalString(value);
        Int64 frac_offset_by_exponent = 0;
        CastError err = NONE;
        if (decimal_parts.exp_part.size != 0)
        {
            std::tie(frac_offset_by_exponent, err)
                = TiDBConvertToInteger<DataTypeUInt8, DataTypeInt64, false>::toInt<Int64>(decimal_parts.exp_part);
            /// follow TiDB's code
            if (err == OVERFLOW_ERR || frac_offset_by_exponent > std::numeric_limits<Int32>::max() / 2
                || frac_offset_by_exponent < std::numeric_limits<Int32>::min() / 2)
            {
                context.getDAGContext()->handleOverflowError("cast string as decimal", Errors::Types::Truncated);
                if (decimal_parts.exp_part.data[0] == '-')
                    return static_cast<UType>(0);
                else
                    return static_cast<UType>(DecimalMaxValue::Get(prec));
            }
        }
        Int256 v = 0;
        bool is_negative = false;
        size_t pos = 0;
        if (decimal_parts.int_part.data[pos] == '+' || decimal_parts.int_part.data[pos] == '-')
        {
            if (decimal_parts.int_part.data[pos] == '-')
                is_negative = true;
            pos++;
        }
        Int256 max_value = DecimalMaxValue::Get(prec);

        Int64 current_scale = frac_offset_by_exponent >= 0
            ? -(decimal_parts.int_part.size - pos + frac_offset_by_exponent)
            : -frac_offset_by_exponent - (decimal_parts.int_part.size - pos + decimal_parts.frac_part.size);

        /// handle original int part
        for (; pos < decimal_parts.int_part.size; pos++)
        {
            if (current_scale == scale)
                break;
            v = v * 10 + decimal_parts.int_part.data[pos] - '0';
            if (v > max_value)
            {
                context.getDAGContext()->handleOverflowError("cast string as decimal", Errors::Types::Truncated);
                return static_cast<UType>(is_negative ? -max_value : max_value);
            }
            current_scale++;
        }

        if (current_scale == scale)
        {
            if (pos < decimal_parts.int_part.size || decimal_parts.frac_part.size > 0)
                context.getDAGContext()->handleTruncateError("cast string as decimal");
            /// do not need to handle original frac part, just do rounding
            if (pos < decimal_parts.int_part.size)
            {
                if (decimal_parts.int_part.data[pos] >= '5')
                    v++;
            }
            else if (decimal_parts.frac_part.size > 0 && decimal_parts.frac_part.data[0] >= '5')
            {
                v++;
            }
        }
        else
        {
            /// handle original frac part
            pos = 0;
            for (; pos < decimal_parts.frac_part.size; pos++)
            {
                if (current_scale == scale)
                    break;
                v = v * 10 + decimal_parts.frac_part.data[pos] - '0';
                if (v > max_value)
                {
                    context.getDAGContext()->handleOverflowError("cast string as decimal", Errors::Types::Truncated);
                    return static_cast<UType>(is_negative ? -max_value : max_value);
                }
                current_scale++;
            }
            if (current_scale == scale)
            {
                if (pos < decimal_parts.frac_part.size)
                    context.getDAGContext()->handleTruncateError("cast string as decimal");
                if (pos < decimal_parts.frac_part.size && decimal_parts.frac_part.data[pos] >= '5')
                    v++;
            }
            else
            {
                while (current_scale < scale)
                {
                    v *= 10;
                    if (v > max_value)
                    {
                        context.getDAGContext()->handleOverflowError("cast string as decimal", Errors::Types::Truncated);
                        return static_cast<UType>(is_negative ? -max_value : max_value);
                    }
                    current_scale++;
                }
            }
        }

        if (v > max_value)
        {
            context.getDAGContext()->handleOverflowError("cast string as decimal", Errors::Types::Truncated);
            return static_cast<UType>(is_negative ? -max_value : max_value);
        }
        return static_cast<UType>(is_negative ? -v : v);
    }

    /// cast int/real/time/decimal as decimal
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, PrecType prec [[maybe_unused]], ScaleType scale,
        bool, const tipb::FieldType &, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        auto col_to = ColumnDecimal<ToFieldType>::create(size, static_cast<ToFieldType>(0), scale);
        typename ColumnDecimal<ToFieldType>::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size, 0);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (IsDecimal<FromFieldType>)
        {
            /// cast decimal as decimal
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnDecimal<FromFieldType>::Container & vec_from = col_from->getData();

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = toTiDBDecimal<FromFieldType, ToFieldType>(vec_from[i], vec_from.getScale(), prec, scale, context);
        }
        else if constexpr (std::is_same_v<DataTypeMyDateTime, FromDataType> || std::is_same_v<DataTypeMyDate, FromDataType>)
        {
            /// cast time as decimal
            const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
            const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get());
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();

            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (std::is_same_v<DataTypeMyDate, FromDataType>)
                {
                    MyDate date(vec_from[i]);
                    vec_to[i] = toTiDBDecimal<ToFieldType>(date, prec, scale, context);
                }
                else
                {
                    MyDateTime date_time(vec_from[i]);
                    vec_to[i] = toTiDBDecimal<ToFieldType>(date_time, prec, scale, type.getFraction(), context);
                }
            }
        }
        else if constexpr (std::is_same_v<DataTypeString, FromDataType>)
        {
            /// cast string as decimal
            const IColumn * col_from = block.getByPosition(arguments[0]).column.get();
            const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
            const ColumnString::Chars_t * chars = &col_from_string->getChars();
            const IColumn::Offsets * offsets = &col_from_string->getOffsets();
            size_t current_offset = 0;
            for (size_t i = 0; i < size; i++)
            {
                size_t next_offset = (*offsets)[i];
                size_t string_size = next_offset - current_offset - 1;
                StringRef string_value(&(*chars)[current_offset], string_size);
                vec_to[i] = strToTiDBDecimal<ToFieldType>(string_value, prec, scale, context);
                current_offset = next_offset;
            }
        }
        else if (const ColumnVector<FromFieldType> * col_from
            = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            /// cast int/real as decimal
            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();

            if constexpr (std::is_integral_v<FromFieldType>)
            {
                /// cast enum/int as decimal
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = toTiDBDecimal<FromFieldType, ToFieldType>(vec_from[i], prec, scale, context);
            }
            else
            {
                /// cast real as decimal
                static_assert(std::is_floating_point_v<FromFieldType>);
                for (size_t i = 0; i < size; ++i)
                    // Always use Float64 to avoid overflow for vec_from[i] * 10^scale.
                    vec_to[i] = toTiDBDecimal<Float64, ToFieldType>(static_cast<Float64>(vec_from[i]), prec, scale, context);
            }
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
        }
        if constexpr (return_nullable)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};

/// cast int/real/decimal/time/string as Date/DateTime
template <typename FromDataType, typename ToDataType, bool return_nullable>
struct TiDBConvertToTime
{
    static_assert(std::is_same_v<ToDataType, DataTypeMyDate> || std::is_same_v<ToDataType, DataTypeMyDateTime>);

    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    static void execute(
        Block & block, const ColumnNumbers & arguments, size_t result, bool, const tipb::FieldType &, const Context & context)
    {
        size_t size = block.getByPosition(arguments[0]).column->size();
        auto col_to = ColumnUInt64::create(size, 0);
        ColumnUInt64::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;

        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        int to_fsp [[maybe_unused]] = 0;
        if constexpr (std::is_same_v<ToDataType, DataTypeMyDateTime>)
        {
            const auto * tp = dynamic_cast<const DataTypeMyDateTime *>(removeNullable(block.getByPosition(result).type).get());
            to_fsp = tp->getFraction();
        }

        if constexpr (return_nullable)
        {
            col_null_map_to = ColumnUInt8::create(size, 0);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            // cast string as time
            const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
            const ColumnString * col_from = checkAndGetColumn<ColumnString>(col_with_type_and_name.column.get());
            const ColumnString::Chars_t * chars = &col_from->getChars();
            const ColumnString::Offsets * offsets = &col_from->getOffsets();

            size_t current_offset = 0;
            for (size_t i = 0; i < size; i++)
            {
                size_t next_offset = (*offsets)[i];
                size_t string_size = next_offset - current_offset - 1;
                StringRef string_ref(&(*chars)[current_offset], string_size);
                String string_value = string_ref.toString();
                try
                {
                    Field packed_uint_value = parseMyDateTime(string_value, to_fsp);
                    UInt64 packed_uint = packed_uint_value.template safeGet<UInt64>();
                    MyDateTime datetime(packed_uint);
                    if constexpr (std::is_same_v<ToDataType, DataTypeMyDate>)
                    {
                        MyDate date(datetime.year, datetime.month, datetime.day);
                        vec_to[i] = date.toPackedUInt();
                    }
                    else
                    {
                        vec_to[i] = packed_uint;
                    }
                }
                catch (const Exception &)
                {
                    // Fill NULL if cannot parse
                    (*vec_null_map_to)[i] = 1;
                    context.getDAGContext()->handleInvalidTime("Invalid time value: '" + string_value + "'", Errors::Types::WrongValue);
                }
                current_offset = next_offset;
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeMyDate> || std::is_same_v<FromDataType, DataTypeMyDateTime>)
        {
            // cast time as time
            const auto * col_from = checkAndGetColumn<ColumnUInt64>(block.getByPosition(arguments[0]).column.get());
            const ColumnUInt64::Container & vec_from = col_from->getData();

            for (size_t i = 0; i < size; i++)
            {
                MyDateTime datetime(vec_from[i]);

                if constexpr (std::is_same_v<ToDataType, DataTypeMyDate>)
                {
                    MyDate date(datetime.year, datetime.month, datetime.day);
                    vec_to[i] = date.toPackedUInt();
                }
                else
                {
                    int from_fsp = 0;
                    if constexpr (std::is_same_v<FromDataType, DataTypeMyDateTime>)
                    {
                        auto & from_type = static_cast<const DataTypeMyDateTime &>(type);
                        from_fsp = from_type.getFraction();
                    }
                    UInt32 micro_second = datetime.micro_second;
                    UInt64 packed_uint = vec_from[i];
                    if (to_fsp < from_fsp)
                    {
                        micro_second = micro_second / std::pow(10, 6 - to_fsp - 1);
                        micro_second = (micro_second + 5) / 10;
                        // Overflow
                        if (micro_second >= std::pow(10, to_fsp))
                        {
                            static const auto lut = DateLUT::instance("UTC");
                            datetime.micro_second = 0;
                            packed_uint = datetime.toPackedUInt();
                            packed_uint = AddSecondsImpl::execute(packed_uint, 1, lut);
                        }
                        else
                        {
                            datetime.micro_second = micro_second * std::pow(10, 6 - to_fsp);
                            packed_uint = datetime.toPackedUInt();
                        }
                    }
                    vec_to[i] = packed_uint;
                }
            }
        }
        else if constexpr (std::is_integral_v<FromFieldType>)
        {
            // cast int as time
            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());

            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();

            for (size_t i = 0; i < size; i++)
            {
                try
                {
                    MyDateTime datetime(0, 0, 0, 0, 0, 0, 0);
                    bool is_null = numberToDateTime(vec_from[i], datetime, context.getDAGContext());
                    if constexpr (std::is_same_v<ToDataType, DataTypeMyDate>)
                    {
                        MyDate date(datetime.year, datetime.month, datetime.day);
                        vec_to[i] = date.toPackedUInt();
                    }
                    else
                    {
                        vec_to[i] = datetime.toPackedUInt();
                    }
                    (*vec_null_map_to)[i] = is_null;
                }
                catch (const TiFlashException & e)
                {
                    // Cannot cast, fill with NULL
                    (*vec_null_map_to)[i] = 1;
                    context.getDAGContext()->handleInvalidTime(
                        "Invalid time value: '" + toString(vec_from[i]) + "'", Errors::Types::WrongValue);
                }
            }
        }
        else if constexpr (std::is_floating_point_v<FromFieldType>)
        {
            // cast float as time
            // MySQL compatibility: 0 should not be converted to null, see TiDB#11203
            assert(return_nullable);
            const ColumnVector<FromFieldType> * col_from
                = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());

            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();

            for (size_t i = 0; i < size; i++)
            {
                Float64 value = vec_from[i];
                // Convert to string and then parse to time
                String value_str = toString(value);

                if (value_str == "0")
                {
                    (*vec_null_map_to)[i] = 1;
                }
                else
                {
                    try
                    {
                        Field packed_uint_value = parseMyDateTime(value_str, to_fsp);
                        UInt64 packed_uint = packed_uint_value.template safeGet<UInt64>();
                        MyDateTime datetime(packed_uint);
                        if constexpr (std::is_same_v<ToDataType, DataTypeMyDate>)
                        {
                            MyDate date(datetime.year, datetime.month, datetime.day);
                            vec_to[i] = date.toPackedUInt();
                        }
                        else
                        {
                            vec_to[i] = packed_uint;
                        }
                    }
                    catch (const Exception &)
                    {
                        // Fill NULL if cannot parse
                        (*vec_null_map_to)[i] = 1;
                        context.getDAGContext()->handleInvalidTime("Invalid time value: '" + value_str + "'", Errors::Types::WrongValue);
                    }
                }
            }
        }
        else if constexpr (IsDecimal<FromFieldType>)
        {
            const auto * col_from = checkAndGetColumn<ColumnDecimal<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
            const typename ColumnDecimal<FromFieldType>::Container & vec_from = col_from->getData();


            for (size_t i = 0; i < size; i++)
            {
                String value_str = vec_from[i].toString(type.getScale());
                try
                {
                    Field value = parseMyDateTime(value_str, to_fsp);
                    MyDateTime datetime(value.template safeGet<UInt64>());
                    if constexpr (std::is_same_v<ToDataType, DataTypeMyDate>)
                    {
                        MyDate date(datetime.year, datetime.month, datetime.day);
                        vec_to[i] = date.toPackedUInt();
                    }
                    else
                    {
                        vec_to[i] = datetime.toPackedUInt();
                    }
                }
                catch (const Exception &)
                {
                    (*vec_null_map_to)[i] = 1;
                    context.getDAGContext()->handleInvalidTime("Invalid time value: '" + value_str + "'", Errors::Types::WrongValue);
                }
            }
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function tidb_cast",
                ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr (return_nullable)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};

inline bool getDatetime(const Int64 & num, MyDateTime & result, DAGContext * ctx)
{
    UInt64 ymd = num / 1000000;
    UInt64 hms = num - ymd * 1000000;

    UInt64 year = ymd / 10000;
    ymd %= 10000;
    UInt64 month = ymd / 100;
    UInt64 day = ymd % 100;

    UInt64 hour = hms / 10000;
    hms %= 10000;
    UInt64 minute = hms / 100;
    UInt64 second = hms % 100;

    if (toCoreTimeChecked(year, month, day, hour, minute, second, 0, result))
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }
    if (ctx)
    {
        result.check(ctx->allowZeroInDate(), ctx->allowInvalidDate());
    }
    else
    {
        result.check(false, false);
    }
    return false;
}

// Convert a integer number to DateTime and return true if the result is NULL.
// If number is invalid(according to SQL_MODE), return NULL and handle the error with DAGContext.
// This function may throw exception.
inline bool numberToDateTime(Int64 number, MyDateTime & result, DAGContext * ctx)
{
    MyDateTime datetime(0);
    if (number == 0)
    {
        result = datetime;
        return true;
    }

    // datetime type
    if (number >= 10000101000000)
    {
        return getDatetime(number, result, ctx);
    }

    // check MMDD
    if (number < 101)
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }

    // check YYMMDD: 2000-2069
    if (number <= 69 * 10000 + 1231)
    {
        number = (number + 20000000) * 1000000;
        return getDatetime(number, result, ctx);
    }

    if (number < 70 * 10000 + 101)
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }

    // check YYMMDD
    if (number <= 991231)
    {
        number = (number + 19000000) * 1000000;
        return getDatetime(number, result, ctx);
    }

    // check hour/min/second
    if (number <= 99991231)
    {
        number *= 1000000;
        return getDatetime(number, result, ctx);
    }

    // check MMDDHHMMSS
    if (number < 101000000)
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }

    // check YYMMDDhhmmss: 2000-2069
    if (number <= 69 * 10000000000 + 1231235959)
    {
        number += 20000000000000;
        return getDatetime(number, result, ctx);
    }

    // check YYYYMMDDhhmmss
    if (number < 70 * 10000000000 + 101000000)
    {
        throw TiFlashException("Incorrect time value", Errors::Types::WrongValue);
    }

    // check YYMMDDHHMMSS
    if (number <= 991231235959)
    {
        number += 19000000000000;
        return getDatetime(number, result, ctx);
    }

    return getDatetime(number, result, ctx);
}

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
    bool useDefaultImplementationForNulls() const override { return false; }
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
    WrapperType createWrapper(const DataTypePtr & to_type) const
    {
        /// cast as int
        if (checkDataType<DataTypeUInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertToInteger<FromDataType, DataTypeUInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeInt64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertToInteger<FromDataType, DataTypeInt64, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        /// cast as decimal
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
        /// cast as real
        if (checkDataType<DataTypeFloat64>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                if (hasUnsignedFlag(tidb_tp_))
                {
                    TiDBConvertToFloat<FromDataType, DataTypeFloat64, return_nullable, true>::execute(
                        block, arguments, result, in_union_, tidb_tp_, context_);
                }
                else
                {
                    TiDBConvertToFloat<FromDataType, DataTypeFloat64, return_nullable, false>::execute(
                        block, arguments, result, in_union_, tidb_tp_, context_);
                }
            };
        /// cast as string
        if (checkDataType<DataTypeString>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertToString<FromDataType, return_nullable>::execute(block, arguments, result, in_union_, tidb_tp_, context_);
            };
        /// cast as time
        if (checkDataType<DataTypeMyDate>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertToTime<FromDataType, DataTypeMyDate, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };
        if (checkDataType<DataTypeMyDateTime>(to_type.get()))
            return [](Block & block, const ColumnNumbers & arguments, const size_t result, bool in_union_, const tipb::FieldType & tidb_tp_,
                       const Context & context_) {
                TiDBConvertToTime<FromDataType, DataTypeMyDateTime, return_nullable>::execute(
                    block, arguments, result, in_union_, tidb_tp_, context_);
            };

        // todo support convert to duration/json type
        throw Exception{"tidb_cast to " + to_type->getName() + " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
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
        if (isIdentityCast(from_type, to_type))
            return createIdentityWrapper(from_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt8>(from_type.get()))
            return createWrapper<DataTypeUInt8, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt16>(from_type.get()))
            return createWrapper<DataTypeUInt16, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt32>(from_type.get()))
            return createWrapper<DataTypeUInt32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeUInt64>(from_type.get()))
            return createWrapper<DataTypeUInt64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt8>(from_type.get()))
            return createWrapper<DataTypeInt8, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt16>(from_type.get()))
            return createWrapper<DataTypeInt16, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt32>(from_type.get()))
            return createWrapper<DataTypeInt32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeInt64>(from_type.get()))
            return createWrapper<DataTypeInt64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeFloat32>(from_type.get()))
            return createWrapper<DataTypeFloat32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeFloat64>(from_type.get()))
            return createWrapper<DataTypeFloat64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal32>(from_type.get()))
            return createWrapper<DataTypeDecimal32, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal64>(from_type.get()))
            return createWrapper<DataTypeDecimal64, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal128>(from_type.get()))
            return createWrapper<DataTypeDecimal128, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeDecimal256>(from_type.get()))
            return createWrapper<DataTypeDecimal256, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeMyDate>(from_type.get()))
            return createWrapper<DataTypeMyDate, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeMyDateTime>(from_type.get()))
            return createWrapper<DataTypeMyDateTime, return_nullable>(to_type);
        if (const auto from_actual_type = checkAndGetDataType<DataTypeString>(from_type.get()))
            return createWrapper<DataTypeString, return_nullable>(to_type);

        // todo support convert to duration/json type
        throw Exception{
            "tidb_cast from " + from_type->getName() + " to " + to_type->getName() + " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
    }

    bool isIdentityCast(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        // todo should remove !from_type->isParametric(), because when a type equals to
        //  other type, its parameter should be the same
        DataTypePtr from_inner_type = removeNullable(from_type);
        DataTypePtr to_inner_type = removeNullable(to_type);
        return from_type->isNullable() == to_type->isNullable() && from_inner_type->equals(*to_inner_type) && !from_inner_type->isParametric() && !from_inner_type->isString();
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

        if (isIdentityCast(from_type, to_type))
            return createIdentityWrapper(from_type);
            
        DataTypePtr from_inner_type = removeNullable(from_type);
        DataTypePtr to_inner_type = removeNullable(to_type);

        auto wrapper = prepareImpl(from_inner_type, to_inner_type, to_type->isNullable());
        if (from_type->isNullable())
        {
            return [wrapper, to_type](Block & block, const ColumnNumbers & arguments, size_t result, bool in_union_,
                       const tipb::FieldType & tidb_tp_, const Context & context_) {
                const auto & from_col = block.getByPosition(arguments[0]).column;
                const auto & from_nullable_col = static_cast<const ColumnNullable &>(*from_col);
                const auto & from_null_map = from_nullable_col.getNullMapData();
                /// make sure if to_type is not nullable, then there is no null value in from_column
                if (!to_type->isNullable())
                {
                    if (!memoryIsZero(from_null_map.data(), from_null_map.size()))
                        throw Exception{
                            "Cannot convert NULL value to non-Nullable type", ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
                }
                Block tmp_block = createBlockWithNestedColumns(block, arguments, result);
                wrapper(tmp_block, arguments, result, in_union_, tidb_tp_, context_);
                if (!to_type->isNullable())
                {
                    block.getByPosition(result).column = tmp_block.getByPosition(result).column;
                }
                else
                {
                    ColumnPtr result_null_map_column
                        = static_cast<const ColumnNullable &>(*tmp_block.getByPosition(result).column).getNullMapColumnPtr();
                    ColumnPtr result_not_nullable
                        = static_cast<const ColumnNullable &>(*tmp_block.getByPosition(result).column).getNestedColumnPtr();
                    size_t size = result_null_map_column->size();
                    MutableColumnPtr mutable_result_null_map_column = (*std::move(result_null_map_column)).mutate();
                    NullMap & result_null_map = static_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
                    for (size_t i = 0; i < size; i++)
                    {
                        if (from_null_map[i])
                            result_null_map[i] = 1;
                    }
                    result_null_map_column = std::move(mutable_result_null_map_column);
                    if (result_not_nullable->isColumnConst())
                    {
                        block.getByPosition(result).column
                            = ColumnNullable::create(result_not_nullable->convertToFullColumnIfConst(), result_null_map_column);
                    }
                    else
                    {
                        block.getByPosition(result).column = ColumnNullable::create(result_not_nullable, result_null_map_column);
                    }
                }
            };
        }
        else
        {
            if (isIdentityCast(from_inner_type, to_inner_type) && to_type->isNullable())
            {
                /// convert not_null type to nullable type
                return [wrapper, to_type](Block & block, const ColumnNumbers & arguments, size_t result, bool in_union_,
                           const tipb::FieldType & tidb_tp_, const Context & context_) {
                    auto & res = block.getByPosition(result);
                    const auto & ret_type = res.type;
                    const auto & nullable_type = static_cast<const DataTypeNullable &>(*ret_type);
                    const auto & nested_type = nullable_type.getNestedType();

                    Block tmp_block = block;
                    size_t tmp_res_index = tmp_block.columns();
                    tmp_block.insert({nullptr, nested_type, ""});

                    wrapper(tmp_block, arguments, tmp_res_index, in_union_, tidb_tp_, context_);
                    /// This is a conversion from an ordinary type to a nullable type.
                    /// So we create a trivial null map.
                    ColumnPtr null_map = ColumnUInt8::create(block.rows(), 0);

                    const auto & tmp_res = tmp_block.getByPosition(tmp_res_index);
                    res.column = ColumnNullable::create(tmp_res.column, null_map);
                };
            }
            else
            {
                return wrapper;
            }
        }
    }

    WrapperType prepareImpl(const DataTypePtr & from_type, const DataTypePtr & to_type, bool return_nullable) const
    {
        if (return_nullable)
            return createWrapper<true>(from_type, to_type);
        else
            return createWrapper<false>(from_type, to_type);
    }
};

class FunctionBuilderTiDBCast : public FunctionBuilderImpl
{
public:
    using MonotonicityForRange = FunctionTiDBCast::MonotonicityForRange;

    static constexpr auto name = "tidb_cast";
    static FunctionBuilderPtr create(const Context & context)
    {
        if (!context.getDAGContext())
        {
            throw Exception("DAGContext should not be nullptr.", ErrorCodes::LOGICAL_ERROR);
        }
        return std::make_shared<FunctionBuilderTiDBCast>(context);
    }

    FunctionBuilderTiDBCast(const Context & context) : context(context) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    void setInUnion(bool in_union_) { in_union = in_union_; }
    void setTiDBFieldType(const tipb::FieldType & tidb_tp_) { tidb_tp = tidb_tp_; }
    bool useDefaultImplementationForNulls() const override { return false; }


protected:
    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, std::shared_ptr<TiDB::ITiDBCollator>) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_shared<FunctionTiDBCast>(context, name, std::move(monotonicity), data_types, return_type, in_union, tidb_tp);
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
    tipb::FieldType tidb_tp;
};

} // namespace DB
