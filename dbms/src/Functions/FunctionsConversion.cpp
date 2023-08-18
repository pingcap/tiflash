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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>

namespace DB
{
void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, Block & block, size_t result)
{
    const IDataType & to_type = *block.getByPosition(result).type;

    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
                << " as " << to_type.getName() << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset() << " (parsed just " << quote
                    << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    if (to_type.isNumber())
        message_buf << ". Note: there are to" << to_type.getName()
                    << "OrZero function, which returns zero instead of throwing exception.";

    throw Exception(message_buf.str(), ErrorCodes::CANNOT_PARSE_TEXT);
}


struct NameTiDBUnixTimeStampInt
{
    static constexpr auto name = "tidbUnixTimeStampInt";
};
struct NameTiDBUnixTimeStampDec
{
    static constexpr auto name = "tidbUnixTimeStampDec";
};

template <typename Name>
class FunctionTiDBUnixTimeStamp : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTiDBUnixTimeStamp>(context); };
    explicit FunctionTiDBUnixTimeStamp(const Context & context)
        : timezone_info(context.getTimezoneInfo()){};

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!arguments[0].type->isMyDateOrMyDateTime())
            throw Exception(
                "The argument of function " + getName() + " must be date or datetime type",
                ErrorCodes::ILLEGAL_COLUMN);

        if constexpr (std::is_same_v<Name, NameTiDBUnixTimeStampInt>)
            return std::make_shared<DataTypeUInt64>();

        int fsp = 0;
        if (checkDataType<DataTypeMyDateTime>(arguments[0].type.get()))
        {
            const auto & datetime_type = dynamic_cast<const DataTypeMyDateTime &>(*arguments[0].type);
            fsp = datetime_type.getFraction();
        }
        return std::make_shared<DataTypeDecimal64>(12 + fsp, fsp);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);

        const auto * col_from = checkAndGetColumn<ColumnUInt64>(col_with_type_and_name.column.get());
        const ColumnUInt64::Container & vec_from = col_from->getData();
        size_t size = vec_from.size();

        if constexpr (std::is_same_v<Name, NameTiDBUnixTimeStampInt>)
        {
            auto col_to = ColumnUInt64::create();
            auto & vec_to = col_to->getData();
            vec_to.resize(size);

            for (size_t i = 0; i < size; i++)
            {
                UInt64 ret = 0;
                if (getUnixTimeStampHelper(vec_from[i], ret))
                    vec_to[i] = ret;
                else
                    vec_to[i] = 0;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else /* if constexpr (std::is_same_v<Name, NameTiDBUnixTimeStampDec>) */
        {
            // Todo: speed up by `prepare`.
            int fsp = 0, multiplier = 1, divider = 1'000'000;
            if (checkDataType<DataTypeMyDateTime>(col_with_type_and_name.type.get()))
            {
                const auto & datetime_type = dynamic_cast<const DataTypeMyDateTime &>(*col_with_type_and_name.type);
                fsp = datetime_type.getFraction();
            }
            multiplier = getScaleMultiplier<Decimal64>(fsp);
            divider = 1'000'000 / multiplier;

            auto col_to = ColumnDecimal<Decimal64>::create(0, fsp);
            auto & vec_to = col_to->getData();
            vec_to.resize(size);

            for (size_t i = 0; i < size; i++)
            {
                UInt64 ret = 0;
                if (getUnixTimeStampHelper(vec_from[i], ret))
                {
                    MyDateTime datetime(vec_from[i]);
                    vec_to[i] = ret * multiplier + datetime.micro_second / divider;
                }
                else
                    vec_to[i] = 0;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
    }

private:
    const TimezoneInfo & timezone_info;

    bool getUnixTimeStampHelper(UInt64 packed, UInt64 & ret) const
    {
        try
        {
            time_t epoch_second = getEpochSecond(MyDateTime(packed), *timezone_info.timezone);
            if (!timezone_info.is_name_based)
                epoch_second -= timezone_info.timezone_offset;
            if (epoch_second <= 0)
            {
                ret = 0;
                return false;
            }
            else if unlikely (epoch_second > std::numeric_limits<Int32>::max())
            {
                ret = 0;
                return false;
            }
            else
            {
                ret = epoch_second;
                return true;
            }
        }
        catch (...)
        {
            return false;
        }
    }
};

void registerFunctionsConversion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToUInt8>();
    factory.registerFunction<FunctionToUInt16>();
    factory.registerFunction<FunctionToUInt32>();
    factory.registerFunction<FunctionToUInt64>();
    factory.registerFunction<FunctionToInt8>();
    factory.registerFunction<FunctionToInt16>();
    factory.registerFunction<FunctionToInt32>();
    factory.registerFunction<FunctionToInt64>();
    factory.registerFunction<FunctionToFloat32>();
    factory.registerFunction<FunctionToFloat64>();

    factory.registerFunction<FunctionToMyDate>();
    factory.registerFunction<FunctionToDateTime>();
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToString>();
    factory.registerFunction<FunctionToFixedString>();

    factory.registerFunction<FunctionToUnixTimestamp>();
    factory.registerFunction<FunctionBuilderCast>();

    factory.registerFunction<FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionToInt8OrZero>();
    factory.registerFunction<FunctionToInt16OrZero>();
    factory.registerFunction<FunctionToInt32OrZero>();
    factory.registerFunction<FunctionToInt64OrZero>();
    factory.registerFunction<FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionToDateOrZero>();
    factory.registerFunction<FunctionToDateTimeOrZero>();

    factory.registerFunction<FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionToInt8OrNull>();
    factory.registerFunction<FunctionToInt16OrNull>();
    factory.registerFunction<FunctionToInt32OrNull>();
    factory.registerFunction<FunctionToInt64OrNull>();
    factory.registerFunction<FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionToDateOrNull>();
    factory.registerFunction<FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionToMyDateOrNull>();
    factory.registerFunction<FunctionToMyDateTimeOrNull>();

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();

    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalSecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMinute, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalHour, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalDay, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalWeek, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMonth, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalYear, PositiveMonotonicity>>();

    factory.registerFunction<FunctionFromUnixTime>();
    factory.registerFunction<FunctionDateFormat>();
    factory.registerFunction<FunctionGetFormat>();
    factory.registerFunction<FunctionTiDBUnixTimeStamp<NameTiDBUnixTimeStampInt>>();
    factory.registerFunction<FunctionTiDBUnixTimeStamp<NameTiDBUnixTimeStampDec>>();
    factory.registerFunction<FunctionStrToDate<NameStrToDateDate>>();
    factory.registerFunction<FunctionStrToDate<NameStrToDateDatetime>>();
}

} // namespace DB
