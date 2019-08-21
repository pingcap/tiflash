#include <Storages/Transaction/Datum.h>
#include <ext/bit_cast.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{

using DB::Field;

template <bool flat>
template <typename Dummy>
std::enable_if_t<flat, Dummy> Datum<flat>::unflatten(TiDB::TP tp)
{
    if (isNull())
        return;

    if (tp == TiDB::TypeDate || tp == TiDB::TypeDatetime)
    {
        UInt64 packed = get<UInt64>();
        UInt64 ymdhms = packed >> 24;
        UInt64 ymd = ymdhms >> 17;
        UInt8 day = UInt8(ymd & ((1 << 5) - 1));
        UInt64 ym = ymd >> 5;
        UInt8 month = UInt8(ym % 13);
        UInt16 year = UInt16(ym / 13);

        UInt64 hms = ymdhms & ((1 << 17) - 1);
        UInt8 second = UInt8(hms & ((1 << 6) - 1));
        UInt8 minute = UInt8((hms >> 6) & ((1 << 6) - 1));
        UInt8 hour = UInt8(hms >> 12);

        const auto & date_lut = DateLUT::instance();

        if (tp == TiDB::TypeDate)
        {
            auto date = date_lut.makeDayNum(year, month, day);
            *this = static_cast<Int64>(date);
        }
        else
        {
            time_t date_time;
            // TODO: Temporary hack invalid date time to zero.
            if (unlikely(year == 0))
                date_time = 0;
            else
            {
                if (unlikely(month == 0 || day == 0))
                {
                    throw Exception(
                        "wrong datetime format: " + std::to_string(year) + " " + std::to_string(month) + " " + std::to_string(day) + ".",
                        DB::ErrorCodes::LOGICAL_ERROR);
                }
                date_time = date_lut.makeDateTime(year, month, day, hour, minute, second);
            }
            Field::operator=(static_cast<Int64>(date_time));
        }
    }
}

template <bool flat>
template <typename Dummy>
std::enable_if_t<!flat, Dummy> Datum<flat>::flatten(TiDB::TP tp)
{
    if (isNull())
        return;

    if (tp == TiDB::TypeDate || tp == TiDB::TypeDatetime)
    {
        DateLUTImpl::Values values;
        UInt8 hour = 0, minute = 0, second = 0;
        const auto & date_lut = DateLUT::instance();
        if (tp == TiDB::TypeDate)
        {
            DayNum_t day_num(static_cast<UInt32>(get<UInt64>()));
            values = date_lut.getValues(day_num);
        }
        else
        {
            time_t date_time(static_cast<UInt64>(get<UInt64>()));
            values = date_lut.getValues(date_time);
            hour = date_lut.toHour(date_time);
            minute = date_lut.toMinute(date_time);
            second = date_lut.toSecond(date_time);
        }
        UInt64 ymd = ((UInt64)values.year * 13 + values.month) << 5 | values.day_of_month;
        UInt64 hms = (UInt64)hour << 12 | minute << 6 | second;
        Field::operator=((ymd << 17 | hms) << 24);
    }
}

template <typename T>
bool integerOverflow(const Datum<true> & datum, const ColumnInfo & column_info)
{
    if (column_info.hasUnsignedFlag())
        // Unsigned checking by bitwise compare.
        return datum.get<UInt64>() != ext::bit_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(datum.get<UInt64>()));
    else
        // Singed checking by arithmetical cast.
        return datum.get<Int64>() != static_cast<Int64>(static_cast<std::make_signed_t<T>>(datum.get<Int64>()));
}

template <bool flat>
template <typename Dummy>
std::enable_if_t<flat, Dummy> Datum<flat>::overflow(const ColumnInfo & column_info)
{
    if (isNull())
        return false;

    switch (column_info.tp)
    {
        case TypeTiny:
            return integerOverflow<Int8>(*this, column_info);
        case TypeShort:
            return integerOverflow<Int16>(*this, column_info);
        case TypeLong:
        case TypeInt24:
            return integerOverflow<Int32>(*this, column_info);
        default:
            return false;
    }
}

template void Datum<true>::unflatten(TP);
template void Datum<false>::flatten(TP);
template bool Datum<true>::overflow<bool>(const ColumnInfo &);

} // namespace TiDB
