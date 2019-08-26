#include <Storages/Transaction/Datum.h>

#include <ext/bit_cast.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{

using DB::Field;

/// Operations to be done on datum.
/// Most generic one that does nothing.
template <TP tp, typename = void>
struct DatumOp
{
    static void unflatten(const Field &, std::optional<Field> &) {}
    static void flatten(const Field &, std::optional<Field> &) {}
    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

/// Specialized for Date/Datetime/Timestamp, actual does unflatten/flatten.
template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeDate || tp == TypeDatetime || tp == TypeTimestamp>::type>
{
    static void unflatten(const Field & orig, std::optional<Field> & copy)
    {
        if (orig.isNull())
            return;

        UInt64 packed = orig.get<UInt64>();
        UInt64 ymdhms = packed >> 24;
        UInt64 ymd = ymdhms >> 17;
        UInt8 day = UInt8(ymd & ((1 << 5) - 1));
        UInt64 ym = ymd >> 5;
        UInt8 month = UInt8(ym % 13);
        UInt16 year = UInt16(ym / 13);

        const auto & date_lut = DateLUT::instance();

        if constexpr (tp == TypeDate)
        {
            auto date = date_lut.makeDayNum(year, month, day);
            copy = static_cast<UInt64>(date);
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
                UInt64 hms = ymdhms & ((1 << 17) - 1);
                UInt8 second = UInt8(hms & ((1 << 6) - 1));
                UInt8 minute = UInt8((hms >> 6) & ((1 << 6) - 1));
                UInt8 hour = UInt8(hms >> 12);

                date_time = date_lut.makeDateTime(year, month, day, hour, minute, second);
            }
            copy = static_cast<Int64>(date_time);
        }
    }

    static void flatten(const Field & orig, std::optional<Field> & copy)
    {
        if (orig.isNull())
            return;

        DateLUTImpl::Values values;
        UInt8 hour = 0, minute = 0, second = 0;
        const auto & date_lut = DateLUT::instance();
        if constexpr (tp == TypeDate)
        {
            DayNum_t day_num(static_cast<UInt32>(orig.get<UInt64>()));
            values = date_lut.getValues(day_num);
        }
        else
        {
            time_t date_time(orig.get<Int64>());
            values = date_lut.getValues(date_time);
            hour = date_lut.toHour(date_time);
            minute = date_lut.toMinute(date_time);
            second = date_lut.toSecond(date_time);
        }
        UInt64 ymd = ((UInt64)values.year * 13 + values.month) << 5 | values.day_of_month;
        UInt64 hms = (UInt64)hour << 12 | minute << 6 | second;
        copy = (ymd << 17 | hms) << 24;
    }

    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

/// Specialized for integer types less than 64 bit, checks overflow.
template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeTiny || tp == TypeShort || tp == TypeLong || tp == TypeInt24>::type>
{
    static void unflatten(const Field &, std::optional<Field> &) {}
    static void flatten(const Field &, std::optional<Field> &) {}

    static bool overflow(const Field & field, const ColumnInfo & column_info)
    {
        if (field.isNull())
            return false;

        if constexpr (tp == TypeTiny)
            return concreteOverflow<Int8>(field, column_info);

        if constexpr (tp == TypeShort)
            return concreteOverflow<Int16>(field, column_info);

        if constexpr (tp == TypeLong || tp == TypeInt24)
            return concreteOverflow<Int32>(field, column_info);
    }

private:
    template <typename T>
    static inline bool concreteOverflow(const Field & field, const ColumnInfo & column_info)
    {
        if (column_info.hasUnsignedFlag())
            // Unsigned checking by bitwise compare.
            return field.get<UInt64>() != ext::bit_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(field.get<UInt64>()));
        else
            // Signed checking by arithmetical cast.
            return field.get<Int64>() != static_cast<Int64>(static_cast<std::make_signed_t<T>>(field.get<Int64>()));
    }
};

/// Specialized for Enum, using unflatten/flatten to transform UInt to Int back and forth.
template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeEnum>::type>
{
    static void unflatten(const Field & orig, std::optional<Field> & copy) { copy = static_cast<Int64>(orig.get<UInt64>()); }

    static void flatten(const Field & orig, std::optional<Field> & copy) { copy = static_cast<UInt64>(orig.get<Int64>()); }

    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

DatumFlat::DatumFlat(const DB::Field & field, TP tp) : DatumBase(field, tp)
{
    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w)                       \
    case Type##tt:                                \
        DatumOp<Type##tt>::unflatten(orig, copy); \
        break;
        COLUMN_TYPES(M)
#undef M
    }
}

bool DatumFlat::overflow(const ColumnInfo & column_info)
{
    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w) \
    case Type##tt:          \
        return DatumOp<Type##tt>::overflow(field(), column_info);
        COLUMN_TYPES(M)
#undef M
    }

    throw DB::Exception("Shouldn't reach here", DB::ErrorCodes::LOGICAL_ERROR);
}

DatumBumpy::DatumBumpy(const DB::Field & field, TP tp) : DatumBase(field, tp)
{
    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w)                     \
    case Type##tt:                              \
        DatumOp<Type##tt>::flatten(orig, copy); \
        break;
        COLUMN_TYPES(M)
#undef M
    }
}

}; // namespace TiDB
