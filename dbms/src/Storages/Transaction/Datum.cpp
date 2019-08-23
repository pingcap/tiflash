#include <Storages/Transaction/Datum.h>

#include <ext/bit_cast.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{

using DB::Field;

/// Flat trait for datum that doesn't have flat representation, stores field ref to save a copy, does nothing when unflatten.
template <TP tp, typename = void>
struct FlatTrait : public IFlatTrait
{
    FlatTrait(const Field & field_) : field(field_) {}

protected:
    operator const Field &() override { return field; }

private:
    const Field & field;
};

/// Non-flat trait for datum that doesn't have flat representation, stores field ref to save a copy, does nothing when flatten.
template <TP tp, typename = void>
struct NonFlatTrait : public INonFlatTrait
{
    NonFlatTrait(const Field & field_) : field(field_) {}

protected:
    operator const Field &() override { return field; }

private:
    const Field & field;
};

/// Specialized flat trait for date/datetime, inevitably stores field copy as unflatten needs to modify the field value.
template <TP tp>
struct FlatTrait<tp, typename std::enable_if<tp == TypeDate || tp == TypeDatetime>::type> : public IFlatTrait
{
    FlatTrait(const Field & field_) : field(field_) {}

protected:
    void unflatten() override
    {
        if (field.isNull())
            return;

        UInt64 packed = field.get<UInt64>();
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
            field = static_cast<Int64>(date);
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
            field = static_cast<Int64>(date_time);
        }
    }

    operator const Field &() override { return field; }

private:
    Field field;
};

/// Specialized non-flat trait for date/datetime, inevitably stores field copy as flatten needs to modify the field value.
template <TP tp>
struct NonFlatTrait<tp, typename std::enable_if<tp == TypeDate || tp == TypeDatetime>::type> : INonFlatTrait
{
    NonFlatTrait(const Field & field_) : field(field_) {}

protected:
    void flatten() override
    {
        if (field.isNull())
            return;

        DateLUTImpl::Values values;
        UInt8 hour = 0, minute = 0, second = 0;
        const auto & date_lut = DateLUT::instance();
        if constexpr (tp == TypeDate)
        {
            DayNum_t day_num(static_cast<UInt32>(field.get<UInt64>()));
            values = date_lut.getValues(day_num);
        }
        else
        {
            time_t date_time(static_cast<UInt64>(field.get<UInt64>()));
            values = date_lut.getValues(date_time);
            hour = date_lut.toHour(date_time);
            minute = date_lut.toMinute(date_time);
            second = date_lut.toSecond(date_time);
        }
        UInt64 ymd = ((UInt64)values.year * 13 + values.month) << 5 | values.day_of_month;
        UInt64 hms = (UInt64)hour << 12 | minute << 6 | second;
        field = (ymd << 17 | hms) << 24;
    }

    operator const DB::Field &() override { return field; }

private:
    Field field;
};

/// Specialized flat trait for integer types less than 64b, check overflow.
template <TP tp>
struct FlatTrait<tp, typename std::enable_if<tp == TypeTiny || tp == TypeShort || tp == TypeLong || tp == TypeInt24>::type>
    : public IFlatTrait
{
    FlatTrait(const Field & field_) : field(field_) {}

    bool overflow(const ColumnInfo & column_info) const override
    {
        if (field.isNull())
            return false;

        if constexpr (tp == TypeTiny)
            return concreteOverflow<Int8>(column_info);

        if constexpr (tp == TypeShort)
            return concreteOverflow<Int16>(column_info);

        if constexpr (tp == TypeLong || tp == TypeInt24)
            return concreteOverflow<Int32>(column_info);
    }

protected:
    operator const DB::Field &() override { return field; }

    template <typename T>
    inline bool concreteOverflow(const ColumnInfo & column_info) const
    {
        if (column_info.hasUnsignedFlag())
            // Unsigned checking by bitwise compare.
            return field.get<UInt64>() != ext::bit_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(field.get<UInt64>()));
        else
            // Singed checking by arithmetical cast.
            return field.get<Int64>() != static_cast<Int64>(static_cast<std::make_signed_t<T>>(field.get<Int64>()));
    }

private:
    const Field & field;
};

/// Extractors for concrete trait types.
template <typename Trait, TP tp, typename = void>
struct ConcreteTrait;
template <typename Trait, TP tp>
struct ConcreteTrait<Trait, tp, typename std::enable_if<std::is_same<Trait, IFlatTrait>::value>::type>
{
    using Type = FlatTrait<tp>;
};
template <typename Trait, TP tp>
struct ConcreteTrait<Trait, tp, typename std::enable_if<std::is_same<Trait, INonFlatTrait>::value>::type>
{
    using Type = NonFlatTrait<tp>;
};

template <typename Trait>
Datum<Trait> makeDatum(const Field & field, TP tp)
{
    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w) \
    case Type##tt:          \
        return Datum<Trait>(std::make_shared<typename ConcreteTrait<Trait, Type##tt>::Type>(field));
        COLUMN_TYPES(M)
#undef M
    }

    throw DB::Exception("Shouldn't reach here", DB::ErrorCodes::LOGICAL_ERROR);
}

template Datum<IFlatTrait> makeDatum<IFlatTrait>(const Field &, TP);
template Datum<INonFlatTrait> makeDatum<INonFlatTrait>(const Field &, TP);

}; // namespace TiDB
