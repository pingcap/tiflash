#include <Common/MyTime.h>
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
    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeDate || tp == TypeTime || tp == TypeDatetime || tp == TypeTimestamp>::type>
{
    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

/// Specialized for integer types less than 64b, checks overflow.
template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeTiny || tp == TypeShort || tp == TypeLong || tp == TypeInt24>::type>
{
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

DatumFlat::DatumFlat(const DB::Field & field, TP tp) : DatumBase(field, tp) {}

bool DatumFlat::invalidNull(const ColumnInfo & column_info) { return column_info.hasNotNullFlag() && orig.isNull(); }

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

}; // namespace TiDB
