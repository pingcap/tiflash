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

#include <TiDB/Decode/Datum.h>
#include <TiDB/Schema/TiDB.h>

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

/// Specialized for integer types less than 64 bit, checks overflow.
template <TP tp>
struct DatumOp<
    tp,
    typename std::enable_if<tp == TypeTiny || tp == TypeShort || tp == TypeLong || tp == TypeInt24>::type>
{
    static void unflatten(const Field &, std::optional<Field> &) {}
    static void flatten(const Field &, std::optional<Field> &) {}

    static bool overflow(const Field & field, const ColumnInfo & column_info)
    {
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
            return field.get<UInt64>()
                != ext::bit_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(field.get<UInt64>()));
        else
            // Signed checking by arithmetical cast.
            return field.get<Int64>() != static_cast<Int64>(static_cast<std::make_signed_t<T>>(field.get<Int64>()));
    }
};

/// Specialized for Enum, using unflatten/flatten to transform UInt to Int back and forth.
template <TP tp>
struct DatumOp<tp, typename std::enable_if<tp == TypeEnum>::type>
{
    static void unflatten(const Field & orig, std::optional<Field> & copy)
    {
        copy = static_cast<Int64>(orig.get<UInt64>());
    }

    static void flatten(const Field & orig, std::optional<Field> & copy)
    {
        copy = static_cast<UInt64>(orig.get<Int64>());
    }

    static bool overflow(const Field &, const ColumnInfo &) { return false; }
};

DatumFlat::DatumFlat(const DB::Field & field, TP tp)
    : DatumBase(field, tp)
{
    if (orig.isNull())
        return;

    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct)                          \
    case Type##tt:                                \
        DatumOp<Type##tt>::unflatten(orig, copy); \
        break;
        COLUMN_TYPES(M)
#undef M
    }
}

bool DatumFlat::invalidNull(const ColumnInfo & column_info)
{
    return column_info.hasNotNullFlag() && orig.isNull();
}

bool DatumFlat::overflow(const ColumnInfo & column_info)
{
    if (orig.isNull())
        return false;

    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct) \
    case Type##tt:       \
        return DatumOp<Type##tt>::overflow(field(), column_info);
        COLUMN_TYPES(M)
#undef M
    }

    throw DB::Exception("Shouldn't reach here", DB::ErrorCodes::LOGICAL_ERROR);
}

DatumBumpy::DatumBumpy(const DB::Field & field, TP tp)
    : DatumBase(field, tp)
{
    if (orig.isNull())
        return;

    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct)                        \
    case Type##tt:                              \
        DatumOp<Type##tt>::flatten(orig, copy); \
        break;
        COLUMN_TYPES(M)
#undef M
    }
}

}; // namespace TiDB
