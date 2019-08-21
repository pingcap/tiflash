#pragma once

#include <Core/Field.h>
#include <Storages/Transaction/TiDB.h>

namespace TiDB
{

template <bool flat>
class Datum : public DB::Field
{
public:
    template <typename Dummy = void, typename = typename std::enable_if_t<flat, Dummy>>
    Datum()
    {}

    template <typename T, typename = typename std::enable_if<flat && !std::is_same<T, DB::Field>::value>::type>
    Datum(T && rhs) : DB::Field(std::forward<T>(rhs))
    {}

    template <typename Dummy = void, typename = typename std::enable_if_t<!flat, Dummy>>
    Datum(const DB::Field & d) : DB::Field(d)
    {}

    template <typename Dummy = void, typename = typename std::enable_if_t<!flat, Dummy>>
    Datum(DB::Field && d) : DB::Field(std::move(d))
    {}

    template <typename Dummy = void>
    std::enable_if_t<flat, Dummy> unflatten(TP tp);

    template <typename Dummy = void>
    std::enable_if_t<!flat, Dummy> flatten(TP tp);

    /// Check overflow for potential un-synced data type widen,
    /// i.e. schema is old and narrow, meanwhile data is new and wide.
    /// So far only integers is possible of overflow.
    template <typename Dummy = bool>
    std::enable_if_t<flat, Dummy> overflow(const ColumnInfo & column_info);
};

} // namespace TiDB
