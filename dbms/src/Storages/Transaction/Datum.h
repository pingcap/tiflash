#pragma once

#include <Core/Field.h>
#include <Storages/Transaction/TiDB.h>

namespace TiDB
{

struct IFlatTrait
{
    template <typename Trait>
    friend struct Datum;

    virtual bool overflow(const ColumnInfo &) const { return false; }

protected:
    virtual ~IFlatTrait() = default;

    virtual void unflatten() {}

    virtual operator const DB::Field &() = 0;
};

struct INonFlatTrait
{
    template <typename Trait>
    friend struct Datum;

protected:
    virtual ~INonFlatTrait() = default;

    virtual void flatten() {}

    virtual operator const DB::Field &() = 0;
};

template <typename Trait>
using TraitPtr = std::shared_ptr<Trait>;

template <typename Trait>
struct Datum
{
    Datum(const TraitPtr<Trait> trait_) : trait(trait_), field(*trait)
    {
        if constexpr (std::is_same<Trait, IFlatTrait>::value)
            trait->unflatten();
        else
            trait->flatten();
    }

    const TraitPtr<Trait> trait;

    const DB::Field & field;
};

template <typename Trait>
Datum<Trait> makeDatum(const DB::Field & field, TP tp);

} // namespace TiDB
