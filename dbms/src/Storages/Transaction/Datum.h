#pragma once

#include <Core/Field.h>
#include <Storages/Transaction/TiDB.h>

namespace TiDB
{

/// TiDB will do a 'flatten' operation to the datum before encoding, which packs the memory representation of datum to a more compact form suiting encoding.
/// The bi-directional data flow will be like:
/// 1. TiDB datum - TiDB.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - Flash.unflatten() -> Flash field.
/// 2. Flash field - Flash.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - TiDB.unflatten() -> TiDB datum.

/// Trait of a flat datum, used to unflatten to Flash field and check overflow (used in schema mismatch detection).
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

/// Trait of a non-flat datum, used to flatten to TiDB datum.
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

/// Datum that will use the given trait to do unflatten/flatten, emits the non-flat/flat representation (in field).
/// Also exposes the trait for outer to do other operations such as overflow check.
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

/// Universal entry of datum creation.
template <typename Trait>
Datum<Trait> makeDatum(const DB::Field & field, TP tp);

} // namespace TiDB
