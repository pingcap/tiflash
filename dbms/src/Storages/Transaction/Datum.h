#pragma once

#include <optional>

#include <Core/Field.h>
#include <Storages/Transaction/TiDB.h>

namespace TiDB
{

/// TiDB will do a 'flatten' operation to the datum before encoding, which packs the memory representation of datum to a more compact form suiting encoding.
/// The bi-directional data flow will be like:
/// 1. TiDB datum - TiDB.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - Flash.unflatten() -> Flash field.
/// 2. Flash field - Flash.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - TiDB.unflatten() -> TiDB datum.

class DatumBase
{
protected:
    DatumBase(const DB::Field & field, TP tp_) : orig(field), tp(tp_) {}

public:
    const DB::Field & field() const { return copy ? *copy : orig; }

protected:
    const DB::Field & orig;
    const TP tp;
    std::optional<DB::Field> copy;
};

/// Flat datum that needs unflatten, emits the bumpy representation (in field).
/// Also exposes the trait for outer to do other operations such as overflow check.
class DatumFlat : public DatumBase
{
public:
    DatumFlat(const DB::Field & field, TP tp);

    /// Checks if it's null value with a not null type for schema mismatch detection.
    bool invalidNull(const ColumnInfo & column_info);

    /// Checks overflow for schema mismatch detection.
    bool overflow(const ColumnInfo & column_info);
};

/// Bumpy datum that needs flatten, emits the flat representation (in field).
class DatumBumpy : public DatumBase
{
public:
    DatumBumpy(const DB::Field & field, TP tp);
};

} // namespace TiDB
