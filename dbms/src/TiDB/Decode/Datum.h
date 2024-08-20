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

#pragma once

#include <Core/Field.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <TiDB/Schema/TiDB_fwd.h>

#include <optional>

namespace TiDB
{

/// TiDB will do a 'flatten' operation to the datum before encoding, which packs the memory representation of datum to a more compact form suiting encoding.
/// The bi-directional data flow will be like:
/// 1. TiDB datum - TiDB.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - Flash.unflatten() -> Flash field.
/// 2. Flash field - Flash.flatten() -> flat representation - encode() -> raw bytes - decode() -> flat representation - TiDB.unflatten() -> TiDB datum.

class DatumBase
{
protected:
    DatumBase(const DB::Field & field, TP tp_)
        : orig(field)
        , tp(tp_)
    {}

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
