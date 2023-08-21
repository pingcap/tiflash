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

#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB
{
namespace tests
{
tipb::Expr columnsToTiPBExpr(
    const String & func_name,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val);

tipb::Expr columnToTiPBExpr(const ColumnWithTypeAndName & column, size_t index);
} // namespace tests
} // namespace DB
