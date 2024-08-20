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

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <TiDB/Schema/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
using ColumnInfo = TiDB::ColumnInfo;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTPtrVec = std::vector<ASTPtr>;

// Because for compatible issues, we need to deal with the duration type separately for computing layer.
// TODO: Need a better implement.
DataTypePtr getDataTypeByColumnInfo(const TiDB::ColumnInfo & column_info);
DataTypePtr getDataTypeByColumnInfoForComputingLayer(const TiDB::ColumnInfo & column_info);

DataTypePtr getDataTypeByFieldType(const tipb::FieldType & field_type);
DataTypePtr getDataTypeByFieldTypeForComputingLayer(const tipb::FieldType & field_type);

DataTypePtr getDataTypeByColumnInfoForDisaggregatedStorageLayer(const TiDB::ColumnInfo & column_info);

TiDB::CodecFlag getCodecFlagByFieldType(const tipb::FieldType & field_type);

// Try best to reverse get TiDB's column info from TiFlash info.
// Used for cases that has absolute need to create a TiDB structure from insufficient knowledge,
// such as mock TiDB table using TiFlash SQL parser, and getting field type for `void` column in DAG.
// Note that not every TiFlash type has a corresponding TiDB type,
// caller should make sure the source type is valid, otherwise exception will be thrown.
ColumnInfo reverseGetColumnInfo(
    const NameAndTypePair & column,
    ColumnID id,
    const Field & default_value,
    bool for_test);

} // namespace DB
