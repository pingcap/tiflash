#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/Transaction/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#include <Core/NamesAndTypes.h>

#pragma GCC diagnostic pop

namespace DB
{
using ColumnInfo = TiDB::ColumnInfo;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;

DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info);

DataTypePtr getDataTypeByFieldType(const tipb::FieldType & field_type);

TiDB::CodecFlag getCodecFlagByFieldType(const tipb::FieldType & field_type);

// Try best to reverse get TiDB's column info from TiFlash info.
// Used for cases that has absolute need to create a TiDB structure from insufficient knowledge,
// such as mock TiDB table using TiFlash SQL parser, and getting field type for `void` column in DAG.
// Note that not every TiFlash type has a corresponding TiDB type,
// caller should make sure the source type is valid, otherwise exception will be thrown.
ColumnInfo reverseGetColumnInfo(const NameAndTypePair & column, ColumnID id, const Field & default_value, bool for_test);

} // namespace DB

