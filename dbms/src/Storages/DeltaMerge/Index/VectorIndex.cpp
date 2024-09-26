// Copyright 2024 PingCAP, Inc.
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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <TiDB/Schema/VectorIndex.h>
#include <tipb/executor.pb.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_QUERY;
} // namespace DB::ErrorCodes

namespace DB::DM
{

bool VectorIndexBuilder::isSupportedType(const IDataType & type)
{
    const auto * nullable = checkAndGetDataType<DataTypeNullable>(&type);
    if (nullable)
        return checkDataTypeArray<DataTypeFloat32>(&*nullable->getNestedType());

    return checkDataTypeArray<DataTypeFloat32>(&type);
}

VectorIndexBuilderPtr VectorIndexBuilder::create(IndexID index_id, const TiDB::VectorIndexDefinitionPtr & definition)
{
    RUNTIME_CHECK(definition->dimension > 0);
    RUNTIME_CHECK(definition->dimension <= TiDB::MAX_VECTOR_DIMENSION);

    switch (definition->kind)
    {
    case tipb::VectorIndexKind::HNSW:
        return std::make_shared<VectorIndexHNSWBuilder>(index_id, definition);
    default:
        throw Exception( //
            ErrorCodes::INCORRECT_QUERY,
            "Unsupported vector index, index_id={} def={}",
            index_id,
            tipb::VectorIndexKind_Name(definition->kind));
    }
}

VectorIndexViewerPtr VectorIndexViewer::view(const dtpb::VectorIndexFileProps & file_props, std::string_view path)
{
    RUNTIME_CHECK(file_props.dimensions() > 0);
    RUNTIME_CHECK(file_props.dimensions() <= TiDB::MAX_VECTOR_DIMENSION);

    tipb::VectorIndexKind kind;
    RUNTIME_CHECK(tipb::VectorIndexKind_Parse(file_props.index_kind(), &kind));

    switch (kind)
    {
    case tipb::VectorIndexKind::HNSW:
        return VectorIndexHNSWViewer::view(file_props, path);
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported vector index {}", file_props.index_kind());
    }
}

} // namespace DB::DM
