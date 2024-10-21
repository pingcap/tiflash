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

#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/VectorIndex.h>


namespace DB::DM
{

/// Builds a VectorIndex in memory.
class VectorIndexBuilder
{
public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;

    using ProceedCheckFn = std::function<bool()>;

public:
    static VectorIndexBuilderPtr create(IndexID index_id, const TiDB::VectorIndexDefinitionPtr & definition);

    static bool isSupportedType(const IDataType & type);

public:
    explicit VectorIndexBuilder(IndexID index_id_, const TiDB::VectorIndexDefinitionPtr & definition_)
        : index_id(index_id_)
        , definition(definition_)
    {}

    virtual ~VectorIndexBuilder() = default;

    virtual void addBlock( //
        const IColumn & column,
        const ColumnVector<UInt8> * del_mark,
        ProceedCheckFn should_proceed)
        = 0;

    virtual void save(std::string_view path) const = 0;
    virtual void save(WriteBuffer & write_buf) const = 0;

public:
    const IndexID index_id;
    const TiDB::VectorIndexDefinitionPtr definition;
};

/// Views a VectorIndex file.
/// It may nor may not read the whole content of the file into memory.
class VectorIndexViewer
{
public:
    /// The key is the row's offset in the DMFile.
    using Key = VectorIndexBuilder::Key;

    /// True bit means the row is valid and should be kept in the search result.
    /// False bit lets the row filtered out and will search for more results.
    using RowFilter = BitmapFilterView;

public:
    static VectorIndexViewerPtr view(const dtpb::VectorIndexFileProps & file_props, std::string_view path);

public:
    explicit VectorIndexViewer(const dtpb::VectorIndexFileProps & file_props_)
        : file_props(file_props_)
    {}

    virtual ~VectorIndexViewer() = default;

    // Invalid rows in `valid_rows` will be discared when applying the search
    virtual std::vector<Key> search(const ANNQueryInfoPtr & queryInfo, const RowFilter & valid_rows) const = 0;

    virtual size_t size() const = 0;

    // Get the value (i.e. vector content) of a Key.
    virtual void get(Key key, std::vector<Float32> & out) const = 0;

public:
    const dtpb::VectorIndexFileProps file_props;
};

} // namespace DB::DM
