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

#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
BoundedSSTFilesToBlockInputStream::BoundedSSTFilesToBlockInputStream( //
    SSTFilesToBlockInputStreamPtr child,
    const ColId pk_column_id_,
    const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
    size_t split_id)
    : pk_column_id(pk_column_id_)
    , _raw_child(std::move(child))
{
    const bool is_common_handle = schema_snap->is_common_handle;
    // Initlize `mvcc_compact_stream`
    // First refine the boundary of blocks. Note that the rows decoded from SSTFiles are sorted by primary key asc, timestamp desc
    // (https://github.com/tikv/tikv/blob/v5.0.1/components/txn_types/src/types.rs#L103-L108).
    // While DMVersionFilter require rows sorted by primary key asc, timestamp asc, so we need an extra sort in PKSquashing.
    auto stream = std::make_shared<PKSquashingBlockInputStream</*need_extra_sort=*/true>>(
        _raw_child,
        pk_column_id,
        is_common_handle,
        split_id);
    mvcc_compact_stream = std::make_unique<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
        stream,
        *(schema_snap->column_defines),
        _raw_child->opts.gc_safepoint,
        is_common_handle);
}

void BoundedSSTFilesToBlockInputStream::readPrefix()
{
    mvcc_compact_stream->readPrefix();
}

void BoundedSSTFilesToBlockInputStream::readSuffix()
{
    mvcc_compact_stream->readSuffix();
}

Block BoundedSSTFilesToBlockInputStream::read()
{
    return mvcc_compact_stream->read();
}

SSTFilesToBlockInputStream::ProcessKeys BoundedSSTFilesToBlockInputStream::getProcessKeys() const
{
    return _raw_child->process_keys;
}


size_t BoundedSSTFilesToBlockInputStream::getSplitId() const
{
    return _raw_child->getSplitId();
}

RegionPtr BoundedSSTFilesToBlockInputStream::getRegion() const
{
    return _raw_child->region;
}

std::tuple<size_t, size_t, size_t, UInt64> //
BoundedSSTFilesToBlockInputStream::getMvccStatistics() const
{
    return std::make_tuple(
        mvcc_compact_stream->getEffectiveNumRows(),
        mvcc_compact_stream->getNotCleanRows(),
        mvcc_compact_stream->getDeletedRows(),
        mvcc_compact_stream->getGCHintVersion());
}

} // namespace DM
} // namespace DB
