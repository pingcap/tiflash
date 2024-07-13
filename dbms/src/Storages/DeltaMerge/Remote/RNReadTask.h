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

#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/KVStore/Types.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace DB::DM
{
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;

struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;
} // namespace DB::DM

namespace DB::DM::Remote
{

struct RNReadSegmentMeta
{
    // ======== Fields below uniquely identify a remote segment ========
    const KeyspaceID keyspace_id;
    const TableID physical_table_id;
    const UInt64 segment_id;
    const StoreID store_id;
    // =================================================================

    // ======== Fields below are other supplementary information about the remote segment ========
    const std::vector<UInt64> delta_tinycf_page_ids;
    const std::vector<size_t> delta_tinycf_page_sizes;
    const SegmentPtr segment;
    const SegmentSnapshotPtr segment_snap;
    const String store_address;
    // ===========================================================================================

    // ======== Fields below are information about this reading ========
    const RowKeyRanges read_ranges;
    const DisaggTaskId snapshot_id;
    const DMContextPtr dm_context;
    // =================================================================
};

/// Represent a read from a remote segment. Initially it is built from information
/// returned by Write Node in EstablishDisaggTask.
/// It is a stateful object, fields may be changed when the segment is being read.
class RNReadSegmentTask : boost::noncopyable
{
public:
    // meta is assigned when this SegmentTask is initially created from info returned
    // by Write Node. It is never changed.
    const RNReadSegmentMeta meta;

    static RNReadSegmentTaskPtr buildFromEstablishResp(
        const LoggerPtr & log,
        const Context & db_context,
        const ScanContextPtr & scan_context,
        const RemotePb::RemoteSegment & proto,
        const DisaggTaskId & snapshot_id,
        StoreID store_id,
        const String & store_address,
        KeyspaceID keyspace_id,
        TableID physical_table_id,
        ColumnID pk_col_id);

    String info() const
    {
        return fmt::format(
            "ReadSegmentTask<store_id={} keyspace={} table_id={} segment_id={}>",
            meta.store_id,
            meta.keyspace_id,
            meta.physical_table_id,
            meta.segment_id);
    }

    /// Called from WorkerFetchPages.
    void initColumnFileDataProvider(const RNLocalPageCacheGuardPtr & pages_guard);

    /// Called from WorkerPrepareStreams.
    void initInputStream(
        const ColumnDefines & columns_to_read,
        UInt64 read_tso,
        const PushDownFilterPtr & push_down_filter,
        ReadMode read_mode);

    BlockInputStreamPtr getInputStream() const
    {
        RUNTIME_CHECK(input_stream != nullptr);
        return input_stream;
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    explicit RNReadSegmentTask(const RNReadSegmentMeta & meta_)
        : meta(meta_)
    {}

private:
    BlockInputStreamPtr input_stream;
};

/// "Remote read" is simply reading from these remote segments.
class RNReadTask : boost::noncopyable
{
public:
    const std::vector<RNReadSegmentTaskPtr> segment_read_tasks;

    static RNReadTaskPtr create(const std::vector<RNReadSegmentTaskPtr> & segment_read_tasks_)
    {
        return std::shared_ptr<RNReadTask>(new RNReadTask(segment_read_tasks_));
    }

private:
    explicit RNReadTask(const std::vector<RNReadSegmentTaskPtr> & segment_read_tasks_)
        : segment_read_tasks(segment_read_tasks_)
    {}
};


} // namespace DB::DM::Remote
