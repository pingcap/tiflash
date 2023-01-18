// Copyright 2022 PingCAP, Ltd.
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

#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M)                    \
    M(Query)                                    \
    M(Merge)                                    \
    M(ReplicatedFetch)                          \
    M(ReplicatedSend)                           \
    M(ReplicatedChecks)                         \
    M(BackgroundPoolTask)                       \
    M(DiskSpaceReservedForMerge)                \
    M(DistributedSend)                          \
    M(QueryPreempted)                           \
    M(TCPConnection)                            \
    M(HTTPConnection)                           \
    M(InterserverConnection)                    \
    M(OpenFileForRead)                          \
    M(OpenFileForWrite)                         \
    M(OpenFileForReadWrite)                     \
    M(SendExternalTables)                       \
    M(QueryThread)                              \
    M(ReadonlyReplica)                          \
    M(LeaderReplica)                            \
    M(MemoryTracking)                           \
    M(MemoryTrackingInBackgroundProcessingPool) \
    M(MemoryTrackingForMerges)                  \
    M(LeaderElection)                           \
    M(EphemeralNode)                            \
    M(DelayedInserts)                           \
    M(ContextLockWait)                          \
    M(StorageBufferRows)                        \
    M(StorageBufferBytes)                       \
    M(DictCacheRequests)                        \
    M(Revision)                                 \
    M(PSMVCCNumSnapshots)                       \
    M(PSMVCCSnapshotsList)                      \
    M(PSMVCCNumDelta)                           \
    M(PSMVCCNumBase)                            \
    M(RWLockWaitingReaders)                     \
    M(RWLockWaitingWriters)                     \
    M(RWLockActiveReaders)                      \
    M(RWLockActiveWriters)                      \
    M(StoreSizeCapacity)                        \
    M(StoreSizeAvailable)                       \
    M(StoreSizeUsed)                            \
    M(DT_DeltaMerge)                            \
    M(DT_DeltaCompact)                          \
    M(DT_DeltaFlush)                            \
    M(DT_SegmentSplit)                          \
    M(DT_SegmentMerge)                          \
    M(DT_PlaceIndexUpdate)                      \
    M(DT_DeltaMergeTotalBytes)                  \
    M(DT_DeltaMergeTotalRows)                   \
    M(DT_DeltaIndexCacheSize)                   \
    M(RaftNumSnapshotsPendingApply)             \
    M(RateLimiterPendingWriteRequest)           \
    M(DT_SegmentReadTasks)                      \
    M(DT_SnapshotOfRead)                        \
    M(DT_SnapshotOfReadRaw)                     \
    M(DT_SnapshotOfSegmentSplit)                \
    M(DT_SnapshotOfSegmentMerge)                \
    M(DT_SnapshotOfDeltaMerge)                  \
    M(DT_SnapshotOfDeltaCompact)                \
    M(DT_SnapshotOfPlaceIndex)                  \
    M(IOLimiterPendingBgWriteReq)               \
    M(IOLimiterPendingFgWriteReq)               \
    M(IOLimiterPendingBgReadReq)                \
    M(IOLimiterPendingFgReadReq)                \
    M(StoragePoolV2Only)                        \
    M(StoragePoolV3Only)                        \
    M(StoragePoolMixMode)                       \
    M(RegionPersisterRunMode)                   \
    M(GlobalStorageRunMode)

namespace CurrentMetrics
{
#define M(NAME) extern const Metric NAME = __COUNTER__;
APPLY_FOR_METRICS(M)
#undef M
constexpr Metric END = __COUNTER__;

std::atomic<Value> values[END]{}; /// Global variable, initialized by zeros.

const char * getDescription(Metric event)
{
    static const char * descriptions[] = {
#define M(NAME) #NAME,
        APPLY_FOR_METRICS(M)
#undef M
    };

    return descriptions[event];
}

Metric end()
{
    return END;
}
} // namespace CurrentMetrics

#undef APPLY_FOR_METRICS
