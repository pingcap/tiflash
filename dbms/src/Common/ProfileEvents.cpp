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

#include <Common/ProfileEvents.h>


/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M)                     \
    M(Query)                                    \
    M(SelectQuery)                              \
    M(InsertQuery)                              \
    M(DeleteQuery)                              \
    M(FileOpen)                                 \
    M(FileOpenFailed)                           \
    M(Seek)                                     \
    M(ReadBufferFromFileDescriptorRead)         \
    M(ReadBufferFromFileDescriptorReadFailed)   \
    M(ReadBufferFromFileDescriptorReadBytes)    \
    M(WriteBufferFromFileDescriptorWrite)       \
    M(WriteBufferFromFileDescriptorWriteFailed) \
    M(WriteBufferFromFileDescriptorWriteBytes)  \
    M(ReadBufferAIORead)                        \
    M(ReadBufferAIOReadBytes)                   \
    M(WriteBufferAIOWrite)                      \
    M(WriteBufferAIOWriteBytes)                 \
    M(ReadCompressedBytes)                      \
    M(CompressedReadBufferBlocks)               \
    M(CompressedReadBufferBytes)                \
    M(UncompressedCacheHits)                    \
    M(UncompressedCacheMisses)                  \
    M(UncompressedCacheWeightLost)              \
    M(IOBufferAllocs)                           \
    M(IOBufferAllocBytes)                       \
    M(ArenaAllocChunks)                         \
    M(ArenaAllocBytes)                          \
    M(FunctionExecute)                          \
    M(TableFunctionExecute)                     \
    M(MarkCacheHits)                            \
    M(MarkCacheMisses)                          \
    M(CreatedReadBufferOrdinary)                \
    M(CreatedReadBufferAIO)                     \
    M(CreatedWriteBufferOrdinary)               \
    M(CreatedWriteBufferAIO)                    \
                                                \
    M(InsertedRows)                             \
    M(InsertedBytes)                            \
    M(DelayedInserts)                           \
    M(RejectedInserts)                          \
    M(DelayedInsertsMilliseconds)               \
    M(DuplicatedInsertedBlocks)                 \
                                                \
    M(DistributedConnectionFailTry)             \
    M(DistributedConnectionMissingTable)        \
    M(DistributedConnectionStaleReplica)        \
    M(DistributedConnectionFailAtAll)           \
                                                \
    M(CompileAttempt)                           \
    M(CompileSuccess)                           \
                                                \
    M(ExternalSortWritePart)                    \
    M(ExternalSortMerge)                        \
    M(ExternalAggregationWritePart)             \
    M(ExternalAggregationMerge)                 \
    M(ExternalAggregationCompressedBytes)       \
    M(ExternalAggregationUncompressedBytes)     \
                                                \
    M(SlowRead)                                 \
    M(ReadBackoff)                              \
                                                \
    M(RegexpCreated)                            \
    M(ContextLock)                              \
                                                \
    M(StorageBufferFlush)                       \
    M(StorageBufferErrorOnFlush)                \
    M(StorageBufferPassedAllMinThresholds)      \
    M(StorageBufferPassedTimeMaxThreshold)      \
    M(StorageBufferPassedRowsMaxThreshold)      \
    M(StorageBufferPassedBytesMaxThreshold)     \
                                                \
    M(DictCacheKeysRequested)                   \
    M(DictCacheKeysRequestedMiss)               \
    M(DictCacheKeysRequestedFound)              \
    M(DictCacheKeysExpired)                     \
    M(DictCacheKeysNotFound)                    \
    M(DictCacheKeysHit)                         \
    M(DictCacheRequestTimeNs)                   \
    M(DictCacheRequests)                        \
    M(DictCacheLockWriteNs)                     \
    M(DictCacheLockReadNs)                      \
                                                \
    M(DistributedSyncInsertionTimeoutExceeded)  \
    M(DataAfterMergeDiffersFromReplica)         \
    M(PolygonsAddedToPool)                      \
    M(PolygonsInPoolAllocatedBytes)             \
    M(RWLockAcquiredReadLocks)                  \
    M(RWLockAcquiredWriteLocks)                 \
    M(RWLockReadersWaitMilliseconds)            \
    M(RWLockWritersWaitMilliseconds)            \
                                                \
    M(PSMWritePages)                            \
    M(PSMWriteIOCalls)                          \
    M(PSV3MBlobExpansion)                       \
    M(PSV3MBlobReused)                          \
    M(PSMWriteBytes)                            \
    M(PSMBackgroundWriteBytes)                  \
    M(PSMReadPages)                             \
    M(PSMBackgroundReadBytes)                   \
                                                \
    M(PSMReadIOCalls)                           \
    M(PSMReadBytes)                             \
    M(PSMWriteFailed)                           \
    M(PSMReadFailed)                            \
                                                \
    M(PSMVCCApplyOnCurrentBase)                 \
    M(PSMVCCApplyOnCurrentDelta)                \
    M(PSMVCCApplyOnNewDelta)                    \
    M(PSMVCCCompactOnDelta)                     \
    M(PSMVCCCompactOnDeltaRebaseRejected)       \
    M(PSMVCCCompactOnBase)                      \
    M(PSMVCCCompactOnBaseCommit)                \
                                                \
    M(DMWriteBytes)                             \
    M(DMWriteBlock)                             \
    M(DMWriteBlockNS)                           \
    M(DMWriteFile)                              \
    M(DMWriteFileNS)                            \
    M(DMDeleteRange)                            \
    M(DMDeleteRangeNS)                          \
    M(DMAppendDeltaPrepare)                     \
    M(DMAppendDeltaPrepareNS)                   \
    M(DMAppendDeltaCommitMemory)                \
    M(DMAppendDeltaCommitMemoryNS)              \
    M(DMAppendDeltaCommitDisk)                  \
    M(DMAppendDeltaCommitDiskNS)                \
    M(DMAppendDeltaCleanUp)                     \
    M(DMAppendDeltaCleanUpNS)                   \
    M(DMPlace)                                  \
    M(DMPlaceNS)                                \
    M(DMPlaceUpsert)                            \
    M(DMPlaceUpsertNS)                          \
    M(DMPlaceDeleteRange)                       \
    M(DMPlaceDeleteRangeNS)                     \
    M(DMDeltaMerge)                             \
    M(DMDeltaMergeNS)                           \
    M(DMSegmentSplit)                           \
    M(DMSegmentSplitNS)                         \
    M(DMSegmentGetSplitPoint)                   \
    M(DMSegmentGetSplitPointNS)                 \
    M(DMSegmentMerge)                           \
    M(DMSegmentMergeNS)                         \
    M(DMFlushDeltaCache)                        \
    M(DMFlushDeltaCacheNS)                      \
    M(DMCleanReadRows)                          \
                                                \
    M(FileFSync)                                \
                                                \
    M(DMFileFilterNoFilter)                     \
    M(DMFileFilterAftPKAndPackSet)              \
    M(DMFileFilterAftRoughSet)                  \
                                                \
    M(ChecksumDigestBytes)                      \
                                                \
    M(RaftWaitIndexTimeout)

namespace ProfileEvents
{
#define M(NAME) extern const Event NAME = __COUNTER__;
APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

std::atomic<Count> counters[END]{}; /// Global variable, initialized by zeros.

const char * getDescription(Event event)
{
    static const char * descriptions[] = {
#define M(NAME) #NAME,
        APPLY_FOR_EVENTS(M)
#undef M
    };

    return descriptions[event];
}

Event end()
{
    return END;
}
} // namespace ProfileEvents

#undef APPLY_FOR_EVENTS
