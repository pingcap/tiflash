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

#include <Common/ProfileEvents.h>


/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M)                    \
    M(Query)                                   \
    M(FileOpen)                                \
    M(FileOpenFailed)                          \
    M(ReadBufferFromFileDescriptorRead)        \
    M(ReadBufferFromFileDescriptorReadFailed)  \
    M(ReadBufferFromFileDescriptorReadBytes)   \
    M(WriteBufferFromFileDescriptorWrite)      \
    M(WriteBufferFromFileDescriptorWriteBytes) \
                                               \
    M(MarkCacheHits)                           \
    M(MarkCacheMisses)                         \
                                               \
    M(ExternalAggregationCompressedBytes)      \
    M(ExternalAggregationUncompressedBytes)    \
                                               \
    M(ContextLock)                             \
    M(CreatedHTTPConnections)                  \
    M(DNSError)                                \
    M(S3ReadMicroseconds)                      \
    M(S3WriteMicroseconds)                     \
    M(S3ReadRequestsCount)                     \
    M(S3WriteRequestsCount)                    \
    M(S3ReadRequestsErrors)                    \
    M(S3WriteRequestsErrors)                   \
    M(S3ReadRequestsThrottling)                \
    M(S3WriteRequestsThrottling)               \
    M(S3ReadRequestsRedirects)                 \
    M(S3WriteRequestsRedirects)                \
                                               \
    M(RWLockAcquiredReadLocks)                 \
    M(RWLockAcquiredWriteLocks)                \
    M(RWLockReadersWaitMilliseconds)           \
    M(RWLockWritersWaitMilliseconds)           \
                                               \
    M(PSMWritePages)                           \
    M(PSMWriteIOCalls)                         \
    M(PSV3MBlobExpansion)                      \
    M(PSV3MBlobReused)                         \
    M(PSMWriteBytes)                           \
    M(PSMBackgroundWriteBytes)                 \
    M(PSMReadPages)                            \
    M(PSMBackgroundReadBytes)                  \
                                               \
    M(PSMReadIOCalls)                          \
    M(PSMReadBytes)                            \
    M(PSMWriteFailed)                          \
    M(PSMReadFailed)                           \
                                               \
    M(PSMVCCApplyOnCurrentBase)                \
    M(PSMVCCApplyOnCurrentDelta)               \
    M(PSMVCCApplyOnNewDelta)                   \
    M(PSMVCCCompactOnDelta)                    \
    M(PSMVCCCompactOnDeltaRebaseRejected)      \
    M(PSMVCCCompactOnBase)                     \
    M(PSMVCCCompactOnBaseCommit)               \
                                               \
    M(DMWriteBlock)                            \
    M(DMWriteBlockNS)                          \
    M(DMWriteFile)                             \
    M(DMWriteFileNS)                           \
    M(DMDeleteRange)                           \
    M(DMDeleteRangeNS)                         \
    M(DMAppendDeltaPrepare)                    \
    M(DMAppendDeltaPrepareNS)                  \
    M(DMAppendDeltaCommitMemory)               \
    M(DMAppendDeltaCommitMemoryNS)             \
    M(DMAppendDeltaCommitDisk)                 \
    M(DMAppendDeltaCommitDiskNS)               \
    M(DMAppendDeltaCleanUp)                    \
    M(DMAppendDeltaCleanUpNS)                  \
    M(DMPlace)                                 \
    M(DMPlaceNS)                               \
    M(DMPlaceUpsert)                           \
    M(DMPlaceUpsertNS)                         \
    M(DMPlaceDeleteRange)                      \
    M(DMPlaceDeleteRangeNS)                    \
    M(DMDeltaMerge)                            \
    M(DMDeltaMergeNS)                          \
    M(DMSegmentSplit)                          \
    M(DMSegmentSplitNS)                        \
    M(DMSegmentGetSplitPoint)                  \
    M(DMSegmentGetSplitPointNS)                \
    M(DMSegmentMerge)                          \
    M(DMSegmentMergeNS)                        \
    M(DMFlushDeltaCache)                       \
    M(DMFlushDeltaCacheNS)                     \
    M(DMCleanReadRows)                         \
    M(DMSegmentIsEmptyFastPath)                \
    M(DMSegmentIsEmptySlowPath)                \
    M(DMSegmentIngestDataByReplace)            \
    M(DMSegmentIngestDataIntoDelta)            \
                                               \
    M(FileFSync)                               \
                                               \
    M(DMFileFilterNoFilter)                    \
    M(DMFileFilterAftPKAndPackSet)             \
    M(DMFileFilterAftRoughSet)                 \
                                               \
    M(ChecksumDigestBytes)                     \
                                               \
    M(RaftWaitIndexTimeout)                    \
                                               \
    M(S3WriteBytes)                            \
    M(S3ReadBytes)                             \
    M(S3PageReaderReusedFile)                  \
    M(S3PageReaderNotReusedFile)               \
    M(S3PageReaderNotReusedFileReadback)       \
    M(S3PageReaderNotReusedFileChangeFile)     \
    M(S3CreateMultipartUpload)                 \
    M(S3UploadPart)                            \
    M(S3CompleteMultipartUpload)               \
    M(S3PutObject)                             \
    M(S3GetObject)                             \
    M(S3HeadObject)                            \
    M(S3ListObjects)                           \
    M(S3DeleteObject)                          \
    M(S3CopyObject)                            \
    M(S3GetObjectRetry)                        \
    M(S3PutObjectRetry)                        \
    M(S3IORead)                                \
    M(S3IOSeek)                                \
    M(FileCacheHit)                            \
    M(FileCacheMiss)                           \
    M(FileCacheEvict)                          \
    M(S3PutDMFile)                             \
    M(S3PutDMFileRetry)                        \
    M(S3WriteDMFileBytes)                      \
    M(DTDeltaIndexError)

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
