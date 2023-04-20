// Copyright 2023 PingCAP, Ltd.
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

#include <DataStreams/HashJoinProbeExec.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>

namespace DB
{
HashJoinProbeExecPtr HashJoinProbeExec::build(
    const JoinPtr & join,
    const BlockInputStreamPtr & probe_stream,
    size_t scan_hash_map_after_probe_stream_index,
    size_t max_block_size)
{
    bool need_scan_hash_map_after_probe = needScanHashMapAfterProbe(join->getKind());
    BlockInputStreamPtr scan_hash_map_stream = nullptr;
    if (need_scan_hash_map_after_probe)
        scan_hash_map_stream = join->createScanHashMapAfterProbeStream(probe_stream->getHeader(), scan_hash_map_after_probe_stream_index, join->getProbeConcurrency(), max_block_size);

    return std::make_shared<HashJoinProbeExec>(
        join,
        nullptr,
        probe_stream,
        need_scan_hash_map_after_probe,
        scan_hash_map_after_probe_stream_index,
        scan_hash_map_stream,
        max_block_size);
}

HashJoinProbeExec::HashJoinProbeExec(
    const JoinPtr & join_,
    const BlockInputStreamPtr & restore_build_stream_,
    const BlockInputStreamPtr & probe_stream_,
    bool need_scan_hash_map_after_probe_,
    size_t scan_hash_map_after_probe_stream_index_,
    const BlockInputStreamPtr & scan_hash_map_after_probe_stream_,
    size_t max_block_size_)
    : join(join_)
    , restore_build_stream(restore_build_stream_)
    , probe_stream(probe_stream_)
    , need_scan_hash_map_after_probe(need_scan_hash_map_after_probe_)
    , scan_hash_map_after_probe_stream_index(scan_hash_map_after_probe_stream_index_)
    , scan_hash_map_after_probe_stream(scan_hash_map_after_probe_stream_)
    , max_block_size(max_block_size_)
    , probe_process_info(max_block_size_)
{}

void HashJoinProbeExec::waitUntilAllBuildFinished()
{
    join->waitUntilAllBuildFinished();
}

void HashJoinProbeExec::waitUntilAllProbeFinished()
{
    join->waitUntilAllProbeFinished();
}

void HashJoinProbeExec::restoreBuild()
{
    restore_build_stream->readPrefix();
    if unlikely (is_cancelled())
        return;
    while (restore_build_stream->read())
    {
        if unlikely (is_cancelled())
            return;
    }
    restore_build_stream->readSuffix();
}

PartitionBlock HashJoinProbeExec::getProbeBlock()
{
    /// Even if spill is enabled, if spill is not triggered during build,
    /// there is no need to dispatch probe block
    if (!join->isSpilled())
    {
        return PartitionBlock{probe_stream->read()};
    }
    else
    {
        while (true)
        {
            if unlikely (is_cancelled())
                return {};

            if (!probe_partition_blocks.empty())
            {
                auto partition_block = std::move(probe_partition_blocks.front());
                probe_partition_blocks.pop_front();
                return partition_block;
            }
            else
            {
                auto new_block = probe_stream->read();
                if (new_block)
                    join->dispatchProbeBlock(new_block, probe_partition_blocks);
                else
                    return {};
            }
        }
    }
}

Block HashJoinProbeExec::probe()
{
    if (probe_process_info.all_rows_joined_finish)
    {
        auto partition_block = getProbeBlock();
        if (partition_block)
        {
            join->checkTypes(partition_block.block);
            probe_process_info.resetBlock(std::move(partition_block.block), partition_block.partition_index);
        }
        else
        {
            return {};
        }
    }
    return join->joinBlock(probe_process_info);
}

HashJoinProbeExecPtr HashJoinProbeExec::tryGetRestoreExec()
{
    if unlikely (is_cancelled())
        return {};

    /// find restore exec in DFS way
    if (auto ret = doTryGetRestoreExec(); ret)
        return ret;

    /// current join has no more partition to restore, so check if previous join still has partition to restore
    return parent ? parent->tryGetRestoreExec() : HashJoinProbeExecPtr{};
}

HashJoinProbeExecPtr HashJoinProbeExec::doTryGetRestoreExec()
{
    assert(join->isEnableSpill());
    /// first check if current join has a partition to restore
    if (join->hasPartitionSpilledWithLock())
    {
        /// get a restore join
        if (auto restore_info = join->getOneRestoreStream(max_block_size); restore_info)
        {
            /// restored join should always enable spill
            assert(restore_info->join && restore_info->join->isEnableSpill());
            size_t scan_hash_map_stream_index = 0;
            if (need_scan_hash_map_after_probe)
            {
                assert(restore_info->scan_hash_map_stream);
                scan_hash_map_stream_index = dynamic_cast<ScanHashMapAfterProbeBlockInputStream *>(restore_info->scan_hash_map_stream.get())->getIndex();
            }
            auto restore_probe_exec = std::make_shared<HashJoinProbeExec>(
                restore_info->join,
                restore_info->build_stream,
                restore_info->probe_stream,
                need_scan_hash_map_after_probe,
                scan_hash_map_stream_index,
                restore_info->scan_hash_map_stream,
                max_block_size);
            restore_probe_exec->parent = shared_from_this();
            restore_probe_exec->setCancellationHook(is_cancelled);
            return restore_probe_exec;
        }
        assert(join->hasPartitionSpilledWithLock() == false);
    }
    return {};
}

void HashJoinProbeExec::cancel()
{
    /// Join::wakeUpAllWaitingThreads wakes up all the threads waiting in Join::waitUntilAllBuildFinished/waitUntilAllProbeFinished,
    /// and once this function is called, all the subsequent call of Join::waitUntilAllBuildFinished/waitUntilAllProbeFinished will
    /// skip waiting directly.
    /// HashJoinProbeBlockInputStream::cancel will be called in two cases:
    /// 1. the query is cancelled by the caller or meet error: in this case, wake up all waiting threads is safe, because no data
    ///    will be used data anymore
    /// 2. the query is executed normally, and one of the data stream has read an empty block, the the data stream and all its
    ///    children will call `cancel(false)`, in this case, there is two sub-cases
    ///    a. the data stream read an empty block because of EOF, then it means there must be no threads waiting in Join, so wake
    ///       up all waiting threads is safe because actually there is no threads to be waken up
    ///    b. the data stream read an empty block because of early exit of some executor(like limit), in this case, waking up the
    ///       waiting threads is not 100% safe because if the probe thread is waken up when build is not finished yet, it may get
    ///       wrong result. Currently, the execution framework ensures that when any of the data stream read empty block because
    ///       of early exit, no further data will be used, and in order to make sure no wrong result is generated
    ///       - for threads reading joined data: will return empty block if build is not finished yet
    ///       - for threads reading non joined data: will return empty block if build or probe is not finished yet
    join->wakeUpAllWaitingThreads();
    if (scan_hash_map_after_probe_stream != nullptr)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(scan_hash_map_after_probe_stream.get()); p_stream != nullptr)
            p_stream->cancel(false);
    }
    if (probe_stream != nullptr)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(probe_stream.get()); p_stream != nullptr)
            p_stream->cancel(false);
    }
    if (restore_build_stream != nullptr)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_build_stream.get()); p_stream != nullptr)
            p_stream->cancel(false);
    }
}

void HashJoinProbeExec::meetError(const String & error_message)
{
    join->meetError(error_message);
}

void HashJoinProbeExec::onProbeStart()
{
    if (join->isRestoreJoin())
        probe_stream->readPrefix();
}

bool HashJoinProbeExec::onProbeFinish()
{
    if (join->isRestoreJoin())
        probe_stream->readSuffix();
    join->finishOneProbe();
    return !need_scan_hash_map_after_probe && !join->isEnableSpill();
}

void HashJoinProbeExec::onScanHashMapAfterProbeStart()
{
    assert(scan_hash_map_after_probe_stream != nullptr);
    scan_hash_map_after_probe_stream->readPrefix();
}

Block HashJoinProbeExec::fetchScanHashMapData()
{
    assert(scan_hash_map_after_probe_stream != nullptr);
    return scan_hash_map_after_probe_stream->read();
}

bool HashJoinProbeExec::onScanHashMapAfterProbeFinish()
{
    scan_hash_map_after_probe_stream->readSuffix();
    if (!join->isEnableSpill())
    {
        return true;
    }
    else
    {
        join->finishOneNonJoin(scan_hash_map_after_probe_stream_index);
        return false;
    }
}
} // namespace DB
