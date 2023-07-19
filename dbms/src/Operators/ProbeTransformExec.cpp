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

#include <Operators/ProbeTransformExec.h>
#include <Flash/Executor/PipelineExecutorContext.h>

namespace DB
{
ProbeTransformExec::ProbeTransformExec(
    PipelineExecutorContext & exec_context_,
    size_t op_index_,
    const JoinPtr & join_,
    BlockInputStreamPtr scan_hash_map_after_probe_stream_,
    UInt64 max_block_size_)
    : exec_context(exec_context_)
    , op_index(op_index_)
    , join(join_)
    , scan_hash_map_after_probe_stream(scan_hash_map_after_probe_stream_)
    , max_block_size(max_block_size_)
{
}

ProbeTransformExecPtr ProbeTransformExec::tryGetRestoreExec()
{
    if unlikely (exec_context.isCancelled())
        return {};

    assert(join->isEnableSpill());
    // first check if current join has a partition to restore
    if (join->hasPartitionSpilledWithLock())
    {
        // get a restore join
        if (auto restore_info = join->getOneRestoreStream(max_block_size); restore_info)
        {
            // restored join should always enable spill
            assert(restore_info->join && restore_info->join->isEnableSpill());
            auto restore_probe_exec = std::make_shared<ProbeTransformExec>(
                exec_context,
                restore_info->stream_index,
                restore_info->join,
                restore_info->scan_hash_map_stream,
                max_block_size);
            restore_probe_exec->parent = shared_from_this();
            // TODO start async restore build/probe tasks here.
            return restore_probe_exec;
        }
        assert(join->hasPartitionSpilledWithLock() == false);
    }

    // current join has no more partition to restore, so check if previous join still has partition to restore
    return parent ? parent->tryGetRestoreExec() : ProbeTransformExecPtr{};
}
} // namespace DB
