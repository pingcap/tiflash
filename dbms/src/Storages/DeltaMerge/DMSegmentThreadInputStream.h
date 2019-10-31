#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace DM
{

class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    /// If handle_real_type_ is empty, means do not convert handle column back to real type.
    DMSegmentThreadInputStream(const DMContextPtr &           dm_context_,
                               const StorageSnapshotPtr &     storage_snap_,
                               const SegmentReadTaskPoolPtr & task_pool_,
                               AfterSegmentRead               after_segment_read_,
                               const ColumnDefines &          columns_to_read_,
                               const RSOperatorPtr &          filter_,
                               UInt64                         max_version_,
                               size_t                         expected_block_size_,
                               bool                           is_raw_,
                               bool                           do_range_filter_for_raw_)
        : dm_context(dm_context_),
          storage_snap(storage_snap_),
          task_pool(task_pool_),
          after_segment_read(after_segment_read_),
          columns_to_read(columns_to_read_),
          filter(filter_),
          header(toEmptyBlock(columns_to_read)),
          max_version(max_version_),
          expected_block_size(expected_block_size_),
          is_raw(is_raw_),
          do_range_filter(do_range_filter_for_raw_),
          log(&Logger::get("DMSegmentThreadInputStream"))
    {
    }

    String getName() const override { return "DeltaMergeSegmentThread"; }
    Block  getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (done)
            return {};
        while (true)
        {
            if (!cur_stream)
            {
                auto task = task_pool->nextTask();
                if (!task)
                {
                    done = true;
                    return {};
                }

                cur_segment = task->segment;
                if (is_raw)
                {
                    cur_stream
                        = cur_segment->getInputStreamRaw(*dm_context, columns_to_read, task->read_snapshot, *storage_snap, do_range_filter);
                }
                else
                {
                    cur_stream = cur_segment->getInputStream(
                        *dm_context,
                        columns_to_read,
                        task->read_snapshot,
                        *storage_snap,
                        task->ranges,
                        filter,
                        max_version,
                        std::max(expected_block_size, (size_t)(dm_context->db_context.getSettingsRef().dm_segment_stable_chunk_rows)));
                }
                LOG_TRACE(log, "Start to read segment [" + DB::toString(cur_segment->segmentId()) + "]");
            }

            Block res = cur_stream->read();
            if (res)
            {
                if (!res.rows())
                    continue;
                else
                    return handleBlock(std::move(res));
            }
            else
            {
                after_segment_read(dm_context, cur_segment);
                LOG_TRACE(log, "Finish reading segment [" + DB::toString(cur_segment->segmentId()) + "]");
                cur_segment = {};
                cur_stream  = {};
            }
        }
    }

    Block handleBlock(Block && original_block)
    {
        Block res;
        for (auto & cd : columns_to_read)
            res.insert(original_block.getByName(cd.name));
        return res;
    }

private:
    DMContextPtr           dm_context;
    StorageSnapshotPtr     storage_snap;
    SegmentReadTaskPoolPtr task_pool;
    AfterSegmentRead       after_segment_read;
    ColumnDefines          columns_to_read;
    RSOperatorPtr          filter;
    Block                  header;

    UInt64 max_version;
    size_t expected_block_size;
    bool   is_raw;
    bool   do_range_filter;

    bool                done = false;
    BlockInputStreamPtr cur_stream;
    SegmentPtr          cur_segment;

    Logger * log;
};

} // namespace DM
} // namespace DB