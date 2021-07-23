#pragma once

#include <Common/FailPoint.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace FailPoints

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    /// If handle_real_type_ is empty, means do not convert handle column back to real type.
    DMSegmentThreadInputStream(const DMContextPtr &           dm_context_,
                               const SegmentReadTaskPoolPtr & task_pool_,
                               AfterSegmentRead               after_segment_read_,
                               const ColumnDefines &          columns_to_read_,
                               const RSOperatorPtr &          filter_,
                               UInt64                         max_version_,
                               size_t                         expected_block_size_,
                               bool                           is_raw_,
                               bool                           do_range_filter_for_raw_)
        : dm_context(dm_context_),
          task_pool(task_pool_),
          after_segment_read(after_segment_read_),
          columns_to_read(columns_to_read_),
          filter(filter_),
          header(toEmptyBlock(columns_to_read)),
          max_version(max_version_),
          expected_block_size(expected_block_size_),
          is_raw(is_raw_),
          do_range_filter_for_raw(do_range_filter_for_raw_),
          log(&Logger::get("DMSegmentThreadInputStream"))
    {
    }

    String getName() const override { return "DeltaMergeSegmentThread"; }
    Block  getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_;
        return readImpl(filter_, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        if (done)
            return {};
        while (true)
        {
            while (!cur_stream)
            {
                auto task = task_pool->nextTask();
                if (!task)
                {
                    done = true;
                    LOG_DEBUG(log, "Read done");
                    return {};
                }

                cur_segment = task->segment;
                if (is_raw)
                {
                    cur_stream = cur_segment->getInputStreamRaw(*dm_context, columns_to_read, task->read_snapshot, do_range_filter_for_raw);
                }
                else
                {
                    cur_stream = cur_segment->getInputStream(
                        *dm_context,
                        columns_to_read,
                        task->read_snapshot,
                        task->ranges,
                        filter,
                        max_version,
                        std::max(expected_block_size, (size_t)(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows)));
                }
                LOG_TRACE(log, "Start to read segment [" + DB::toString(cur_segment->segmentId()) + "]");
            }
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);

            Block res = cur_stream->read(res_filter, return_filter);

            if (res)
            {
                if (!res.rows())
                    continue;
                else
                    return res;
            }
            else
            {
                after_segment_read(dm_context, cur_segment);
                LOG_TRACE(log, "Finish reading segment [" << cur_segment->segmentId() << "]");
                cur_segment = {};
                cur_stream  = {};
            }
        }
    }

private:
    DMContextPtr           dm_context;
    SegmentReadTaskPoolPtr task_pool;
    AfterSegmentRead       after_segment_read;
    ColumnDefines          columns_to_read;
    RSOperatorPtr          filter;
    Block                  header;
    const UInt64           max_version;
    const size_t           expected_block_size;
    const bool             is_raw;
    const bool             do_range_filter_for_raw;

    bool done = false;

    BlockInputStreamPtr cur_stream;

    SegmentPtr cur_segment;

    Logger * log;
};

} // namespace DM
} // namespace DB
