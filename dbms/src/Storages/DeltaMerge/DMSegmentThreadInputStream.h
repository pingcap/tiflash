#pragma once

#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace DM
{

enum SegmentReadType
{
    NORMAL,
    RAW,
    EXPORT_DATA,
};

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
                               SegmentReadType                read_type_,
                               bool                           do_range_filter_for_raw_)
        : dm_context(dm_context_),
          task_pool(task_pool_),
          after_segment_read(after_segment_read_),
          columns_to_read(columns_to_read_),
          filter(filter_),
          header(toEmptyBlock(columns_to_read)),
          max_version(max_version_),
          expected_block_size(expected_block_size_),
          read_type(read_type_),
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
            if (!cur_stream)
            {
                auto task = task_pool->nextTask();
                if (!task)
                {
                    done = true;
                    LOG_DEBUG(log, "Read done");
                    return {};
                }

                cur_segment = task->segment;
                if (read_type == SegmentReadType::RAW)
                {
                    cur_stream = cur_segment->getInputStreamRaw(*dm_context, columns_to_read, task->read_snapshot, do_range_filter_for_raw);
                }
                else if (read_type == SegmentReadType::NORMAL)
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
                else if (read_type == SegmentReadType::EXPORT_DATA)
                {
                    BlockInputStreams streams;
                    for (const auto & range : task->ranges)
                    {
                        streams.push_back(
                            cur_segment->getInputStreamForDataExport(*dm_context, columns_to_read, task->read_snapshot, range));
                    }
                    if (streams.size() == 1)
                    {
                        cur_stream = streams[0];
                    }
                    else
                    {
                        cur_stream = std::make_shared<ConcatBlockInputStream>(streams);
                    }
                }
                else
                {
                    throw DB::TiFlashException("Unknown SegmentReadType: " + std::to_string(read_type), Errors::DeltaTree::Internal);
                }
                LOG_TRACE(log, "Start to read segment [" + DB::toString(cur_segment->segmentId()) + "]");
            }

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
                LOG_TRACE(log, "Finish reading segment [" + DB::toString(cur_segment->segmentId()) + "]");
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
    UInt64                 max_version;
    size_t                 expected_block_size;
    SegmentReadType        read_type;
    bool                   do_range_filter_for_raw;

    bool done = false;

    BlockInputStreamPtr cur_stream;

    SegmentPtr cur_segment;

    Logger * log;
};

} // namespace DM
} // namespace DB