#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace DM
{

class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    /// If handle_real_type_ is empty, means do not convert handle column back to real type.
    DMSegmentThreadInputStream(const SegmentReadTaskPoolPtr & task_pool_,
                               const SegmentStreamCreator &   stream_creator_,
                               const AfterSegmentRead &       after_segment_read_,
                               const ColumnDefines &          columns_to_read_)
        : task_pool(task_pool_),
          stream_creator(stream_creator_),
          after_segment_read(after_segment_read_),
          columns_to_read(columns_to_read_),
          header(toEmptyBlock(columns_to_read)),
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
                cur_stream  = stream_creator(*task);
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
                after_segment_read(cur_segment);
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
    SegmentReadTaskPoolPtr task_pool;
    SegmentStreamCreator   stream_creator;
    AfterSegmentRead       after_segment_read;
    ColumnDefines          columns_to_read;
    Block                  header;

    bool                done = false;
    BlockInputStreamPtr cur_stream;
    SegmentPtr          cur_segment;

    Logger * log;
};

} // namespace DM
} // namespace DB