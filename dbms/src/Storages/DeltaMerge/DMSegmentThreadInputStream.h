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
                               const ColumnDefines &          columns_to_read_)
        : task_pool(task_pool_),
          stream_creator(stream_creator_),
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

                cur_segment_id = task->segment->segmentId();
                cur_stream     = stream_creator(*task);
                LOG_DEBUG(log, "Start to read segment [" + DB::toString(cur_segment_id) + "]");
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
                cur_segment_id = 0;
                cur_stream     = {};
                LOG_DEBUG(log, "Finish reading segment [" + DB::toString(cur_segment_id) + "]");
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
    ColumnDefines          columns_to_read;
    Block                  header;

    bool                done = false;
    BlockInputStreamPtr cur_stream;
    UInt64              cur_segment_id;

    Logger * log;
};

} // namespace DM
} // namespace DB