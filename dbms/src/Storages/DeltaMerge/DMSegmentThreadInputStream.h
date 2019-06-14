#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    DMSegmentThreadInputStream(const SegmentReadTaskPoolPtr & task_pool_)
        : task_pool(task_pool_)
    {
    }

    String getName() const override { return "DeltaMergeSegmentThread"; }
    Block  getHeader() const override { return task_pool->getHeader(); }

protected:
    Block readImpl() override
    {
        if (!task_pool)
            return {};
        while (true)
        {
            if (!cur_stream)
            {
                cur_stream = task_pool->getTask();
                if (!cur_stream) // we are done.
                {
                    task_pool = {};
                    return {};
                }
            }

            Block res = cur_stream->read();
            if (res)
            {
                if (!res.rows())
                    continue;
                else
                    return res;
            }
            else
            {
                cur_stream = {};
            }
        }
    }

private:
    SegmentReadTaskPoolPtr task_pool;
    BlockInputStreamPtr cur_stream;
};
} // namespace DB