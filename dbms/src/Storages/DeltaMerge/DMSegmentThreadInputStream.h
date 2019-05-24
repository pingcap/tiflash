#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    DMSegmentThreadInputStream(const SegmentReadTaskPoolPtr & task_pool_) : task_pool(task_pool_) {}

    String getName() const override { return "DeltaMergeSegmentThread"; }
    Block  getHeader() const override { return task_pool->getHeader(); }

protected:
    Block readImpl() override
    {
        if (done)
            return {};
        while (true)
        {
            if (!cur_stream)
            {
                cur_stream = task_pool->getTask();
                if (!cur_stream) // we are done.
                {
                    done = true;
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
    bool                   done = false;
    BlockInputStreamPtr    cur_stream;
};
} // namespace DB