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
    DMSegmentThreadInputStream(const SegmentReadTaskPoolPtr &  task_pool_,
                               const SegmentStreamCreatorPtr & stream_creator_,
                               const ColumnDefines &           columns_to_read_,
                               const String &                  handle_name_,
                               const DataTypePtr &             handle_real_type_,
                               const Context &                 context_)
        : task_pool(task_pool_),
          stream_creator(stream_creator_),
          columns_to_read(columns_to_read_),
          header(createHeader(columns_to_read)),
          handle_name(handle_name_),
          handle_real_type(handle_real_type_),
          context(context_),
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
                cur_stream     = stream_creator->create(*task);
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

        if (handle_real_type && res.has(handle_name))
        {
            auto pos = res.getPositionByName(handle_name);
            convertColumn(res, pos, handle_real_type, context);
            res.getByPosition(pos).type = handle_real_type;
        }
        return res;
    }

private:
    SegmentReadTaskPoolPtr  task_pool;
    SegmentStreamCreatorPtr stream_creator;
    ColumnDefines           columns_to_read;
    Block                   header;
    String                  handle_name;
    DataTypePtr             handle_real_type;
    const Context &         context;

    bool                done = false;
    BlockInputStreamPtr cur_stream;
    UInt64              cur_segment_id;

    Logger * log;
};

} // namespace DM
} // namespace DB