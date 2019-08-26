#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace DM
{

class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
public:
    DMSegmentThreadInputStream(const SegmentReadTaskPoolPtr & task_pool_,
                               const ColumnDefines &          columns_to_read_,
                               const String &                 handle_name_,
                               const DataTypePtr &            handle_original_type_,
                               const Context &                context_)
        : task_pool(task_pool_),
          columns_to_read(columns_to_read_),
          header(createHeader(columns_to_read)),
          handle_name(handle_name_),
          handle_original_type(handle_original_type_),
          context(context_)
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
                    return handleBlock(std::move(res));
            }
            else
            {
                cur_stream = {};
            }
        }
    }

    Block handleBlock(Block && original_block)
    {
        Block res;
        for (auto & cd : columns_to_read)
            res.insert(original_block.getByName(cd.name));

        if (handle_original_type && res.has(handle_name))
        {
            auto pos = res.getPositionByName(handle_name);
            convertColumn(res, pos, handle_original_type, context);
            res.getByPosition(pos).type = handle_original_type;
        }
        return res;
    }

private:
    SegmentReadTaskPoolPtr task_pool;
    ColumnDefines          columns_to_read;
    Block                  header;
    String                 handle_name;
    DataTypePtr            handle_original_type;
    const Context &        context;

    bool                done = false;
    BlockInputStreamPtr cur_stream;
};

} // namespace DM
} // namespace DB