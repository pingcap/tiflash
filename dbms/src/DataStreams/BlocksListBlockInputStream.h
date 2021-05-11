#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** A stream of blocks from which you can read the next block from an explicitly provided list.
  * Also see OneBlockInputStream.
  */
class BlocksListBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Acquires the ownership of the block list.
    BlocksListBlockInputStream(BlocksList && list_)
        : list(std::move(list_)), it(list.begin()), end(list.end()) {}

    /// Uses a list of blocks lying somewhere else.
    BlocksListBlockInputStream(const BlocksList::iterator & begin_, const BlocksList::iterator & end_)
        : it(begin_), end(end_) {}

    String getName() const override { return "BlocksList"; }

    Block getHeader() const override
    {
        Block res;
        if (!list.empty())
            for (const auto & elem : list.front())
                res.insert({elem.column->cloneEmpty(), elem.type, elem.name, elem.column_id});
        return res;
    }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();

        Block res = *it;
        ++it;
        return res;
    }

private:
    BlocksList list;
    BlocksList::iterator it;
    const BlocksList::iterator end;
};

}
