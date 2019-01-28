#include <DataStreams/DedupSortedBlockInputStream.h>

#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>

// #define DEDUP_TRACER
#ifndef DEDUP_TRACER
    #define TRACER(message)
#else
    #define TRACER(message) LOG_TRACE(log, message)
#endif

namespace CurrentMetrics
{
    // TODO: increase it
    extern const Metric QueryThread;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

DedupSortedBlockInputStream::DedupSortedBlockInputStream(BlockInputStreams & inputs_, const SortDescription & description_)
    : description(description_), queue_max(1), source_blocks(inputs_.size(), queue_max),
        output_block(inputs_.size() * queue_max), readers(inputs_.size())
{
    log = &Logger::get("DedupSorted");

    children.insert(children.end(), inputs_.begin(), inputs_.end());

    for (size_t i = 0; i < inputs_.size(); ++i)
        readers.schedule(std::bind(&DedupSortedBlockInputStream::asynFetch, this, i));

    LOG_DEBUG(log, "Start deduping in single thread, using priority-queue");
    dedup_thread = std::make_unique<std::thread>([this] { asynDedupByQueue(); });
}


DedupSortedBlockInputStream::~DedupSortedBlockInputStream()
{
    readers.wait();
    if (dedup_thread && dedup_thread->joinable())
        dedup_thread->join();
}


Block DedupSortedBlockInputStream::readImpl()
{
    DedupingBlockPtr block = output_block.pop();
    if (!*block)
        return Block();
    return block->finalize();
}


void DedupSortedBlockInputStream::asynFetch(size_t position)
{
    while (true)
    {
        Block block = children[position]->read();
        // TRACER("A Origin read, #" << position << ", rows:" << block.rows());
        source_blocks[position]->push(std::make_shared<DedupingBlock>(block, position, true));
        if (!block)
            break;
    }
}


void DedupSortedBlockInputStream::readFromSource(DedupCursors & output, BoundQueue & bounds)
{
    std::vector<DedupingBlockPtr> blocks(source_blocks.size());
    std::vector<SortCursorImpl> cursors_initing(source_blocks.size());

    for (size_t i = 0; i < source_blocks.size(); i++)
    {
        DedupingBlockPtr block = source_blocks[i]->pop();
        if (!*block)
            continue;

        blocks[i] = block;
        TRACER("R Read #" << i << " " << *blocks[i]);
        pushBlockBounds(block, bounds);

        cursors_initing[i] = SortCursorImpl(*block, description);
        if (cursors_initing[i].has_collation)
            throw Exception("Logical error: DedupSortedBlockInputStream does not support collations", ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = 0; i < blocks.size(); i++)
    {
        if (blocks[i])
            output[i] = std::make_shared<DedupCursor>(cursors_initing[i], blocks[i]);
        else
        {
            TRACER("R Read Null #" << i);
            output[i] = std::make_shared<DedupCursor>();
            output_block.push(blocks[i]);
            finished_streams += 1;
        }
    }
}


void DedupSortedBlockInputStream::asynDedupByQueue()
{
    BoundQueue bounds;
    DedupCursors cursors(source_blocks.size());
    readFromSource(cursors, bounds);
    LOG_DEBUG(log, "P Init Bounds " << bounds.str() << " Cursors " << cursors.size());

    CursorQueue queue;
    DedupCursor max;

    while (!bounds.empty() || max)
    {
        if (bounds.empty())
        {
            TRACER("Q SecondWind Check " << max << " Bounds " << bounds);
            if (!max)
                throw Exception("Deduping: if bounds are empty and loops are going on, max must be assigned.");
            if (max.isLast())
            {
                bool finished = outputAndUpdateCursor(cursors, bounds, max);
                finished_streams += finished ? 1 : 0;
                max = DedupCursor();
                if (!finished)
                {
                    TRACER("Q SecondWind " << bounds);
                }
                else
                {
                    TRACER("Q No SecondWind " << bounds);
                    break;
                }
            }
            else
                throw Exception("Deduping: max must be the last row of the block here.");
        }

        DedupBound bound = bounds.top();
        bounds.pop();
        TRACER("P Pop " << bound << " + " << bounds << " Queue " << queue);

        size_t position = bound.position();

        // Skipping optimizations
        if (queue.size() == 1 && (!bound.is_bottom || queue.top().ptr->position() == position))
        {
            size_t skipped = 0;
            DedupCursor & skipping = *(queue.top().ptr);
            DedupCursor from = skipping;
            queue.pop();
            TRACER("Q Skipping Pop " << skipping);

            if (!bound.is_bottom)
            {
                TRACER("Q GreaterEqualB " << skipping);
                skipped = skipping.skipToGreaterEqualBySearch(bound);
                TRACER("Q GreaterEqualE " << skipping << " Skipped " << skipped);
            }
            else if (skipping.position() == position)
            {
                TRACER("Q ToBottomB " << skipping);
                skipped = skipping.assignCursorPos(bound);
                TRACER("Q ToBottomE " << skipping << " Skipped " << skipped);
            }

            if (max && skipped > 0)
            {
                TRACER("Q Skipping DedupB Max " << max << " Cursor " << from);
                dedupCursor(max, from);
                TRACER("Q Skipping DedupE Max " << max << " Cursor " << from);
                if (max.isLast())
                    finished_streams += outputAndUpdateCursor(cursors, bounds, max) ? 1 : 0;

                if (skipping.position() == position)
                {
                    max = DedupCursor();
                }
                else
                {
                    max = skipping;
                    max.backward();
                }
                TRACER("Q Skipping Max Update " << max);
            }

            TRACER("Q Skipping PushBack " << skipping << " ~ " << queue);
            queue.push(CursorPlainPtr(&skipping));
        }

        if (!bound.is_bottom || bound.block->rows() == 1)
        {
            DedupCursor & cursor = *(cursors[position]);
            queue.push(CursorPlainPtr(&cursor));
            TRACER("Q Push " << cursor << " ~ " << queue);
        }

        while (!queue.empty())
        {
            DedupCursor & cursor = *(queue.top().ptr);
            queue.pop();
            TRACER("Q Pop " << cursor << " + " << queue);

            if (max)
            {
                TRACER("Q DedupB Max " << max << " Cursor " << cursor);
                dedupCursor(max, cursor);
                TRACER("Q DedupE Max " << max << " Cursor " << cursor);
                if (max.isLast())
                    finished_streams += outputAndUpdateCursor(cursors, bounds, max) ? 1 : 0;
            }

            max = cursor;
            TRACER("Q Max Update " << max);

            bool range_done = cursor.isTheSame(bound);
            TRACER("Q Range " << (range_done ? "" : "Not ") << "Done " << cursor << " ?= " << bound);

            if (!cursor.isLast())
            {
                cursor.next();
                queue.push(CursorPlainPtr(&cursor));
                TRACER("Q Next Push " << cursor << " ~ " << queue);
            }

            if (range_done)
                break;
        }
    }

    LOG_DEBUG(log, "P All Done. Bounds " << bounds.str() << " Queue " << queue.str() <<
        "Streams finished " << finished_streams << "/" << cursors.size());
}


bool DedupSortedBlockInputStream::outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor)
{
    TRACER("Q Output " << cursor);
    size_t position = cursor.position();
    output_block.push(cursor.block);

    DedupingBlockPtr block = source_blocks[position]->pop();
    if (!*block)
    {
        TRACER("Q Finish #" << position << " Bounds " << bounds << " Cursors " << cursors.size());
        cursors[position] = std::make_shared<DedupCursor>();
        output_block.push(block);
        return true;
    }
    else
    {
        TRACER("Q New Block " << *block << " #" << position);
        pushBlockBounds(block, bounds);
        cursors[position] = std::make_shared<DedupCursor>(SortCursorImpl(*block, description), block);
        return false;
    }
}


void DedupSortedBlockInputStream::pushBlockBounds(const DedupingBlockPtr & block, BoundQueue & bounds)
{
    TRACER("B Push " << *block << " To " << bounds);
    if (block->rows() > 1)
    {
        DedupBound bound(DedupCursor(SortCursorImpl(*block, description), block));
        TRACER("B New Top " << bound);
        bounds.push(bound);
        TRACER("B Push Top To " << bounds);
    }

    DedupBound bottom(DedupCursor(SortCursorImpl(*block, description), block));
    bottom.setToBottom();
    TRACER("B New Bottom " << bottom);
    bounds.push(bottom);
    TRACER("B Push Bottom To " << bounds);
}


}
