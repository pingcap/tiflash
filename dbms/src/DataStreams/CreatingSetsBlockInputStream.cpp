#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/Join.h>
#include <Interpreters/Set.h>
#include <Storages/IStorage.h>

#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(const BlockInputStreamPtr & input,
    std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
    const SizeLimits & network_transfer_limits, Int64 mpp_task_id_)
    : subqueries_for_sets_list(std::move(subqueries_for_sets_list_)), network_transfer_limits(network_transfer_limits), mpp_task_id(mpp_task_id_)
{
    init(input);
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input, const SubqueriesForSets & subqueries_for_sets, const SizeLimits & network_transfer_limits)
    : network_transfer_limits(network_transfer_limits)
{
    subqueries_for_sets_list.push_back(subqueries_for_sets);
    init(input);
}

void CreatingSetsBlockInputStream::init(const BlockInputStreamPtr & input)
{
    for (auto & subqueries_for_sets : subqueries_for_sets_list)
    {
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source)
            {
                children.push_back(elem.second.source);

                if (elem.second.set)
                    elem.second.set->setHeader(elem.second.source->getHeader());
            }
        }
    }

    children.push_back(input);
}


Block CreatingSetsBlockInputStream::readImpl()
{
    Block res;

    createAll();

    if (isCancelledOrThrowIfKilled())
        return res;

    return children.back()->read();
}


void CreatingSetsBlockInputStream::readPrefixImpl() { createAll(); }


Block CreatingSetsBlockInputStream::getTotals()
{
    auto input = dynamic_cast<IProfilingBlockInputStream *>(children.back().get());

    if (input)
        return input->getTotals();
    else
        return totals;
}


void CreatingSetsBlockInputStream::createAll()
{
    if (!created)
    {
        for (auto & subqueries_for_sets : subqueries_for_sets_list)
        {
            for (auto &elem : subqueries_for_sets)
            {
                if (elem.second.join)
                    elem.second.join->setFinishBuildTable(false);
            }
        }
        for (auto & subqueries_for_sets : subqueries_for_sets_list)
        {
            for (auto & elem : subqueries_for_sets)
            {
                if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
                {
                    if (isCancelledOrThrowIfKilled())
                        return;

                    workers.push_back(std::thread(&CreatingSetsBlockInputStream::createOne, this, std::ref(elem.second), current_memory_tracker));
                }
            }
        }
        for (auto & work : workers)
        {
            work.join();
        }

        created = true;
    }
}

void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery, MemoryTracker * memory_tracker)
{
    current_memory_tracker = memory_tracker;
    LOG_TRACE(log,
        (subquery.set ? "Creating set. " : "") << (subquery.join ? "Creating join. " : "")
                                               << (subquery.table ? "Filling temporary table. " : "") << " for task "
                                               << std::to_string(mpp_task_id));
    Stopwatch watch;

    BlockOutputStreamPtr table_out;
    if (subquery.table)
        table_out = subquery.table->write({}, {});


    bool done_with_set = !subquery.set;
    bool done_with_join = !subquery.join;
    bool done_with_table = !subquery.table;

    if (done_with_set && done_with_join && done_with_table)
        throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

    if (table_out)
        table_out->writePrefix();
    std::unique_ptr<ThreadPool> join_build_thread_pool = nullptr;
    if (subquery.join != nullptr && subquery.join->getBuildConcurrency() > 1)
        join_build_thread_pool = std::make_unique<ThreadPool>(subquery.join->getBuildConcurrency());

    while (Block block = subquery.source->read())
    {
        if (isCancelled())
        {
            LOG_DEBUG(log, "Query was cancelled during set / join or temporary table creation.");
            return;
        }

        if (!done_with_set)
        {
            if (!subquery.set->insertFromBlock(block, /*fill_set_elements=*/false))
                done_with_set = true;
        }

        if (!done_with_join)
        {
            if (join_build_thread_pool == nullptr)
            {
                if (!subquery.join->insertFromBlock(block))
                    done_with_join = true;
            }
            else
            {
                subquery.join->insertFromBlockASync(block, *join_build_thread_pool);
                if (subquery.join->isBuildSetExceeded())
                    done_with_join = true;
            }
        }

        if (!done_with_table)
        {
            block = materializeBlock(block);
            table_out->write(block);

            rows_to_transfer += block.rows();
            bytes_to_transfer += block.bytes();

            if (!network_transfer_limits.check(
                    rows_to_transfer, bytes_to_transfer, "IN/JOIN external table", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                done_with_table = true;
        }

        if (done_with_set && done_with_join && done_with_table)
        {
            if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
                profiling_in->cancel(false);

            break;
        }
    }

    if (join_build_thread_pool != nullptr)
        join_build_thread_pool->wait();

    if (subquery.join)
        subquery.join->setFinishBuildTable(true);

    if (table_out)
        table_out->writeSuffix();

    watch.stop();

    size_t head_rows = 0;
    if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
    {
        const BlockStreamProfileInfo & profile_info = profiling_in->getProfileInfo();

        head_rows = profile_info.rows;

        if (subquery.join)
            subquery.join->setTotals(profiling_in->getTotals());
    }

    if (head_rows != 0)
    {
        std::stringstream msg;
        msg << std::fixed << std::setprecision(3);
        msg << "Created. ";

        if (subquery.set)
            msg << "Set with " << subquery.set->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.join)
            msg << "Join with " << subquery.join->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.table)
            msg << "Table with " << head_rows << " rows. ";

        msg << "In " << watch.elapsedSeconds() << " sec. ";
        msg << "using " << std::to_string(subquery.join == nullptr ? 1 : subquery.join->getBuildConcurrency()) << " threads ";
        msg << "for task " << std::to_string(mpp_task_id) << ".";
        LOG_DEBUG(log, msg.rdbuf());
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result for task " << std::to_string(mpp_task_id) << ".");
    }
}

} // namespace DB
