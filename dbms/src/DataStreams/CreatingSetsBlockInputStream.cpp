#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/Join.h>
#include <Interpreters/Set.h>
#include <Storages/IStorage.h>

#include <iomanip>


namespace DB
{
namespace FailPoints
{
extern const char exception_in_creating_set_input_stream[];
extern const char exception_mpp_hash_build[];
} // namespace FailPoints
namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
    const SizeLimits & network_transfer_limits,
    const MPPTaskId & mpp_task_id_,
    const LogWithPrefixPtr & log_)
    : subqueries_for_sets_list(std::move(subqueries_for_sets_list_))
    , network_transfer_limits(network_transfer_limits)
    , mpp_task_id(mpp_task_id_)
    , log(getMPPTaskLog(log_, name, mpp_task_id))
{
    init(input);
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    const SubqueriesForSets & subqueries_for_sets,
    const SizeLimits & network_transfer_limits,
    const LogWithPrefixPtr & log_)
    : network_transfer_limits(network_transfer_limits)
    , log(getMPPTaskLog(log_, name, mpp_task_id))
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


void CreatingSetsBlockInputStream::readPrefixImpl()
{
    createAll();
}


Block CreatingSetsBlockInputStream::getTotals()
{
    auto * input = dynamic_cast<IProfilingBlockInputStream *>(children.back().get());

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
            for (auto & elem : subqueries_for_sets)
            {
                if (elem.second.join)
                    elem.second.join->setBuildTableState(Join::BuildTableState::WAITING);
            }
        }
        Stopwatch watch;
        auto thread_manager = newThreadManager();
        for (auto & subqueries_for_sets : subqueries_for_sets_list)
        {
            for (auto & elem : subqueries_for_sets)
            {
                if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
                {
                    if (isCancelledOrThrowIfKilled())
                        return;
                    thread_manager->schedule(true, "CreatingSets", [this, &item = elem.second] { createOne(item); });
                    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_in_creating_set_input_stream);
                }
            }
        }

        thread_manager->wait();

        if (!exception_from_workers.empty())
        {
            LOG_FMT_ERROR(log, "Creating all tasks of {} takes {} sec with exception and rethrow the first of total {} exceptions", mpp_task_id.toString(), watch.elapsedSeconds(), exception_from_workers.size());
            std::rethrow_exception(exception_from_workers.front());
        }
        LOG_FMT_DEBUG(log, "Creating all tasks of {} takes {} sec. ", mpp_task_id.toString(), watch.elapsedSeconds());

        created = true;
    }
}

void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery)
{
    auto log_msg = fmt::format("{} for task {}",
                               subquery.set ? "Creating set. " : subquery.join ? "Creating join. "
                                   : subquery.table                            ? "Filling temporary table. "
                                                                               : "null subquery",
                               mpp_task_id.toString());
    Stopwatch watch;
    try
    {
        LOG_FMT_DEBUG(log, "{}", log_msg);
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

        while (Block block = subquery.source->read())
        {
            if (isCancelled())
            {
                LOG_FMT_DEBUG(log, "Query was cancelled during set / join or temporary table creation.");
                return;
            }

            if (!done_with_set)
            {
                if (!subquery.set->insertFromBlock(block, /*fill_set_elements=*/false))
                    done_with_set = true;
            }

            if (!done_with_join)
            {
                // move building hash tables into `HashJoinBuildBlockInputStream`, so that fetch block and insert block into a hash table are
                // running into a thread, avoiding generating more threads.
                if (subquery.join->isBuildSetExceeded())
                    done_with_join = true;
            }

            if (!done_with_table)
            {
                block = materializeBlock(block);
                table_out->write(block);

                rows_to_transfer += block.rows();
                bytes_to_transfer += block.bytes();

                if (!network_transfer_limits.check(
                        rows_to_transfer,
                        bytes_to_transfer,
                        "IN/JOIN external table",
                        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                    done_with_table = true;
            }

            if (done_with_set && done_with_join && done_with_table)
            {
                if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
                    profiling_in->cancel(false);

                break;
            }
        }


        if (subquery.join)
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_build);
            subquery.join->setBuildTableState(Join::BuildTableState::SUCCEED);
        }

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

            LOG_FMT_DEBUG(log, "{}", msg.rdbuf()->str());
        }
        else
        {
            LOG_FMT_DEBUG(log, "Subquery has empty result for task {}. ", mpp_task_id.toString());
        }
    }
    catch (...)
    {
        std::unique_lock<std::mutex> lock(exception_mutex);
        exception_from_workers.push_back(std::current_exception());
        if (subquery.join)
            subquery.join->setBuildTableState(Join::BuildTableState::FAILED);
        LOG_FMT_ERROR(log, "{} throw exception: {} In {} sec. ", log_msg, getCurrentExceptionMessage(false, true), watch.elapsedSeconds());
    }
}

} // namespace DB
