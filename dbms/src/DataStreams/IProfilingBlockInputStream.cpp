#include <Common/joinStr.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Quota.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
extern const int TOO_MANY_BYTES;
extern const int TOO_MANY_ROWS_OR_BYTES;
extern const int TIMEOUT_EXCEEDED;
extern const int TOO_SLOW;
extern const int LOGICAL_ERROR;
extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
} // namespace ErrorCodes


IProfilingBlockInputStream::IProfilingBlockInputStream()
{
    info.parent = this;
}

Block IProfilingBlockInputStream::read()
{
    FilterPtr filter;
    return read(filter, false);
}

Block IProfilingBlockInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    if (total_rows_approx)
    {
        progressImpl(Progress(0, 0, total_rows_approx));
        total_rows_approx = 0;
    }

    if (!info.started)
    {
        info.total_stopwatch.start();
        info.last_wait_ts = info.total_stopwatch.elapsed();
        info.started = true;
    }

    Block res;

    if (isCancelledOrThrowIfKilled())
        return res;

    info.waiting_duration += info.total_stopwatch.elapsed() - info.last_wait_ts;
    auto start_ts = info.total_stopwatch.elapsed();

    if (!checkTimeLimit())
        limit_exceeded_need_break = true;

    if (!limit_exceeded_need_break)
    {
        if (info.is_first)
        {
            info.is_first = false;
            info.first_ts = info.now();
        }

        if (return_filter)
            res = readImpl(res_filter, return_filter);
        else
            res = readImpl();

        if (res)
        {
            info.total_rows += res.rows();
            info.total_bytes += res.bytes();
        }
        else
        {
            info.last_ts = info.now();
        }
    }

    if (res)
    {
        info.update(res);

        if (enabled_extremes)
            updateExtremes(res);

        if (limits.mode == LIMITS_CURRENT && !limits.size_limits.check(info.rows, info.bytes, "result", ErrorCodes::TOO_MANY_ROWS_OR_BYTES))
            limit_exceeded_need_break = true;

        if (quota != nullptr)
            checkQuota(res);
    }
    else
    {
        /** If the thread is over, then we will ask all children to abort the execution.
          * This makes sense when running a query with LIMIT
          * - there is a situation when all the necessary data has already been read,
          *   but children sources are still working,
          *   herewith they can work in separate threads or even remotely.
          */
        cancel(false);
    }

    progress(Progress(res.rows(), res.bytes()));

#ifndef NDEBUG
    if (res)
    {
        Block header = getHeader();
        if (header)
            assertBlocksHaveEqualStructure(res, header, getName());
    }
#endif

    auto duration = info.total_stopwatch.elapsed() - start_ts;
    info.running_duration += duration;
    info.updateExecutionTime(duration);

    info.last_wait_ts = info.total_stopwatch.elapsed();
    return res;
}


void IProfilingBlockInputStream::dumpProfileInfo(FmtBuffer & buf)
{
    std::unordered_set<Int64> dumped;

    buf.append("[");
    recursiveDumpProfileInfo(buf, dumped);
    buf.append("]");
}

void IProfilingBlockInputStream::recursiveDumpProfileInfo(FmtBuffer & buf, std::unordered_set<Int64> & dumped)
{
    if (dumped.count(info.id))
        return;

    bool first = dumped.empty();
    dumped.insert(info.id);

    if (!first)
        buf.append(",");
    buf.append("{");

    buf.fmtAppend("\"id\":{},", info.id)
        .fmtAppend(R"("name":"{}",)", getName())
        .fmtAppend(R"("executor":"{}",)", info.executor.empty() ? "<unknown>" : info.executor);

    buf.append("\"children\":[");
    joinStr(
        children.begin(),
        children.end(),
        buf,
        [](const std::shared_ptr<IBlockInputStream> & child, FmtBuffer & buf) {
            auto * child_ptr = dynamic_cast<IProfilingBlockInputStream *>(child.get());
            buf.fmtAppend("{}", child_ptr->info.id);
        },
        ",");
    buf.append("],");

    buf.append("\"stat\":");
    dumpProfileInfoImpl(buf);

    buf.append("}");

    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        child.recursiveDumpProfileInfo(buf, dumped);
        return false;
    });
}

void IProfilingBlockInputStream::dumpProfileInfoImpl(FmtBuffer & buf)
{
    buf.append("{");

    buf.fmtAppend(R"("schema":"{}",)", getHeader().dumpStructure())
        .fmtAppend("\"prefix_duration\":{},", info.prefix_duration)
        .fmtAppend("\"suffix_duration\":{},", info.suffix_duration)
        .fmtAppend("\"pull_duration\":{},", info.timeline.getCounter(Timeline::PULL))
        .fmtAppend("\"self_duration\":{},", info.timeline.getCounter(Timeline::SELF))
        .fmtAppend("\"push_duration\":{},", info.timeline.getCounter(Timeline::PUSH))
        .fmtAppend("\"first_ts\":{},", info.toNanoseconds(info.first_ts))
        .fmtAppend("\"last_ts\":{},", info.toNanoseconds(info.last_ts))
        .fmtAppend("\"total_rows\":{},", info.total_rows)
        .fmtAppend("\"total_bytes\":{},", info.total_bytes);

    buf.append("\"timeline\":");
    info.timeline.flushBuffer();
    info.timeline.dump(buf);

    buf.append("}");
}

Timeline::Timer IProfilingBlockInputStream::newTimer(Timeline::CounterType type, bool running)
{
    return info.timeline.newTimer(type, running);
}

void IProfilingBlockInputStream::readPrefix()
{
    info.total_stopwatch.start();

    info.prefix_ts = info.now();

    auto start_ts = info.total_stopwatch.elapsed();
    readPrefixImpl();

    forEachChild([&](IBlockInputStream & child) {
        child.readPrefix();
        return false;
    });
    info.prefix_duration = info.total_stopwatch.elapsed() - start_ts;
    info.updateExecutionTime(info.prefix_duration);

    info.last_wait_ts = info.total_stopwatch.elapsed();
    info.started = true;
}


void IProfilingBlockInputStream::readSuffix()
{
    if (!info.started)
    {
        info.total_stopwatch.start();
        info.last_wait_ts = info.total_stopwatch.elapsed();
        info.started = true;
    }

    info.waiting_duration += info.total_stopwatch.elapsed() - info.last_wait_ts;

    auto start_ts = info.total_stopwatch.elapsed();
    forEachChild([&](IBlockInputStream & child) {
        child.readSuffix();
        return false;
    });

    readSuffixImpl();
    info.suffix_duration = info.total_stopwatch.elapsed() - start_ts;
    info.updateExecutionTime(info.suffix_duration);

    info.suffix_ts = info.now();
}


void IProfilingBlockInputStream::updateExtremes(Block & block)
{
    size_t num_columns = block.columns();

    if (!extremes)
    {
        MutableColumns extremes_columns(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const ColumnPtr & src = block.safeGetByPosition(i).column;

            if (src->isColumnConst())
            {
                /// Equal min and max.
                extremes_columns[i] = src->cloneResized(2);
            }
            else
            {
                Field min_value;
                Field max_value;

                src->getExtremes(min_value, max_value);

                extremes_columns[i] = src->cloneEmpty();

                extremes_columns[i]->insert(min_value);
                extremes_columns[i]->insert(max_value);
            }
        }

        extremes = block.cloneWithColumns(std::move(extremes_columns));
    }
    else
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnPtr & old_extremes = extremes.safeGetByPosition(i).column;

            if (old_extremes->isColumnConst())
                continue;

            Field min_value = (*old_extremes)[0];
            Field max_value = (*old_extremes)[1];

            Field cur_min_value;
            Field cur_max_value;

            block.safeGetByPosition(i).column->getExtremes(cur_min_value, cur_max_value);

            if (cur_min_value < min_value)
                min_value = cur_min_value;
            if (cur_max_value > max_value)
                max_value = cur_max_value;

            MutableColumnPtr new_extremes = old_extremes->cloneEmpty();

            new_extremes->insert(min_value);
            new_extremes->insert(max_value);

            old_extremes = std::move(new_extremes);
        }
    }
}


static bool handleOverflowMode(OverflowMode mode, const String & message, int code)
{
    switch (mode)
    {
    case OverflowMode::THROW:
        throw Exception(message, code);
    case OverflowMode::BREAK:
        return false;
    default:
        throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
    }
};


bool IProfilingBlockInputStream::checkTimeLimit()
{
    if (limits.max_execution_time != 0
        && info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
        return handleOverflowMode(limits.timeout_overflow_mode,
                                  "Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
                                      + " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
                                  ErrorCodes::TIMEOUT_EXCEEDED);

    return true;
}


void IProfilingBlockInputStream::checkQuota(Block & block)
{
    switch (limits.mode)
    {
    case LIMITS_TOTAL:
        /// Checked in `progress` method.
        break;

    case LIMITS_CURRENT:
    {
        time_t current_time = time(nullptr);
        double total_elapsed = info.total_stopwatch.elapsedSeconds();

        quota->checkAndAddResultRowsBytes(current_time, block.rows(), block.bytes());
        quota->checkAndAddExecutionTime(current_time, Poco::Timespan((total_elapsed - prev_elapsed) * 1000000.0));

        prev_elapsed = total_elapsed;
        break;
    }

    default:
        throw Exception("Logical error: unknown limits mode.", ErrorCodes::LOGICAL_ERROR);
    }
}


void IProfilingBlockInputStream::progressImpl(const Progress & value)
{
    if (progress_callback)
        progress_callback(value);

    if (process_list_elem)
    {
        if (!process_list_elem->updateProgressIn(value))
            cancel(/* kill */ true);

        /// The total amount of data processed or intended for processing in all leaf sources, possibly on remote servers.

        ProgressValues progress = process_list_elem->getProgressIn();
        size_t total_rows_estimate = std::max(progress.rows, progress.total_rows);

        /** Check the restrictions on the amount of data to read, the speed of the query, the quota on the amount of data to read.
            * NOTE: Maybe it makes sense to have them checked directly in ProcessList?
            */

        if (limits.mode == LIMITS_TOTAL
            && ((limits.size_limits.max_rows && total_rows_estimate > limits.size_limits.max_rows)
                || (limits.size_limits.max_bytes && progress.bytes > limits.size_limits.max_bytes)))
        {
            switch (limits.size_limits.overflow_mode)
            {
            case OverflowMode::THROW:
            {
                if (limits.size_limits.max_rows && total_rows_estimate > limits.size_limits.max_rows)
                    throw Exception("Limit for rows to read exceeded: " + toString(total_rows_estimate)
                                        + " rows read (or to read), maximum: " + toString(limits.size_limits.max_rows),
                                    ErrorCodes::TOO_MANY_ROWS);
                else
                    throw Exception("Limit for (uncompressed) bytes to read exceeded: " + toString(progress.bytes)
                                        + " bytes read, maximum: " + toString(limits.size_limits.max_bytes),
                                    ErrorCodes::TOO_MANY_BYTES);
                break;
            }

            case OverflowMode::BREAK:
            {
                /// For `break`, we will stop only if so many rows were actually read, and not just supposed to be read.
                if ((limits.size_limits.max_rows && progress.rows > limits.size_limits.max_rows)
                    || (limits.size_limits.max_bytes && progress.bytes > limits.size_limits.max_bytes))
                {
                    cancel(false);
                }

                break;
            }

            default:
                throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
            }
        }

        size_t total_rows = progress.total_rows;

        if (limits.min_execution_speed || (total_rows && limits.timeout_before_checking_execution_speed != 0))
        {
            double total_elapsed = info.total_stopwatch.elapsedSeconds();

            if (total_elapsed > limits.timeout_before_checking_execution_speed.totalMicroseconds() / 1000000.0)
            {
                if (limits.min_execution_speed && progress.rows / total_elapsed < limits.min_execution_speed)
                    throw Exception("Query is executing too slow: " + toString(progress.rows / total_elapsed)
                                        + " rows/sec., minimum: " + toString(limits.min_execution_speed),
                                    ErrorCodes::TOO_SLOW);

                size_t total_rows = progress.total_rows;

                /// If the predicted execution time is longer than `max_execution_time`.
                if (limits.max_execution_time != 0 && total_rows)
                {
                    double estimated_execution_time_seconds = total_elapsed * (static_cast<double>(total_rows) / progress.rows);

                    if (estimated_execution_time_seconds > limits.max_execution_time.totalSeconds())
                        throw Exception("Estimated query execution time (" + toString(estimated_execution_time_seconds) + " seconds)"
                                            + " is too long. Maximum: " + toString(limits.max_execution_time.totalSeconds())
                                            + ". Estimated rows to process: " + toString(total_rows),
                                        ErrorCodes::TOO_SLOW);
                }
            }
        }

        if (quota != nullptr && limits.mode == LIMITS_TOTAL)
        {
            quota->checkAndAddReadRowsBytes(time(nullptr), value.rows, value.bytes);
        }
    }
}


void IProfilingBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        child.cancel(kill);
        return false;
    });
}


bool IProfilingBlockInputStream::isCancelled() const
{
    return is_cancelled;
}

bool IProfilingBlockInputStream::isCancelledOrThrowIfKilled() const
{
    if (!is_cancelled)
        return false;
    if (is_killed)
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    return true;
}


void IProfilingBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    progress_callback = callback;

    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        child.setProgressCallback(callback);
        return false;
    });
}


void IProfilingBlockInputStream::setProcessListElement(ProcessListElement * elem)
{
    process_list_elem = elem;

    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        child.setProcessListElement(elem);
        return false;
    });
}


Block IProfilingBlockInputStream::getTotals()
{
    if (totals)
        return totals;

    Block res;
    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        res = child.getTotals();
        if (res)
            return true;
        return false;
    });
    return res;
}

Block IProfilingBlockInputStream::getExtremes()
{
    if (extremes)
        return extremes;

    Block res;
    forEachProfilingChild([&](IProfilingBlockInputStream & child) {
        res = child.getExtremes();
        if (res)
            return true;
        return false;
    });
    return res;
}

} // namespace DB
