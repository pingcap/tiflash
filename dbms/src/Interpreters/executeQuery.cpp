// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <IO/Buffer/ConcatReadBuffer.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <Interpreters/IQuerySource.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/Quota.h>
#include <Interpreters/SQLQuerySource.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <fmt/core.h>
#include <tipb/expression.pb.h>
#include <tipb/select.pb.h>


namespace ProfileEvents
{
extern const Event Query;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int QUERY_IS_TOO_LARGE;
extern const int INTO_OUTFILE_NOT_ALLOWED;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char random_interpreter_failpoint[];
} // namespace FailPoints
namespace
{
void checkASTSizeLimits(const IAST & ast, const Settings & settings)
{
    if (settings.max_ast_depth)
        ast.checkDepth(settings.max_ast_depth);
    if (settings.max_ast_elements)
        ast.checkSize(settings.max_ast_elements);
}


String joinLines(const String & query)
{
    String res = query;
    std::replace(res.begin(), res.end(), '\n', ' ');
    return res;
}

LoggerPtr getLogger(const Context & context)
{
    auto * dag_context = context.getDAGContext();
    return (dag_context && dag_context->log) ? dag_context->log : Logger::get();
}


/// Call this inside catch block.
void setExceptionStackTrace(QueryLogElement & elem)
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        elem.stack_trace = e.getStackTrace().toString();
    }
    catch (...)
    {}
}


/// Log exception (with query info) into text log (not into system table).
void logException(Context & context, QueryLogElement & elem, const LoggerPtr & logger)
{
    LOG_ERROR(
        logger,
        "{} (from {}) (in query: {}){}",
        elem.exception,
        context.getClientInfo().current_address.toString(),
        joinLines(elem.query),
        (!elem.stack_trace.empty() ? ", Stack trace:\n\n" + elem.stack_trace : ""));
}


void onExceptionBeforeStart(const String & query, Context & context, time_t current_time, const LoggerPtr & logger)
{
    /// Exception before the query execution.
    context.getQuota().addError();

    bool log_queries = context.getSettingsRef().log_queries;

    /// Log the start of query execution into the table if necessary.
    if (log_queries)
    {
        QueryLogElement elem;

        elem.type = QueryLogElement::EXCEPTION_BEFORE_START;

        elem.event_time = current_time;
        elem.query_start_time = current_time;

        elem.query = query.substr(0, context.getSettingsRef().log_queries_cut_to_length);
        elem.exception = getCurrentExceptionMessage(false);

        elem.client_info = context.getClientInfo();

        setExceptionStackTrace(elem);
        logException(context, elem, logger);

        if (auto * query_log = context.getQueryLog())
            query_log->add(elem);
    }
}

void prepareForInputStream(Context & context, const BlockInputStreamPtr & in)
{
    assert(in);
    if (auto * stream = dynamic_cast<IProfilingBlockInputStream *>(in.get()))
    {
        stream->setProgressCallback(context.getProgressCallback());
        stream->setProcessListElement(context.getProcessListElement());
    }
}

std::tuple<ASTPtr, BlockIO> executeQueryImpl(
    SQLQuerySource & query_src,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage)
{
    auto execute_query_logger = getLogger(context);

    ProfileEvents::increment(ProfileEvents::Query);
    time_t current_time = time(nullptr);

    context.setQueryContext(context);

    const Settings & settings = context.getSettingsRef();

    ASTPtr ast;
    String query;

    /// Don't limit the size of internal queries.
    size_t max_query_size = 0;
    if (!internal)
        max_query_size = settings.max_query_size;

    try
    {
        std::tie(query, ast) = query_src.parse(max_query_size);
    }
    catch (...)
    {
        if (!internal)
        {
            /// Anyway log the query.
            String str = query_src.str(max_query_size);
            logQuery(str.substr(0, settings.log_queries_cut_to_length), context, execute_query_logger);
            onExceptionBeforeStart(str, context, current_time, execute_query_logger);
        }

        throw;
    }

    BlockIO res;

    try
    {
        if (!internal)
            logQuery(query.substr(0, settings.log_queries_cut_to_length), context, execute_query_logger);

        /// Check the limits.
        checkASTSizeLimits(*ast, settings);

        QuotaForIntervals & quota = context.getQuota();

        quota
            .addQuery(); /// NOTE Seems that when new time interval has come, first query is not accounted in number of queries.
        quota.checkExceeded(current_time);

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        ProcessList::EntryPtr process_list_entry;
        if (!internal && nullptr == typeid_cast<const ASTShowProcesslistQuery *>(&*ast))
        {
            process_list_entry = setProcessListElement(context, query, ast.get(), false);
        }

        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);
        auto interpreter = query_src.interpreter(context, stage);
        res = interpreter->execute();

        /// Delayed initialization of query streams (required for KILL QUERY purposes)
        if (process_list_entry)
            (*process_list_entry)->setQueryStreams(res);

        /// Hold element of process list till end of query execution.
        res.process_list_entry = process_list_entry;

        if (res.in)
        {
            prepareForInputStream(context, res.in);
        }

        if (res.out)
        {
            if (auto * stream = dynamic_cast<CountingBlockOutputStream *>(res.out.get()))
            {
                stream->setProcessListElement(context.getProcessListElement());
            }
        }

        /// Everything related to query log.
        {
            QueryLogElement elem;

            elem.type = QueryLogElement::QUERY_START;

            elem.event_time = current_time;
            elem.query_start_time = current_time;

            elem.query = query.substr(0, settings.log_queries_cut_to_length);

            elem.client_info = context.getClientInfo();

            bool log_queries = settings.log_queries && !internal;

            /// Log into system table start of query execution, if need.
            if (log_queries)
            {
                if (auto * query_log = context.getQueryLog())
                    query_log->add(elem);
            }

            /// Also make possible for caller to log successful query finish and exception during execution.
            res.finish_callback = [elem, &context, log_queries, execute_query_logger](
                                      IBlockInputStream * stream_in,
                                      IBlockOutputStream * stream_out) mutable {
                ProcessListElement * process_list_elem = context.getProcessListElement();

                if (!process_list_elem)
                    return;

                ProcessInfo info = process_list_elem->getInfo();

                double elapsed_seconds = info.elapsed_seconds;

                elem.type = QueryLogElement::QUERY_FINISH;

                elem.event_time = time(nullptr);
                elem.query_duration_ms = elapsed_seconds * 1000;

                elem.read_rows = info.read_rows;
                elem.read_bytes = info.read_bytes;

                elem.written_rows = info.written_rows;
                elem.written_bytes = info.written_bytes;

                elem.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

                if (stream_in)
                {
                    if (const auto * profiling_stream = dynamic_cast<const IProfilingBlockInputStream *>(stream_in))
                    {
                        const BlockStreamProfileInfo & info = profiling_stream->getProfileInfo();

                        /// NOTE: INSERT SELECT query contains zero metrics
                        elem.result_rows = info.rows;
                        elem.result_bytes = info.bytes;
                    }
                }
                else if (stream_out) /// will be used only for ordinary INSERT queries
                {
                    if (const auto * counting_stream = dynamic_cast<const CountingBlockOutputStream *>(stream_out))
                    {
                        /// NOTE: Redundancy. The same values could be extracted from process_list_elem->progress_out.
                        elem.result_rows = counting_stream->getProgress().rows;
                        elem.result_bytes = counting_stream->getProgress().bytes;
                    }
                }

                if (elem.read_rows != 0)
                {
                    LOG_INFO(
                        execute_query_logger,
                        "Read {} rows, {} in {:.3f} sec., {} rows/sec., {}/sec.",
                        elem.read_rows,
                        formatReadableSizeWithBinarySuffix(elem.read_bytes),
                        elapsed_seconds,
                        static_cast<size_t>(elem.read_rows / elapsed_seconds),
                        formatReadableSizeWithBinarySuffix(elem.read_bytes / elapsed_seconds));
                }

                if (log_queries)
                {
                    if (auto * query_log = context.getQueryLog())
                        query_log->add(elem);
                }
            };

            res.exception_callback = [elem, &context, log_queries, execute_query_logger]() mutable {
                context.getQuota().addError();

                elem.type = QueryLogElement::EXCEPTION_WHILE_PROCESSING;

                elem.event_time = time(nullptr);
                elem.query_duration_ms = 1000 * (elem.event_time - elem.query_start_time);
                elem.exception = getCurrentExceptionMessage(false);

                ProcessListElement * process_list_elem = context.getProcessListElement();

                if (process_list_elem)
                {
                    ProcessInfo info = process_list_elem->getInfo();

                    elem.query_duration_ms = info.elapsed_seconds * 1000;

                    elem.read_rows = info.read_rows;
                    elem.read_bytes = info.read_bytes;

                    elem.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;
                }

                setExceptionStackTrace(elem);
                logException(context, elem, execute_query_logger);

                if (log_queries)
                {
                    if (auto * query_log = context.getQueryLog())
                        query_log->add(elem);
                }
            };

            if (!internal && res.in)
            {
                logQueryPipeline(execute_query_logger, res.in);
            }
        }
    }
    catch (...)
    {
        if (!internal)
            onExceptionBeforeStart(query, context, current_time, execute_query_logger);

        throw;
    }

    return std::make_tuple(ast, res);
}
} // namespace

/// Log query into text log (not into system table).
void logQuery(const String & query, const Context & context, const LoggerPtr & logger)
{
    const auto & current_query_id = context.getClientInfo().current_query_id;
    const auto & initial_query_id = context.getClientInfo().initial_query_id;
    const auto & current_user = context.getClientInfo().current_user;

    LOG_DEBUG(
        logger,
        "(from {}{}, query_id: {}{}) {}",
        context.getClientInfo().current_address.toString(),
        (current_user != "default" ? ", user: " + current_user : ""),
        current_query_id,
        (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id
                                                                           : ""),
        joinLines(query));
}

std::shared_ptr<ProcessListEntry> setProcessListElement(
    Context & context,
    const String & query,
    const IAST * ast,
    bool is_dag_task)
{
    assert(ast);
    auto total_memory = context.getServerInfo().has_value() ? context.getServerInfo()->memory_info.capacity : 0;
    auto process_list_entry
        = context.getProcessList()
              .insert(query, ast, context.getClientInfo(), context.getSettingsRef(), total_memory, is_dag_task);
    context.setProcessListElement(&process_list_entry->get());
    return process_list_entry;
}

void logQueryPipeline(const LoggerPtr & logger, const BlockInputStreamPtr & in)
{
    assert(in);
    auto pipeline_log_str = [&in]() {
        FmtBuffer log_buffer;
        log_buffer.append("Query pipeline:\n");
        in->dumpTree(log_buffer);
        return log_buffer.toString();
    };
    LOG_INFO(logger, pipeline_log_str());
}

BlockIO executeQuery(const String & query, Context & context, bool internal, QueryProcessingStage::Enum stage)
{
    BlockIO streams;
    SQLQuerySource query_src(query.data(), query.data() + query.size());
    std::tie(std::ignore, streams) = executeQueryImpl(query_src, context, internal, stage);
    return streams;
}


void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    bool allow_into_outfile,
    Context & context,
    std::function<void(const String &)> set_content_type)
{
    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    /// If 'istr' is empty now, fetch next data into buffer.
    if (istr.buffer().size() == 0)
        istr.next();

    size_t max_query_size = context.getSettingsRef().max_query_size;

    if (istr.buffer().end() - istr.position() > static_cast<ssize_t>(max_query_size))
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        begin = istr.position();
        end = istr.buffer().end();
        istr.position() += end - begin;
    }
    else
    {
        /// If not - copy enough data into 'parse_buf'.
        parse_buf.resize(max_query_size + 1);
        parse_buf.resize(istr.read(&parse_buf[0], max_query_size + 1));
        begin = &parse_buf[0];
        end = begin + parse_buf.size();
    }

    ASTPtr ast;
    BlockIO streams;

    SQLQuerySource query_info(begin, end);
    std::tie(ast, streams) = executeQueryImpl(query_info, context, false, QueryProcessingStage::Complete);

    try
    {
        if (streams.out)
        {
            InputStreamFromASTInsertQuery in(ast, istr, streams, context);
            copyData(in, *streams.out);
        }

        if (streams.in)
        {
            const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

            WriteBuffer * out_buf = &ostr;
            std::optional<WriteBufferFromFile> out_file_buf;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

                const auto & out_file
                    = typeid_cast<const ASTLiteral &>(*ast_query_with_output->out_file).value.safeGet<std::string>();
                out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                out_buf = &*out_file_buf;
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                ? typeid_cast<const ASTIdentifier &>(*ast_query_with_output->format).name
                : context.getDefaultFormat();

            BlockOutputStreamPtr out = context.getOutputFormat(format_name, *out_buf, streams.in->getHeader());

            if (auto * stream = dynamic_cast<IProfilingBlockInputStream *>(streams.in.get()))
            {
                /// Save previous progress callback if any. TODO Do it more conveniently.
                auto previous_progress_callback = context.getProgressCallback();

                /// NOTE Progress callback takes shared ownership of 'out'.
                stream->setProgressCallback([out, previous_progress_callback](const Progress & progress) {
                    if (previous_progress_callback)
                        previous_progress_callback(progress);
                    out->onProgress(progress);
                });
            }

            if (set_content_type)
                set_content_type(out->getContentType());

            copyData(*streams.in, *out);
        }
    }
    catch (...)
    {
        streams.onException();
        throw;
    }

    streams.onFinish();
}

} // namespace DB
