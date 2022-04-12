// Copyright 2022 PingCAP, Ltd.
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

#include <Common/DNSCache.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>

#include <csignal>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_KILL;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes


namespace
{
ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently execute all commands and genreates final exception message for failed commands
template <typename Callable, typename... Callables>
ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables &&... commands)
{
    ExecutionStatus status_head(0);
    try
    {
        command();
    }
    catch (...)
    {
        status_head = ExecutionStatus::fromCurrentException();
    }

    ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

    auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
    auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

    return ExecutionStatus(res_status, res_message);
}

} // namespace


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_)
    , context(context_)
{}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = typeid_cast<ASTSystemQuery &>(*query_ptr);

    using Type = ASTSystemQuery::Type;

    switch (query.type)
    {
    case Type::SHUTDOWN:
        if (kill(0, SIGTERM))
            throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
        break;
    case Type::KILL:
        if (kill(0, SIGKILL))
            throwFromErrno("System call kill(0, SIGKILL) failed", ErrorCodes::CANNOT_KILL);
        break;
    case Type::DROP_DNS_CACHE:
        DNSCache::instance().drop();
        break;
    case Type::DROP_MARK_CACHE:
        context.dropMarkCache();
        break;
    case Type::DROP_UNCOMPRESSED_CACHE:
        context.dropUncompressedCache();
        break;
    case Type::RELOAD_DICTIONARY:
        context.getExternalDictionaries().reloadDictionary(query.target_dictionary);
        break;
    case Type::RELOAD_DICTIONARIES:
    {
        auto status = getOverallExecutionStatusOfCommands(
            [&] { context.getExternalDictionaries().reload(); },
            [&] { context.getEmbeddedDictionaries().reload(); });
        if (status.code != 0)
            throw Exception(status.message, status.code);
        break;
    }
    case Type::RELOAD_CONFIG:
        context.reloadConfig();
        break;
    case Type::STOP_LISTEN_QUERIES:
    case Type::START_LISTEN_QUERIES:
    case Type::RESTART_REPLICAS:
    case Type::SYNC_REPLICA:
    case Type::STOP_MERGES:
    case Type::START_MERGES:
    case Type::STOP_REPLICATION_QUEUES:
    case Type::START_REPLICATION_QUEUES:
        throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
    default:
        throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}


} // namespace DB
