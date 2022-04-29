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

#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int NOT_IMPLEMENTED;
}


const char * ASTSystemQuery::typeToString(Type type)
{
    switch (type)
    {
        case Type::SHUTDOWN:
            return "SHUTDOWN";
        case Type::KILL:
            return "KILL";
        case Type::DROP_DNS_CACHE:
            return "DROP DNS CACHE";
        case Type::DROP_MARK_CACHE:
            return "DROP MARK CACHE";
        case Type::DROP_UNCOMPRESSED_CACHE:
            return "DROP UNCOMPRESSED CACHE";
        case Type::STOP_LISTEN_QUERIES:
            return "STOP LISTEN QUERIES";
        case Type::START_LISTEN_QUERIES:
            return "START LISTEN QUERIES";
        case Type::RESTART_REPLICAS:
            return "RESTART REPLICAS";
        case Type::SYNC_REPLICA:
            return "SYNC REPLICA";
        case Type::RELOAD_DICTIONARY:
            return "RELOAD DICTIONARY";
        case Type::RELOAD_DICTIONARIES:
            return "RELOAD DICTIONARIES";
        case Type::RELOAD_CONFIG:
            return "RELOAD CONFIG";
        case Type::STOP_MERGES:
            return "STOP MERGES";
        case Type::START_MERGES:
            return "START MERGES";
        case Type::STOP_REPLICATION_QUEUES:
            return "STOP REPLICATION QUEUES";
        case Type::START_REPLICATION_QUEUES:
            return "START REPLICATION QUEUES";
        default:
            throw Exception("Unknown SYSTEM query command", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM " << (settings.hilite ? hilite_none : "");
    settings.ostr << typeToString(type);

    if (type == Type::RELOAD_DICTIONARY)
        settings.ostr << " " << backQuoteIfNeed(target_dictionary);
    else if (type == Type::SYNC_REPLICA)
        throw Exception("SYNC_REPLICA isn't supported yet", ErrorCodes::NOT_IMPLEMENTED);
}


}
