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

#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTSystemQuery : public IAST
{
public:

    enum class Type
    {
        UNKNOWN,
        SHUTDOWN,
        KILL,
        DROP_DNS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        SYNC_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_CONFIG,
        STOP_MERGES,
        START_MERGES,
        STOP_REPLICATION_QUEUES,
        START_REPLICATION_QUEUES,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    String target_dictionary;
    //String target_replica_database;
    //String target_replica_table;

    String getID() const override { return "SYSTEM query"; };

    ASTPtr clone() const override { return std::make_shared<ASTSystemQuery>(*this); }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
